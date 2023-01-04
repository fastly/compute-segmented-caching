use anyhow::{anyhow, Context, Error};
use fastly::http::{body::StreamingBody, HeaderValue, Method, StatusCode};
use fastly::{Body, Request, Response};
use std::cmp::min;
use std::collections::VecDeque;
use std::io::Read;

struct Config {
    block_size: usize,
    parallelism: usize,
    read_chunk_size: usize,
    backend_name: String,
    backend_host: HeaderValue,
}

impl Config {
    fn new(req: &Request) -> Self {
        let mut block_size = 1024 * 1024;
        let mut parallelism = 5;
        let mut read_chunk_size = 65536;
        if let Some(field) = req.get_header("x-sc-conf").and_then(|hv| hv.to_str().ok()) {
            for part in field.split(",") {
                match part.split_once("=") {
                    Some(("b", value)) => {
                        if let Ok(value) = value.parse() {
                            if 1024 * 1024 <= value && value <= 50 * 1024 * 1024 {
                                block_size = value;
                            }
                        }
                    }
                    Some(("p", value)) => {
                        if let Ok(value) = value.parse() {
                            if 1 <= parallelism && parallelism <= 10 {
                                parallelism = value;
                            }
                        }
                    }
                    Some(("r", value)) => {
                        if let Ok(value) = value.parse() {
                            if 1024 <= value && value <= 1024 * 1024 {
                                read_chunk_size = value;
                            }
                        }
                    }
                    _ => (),
                }
            }
        }
        Self {
            block_size,
            parallelism,
            read_chunk_size,
            backend_name: String::from("YOUR_BACKEND_NAME_HERE"),
            backend_host: HeaderValue::from_static("the.host.header.for.your.backend.here"),
        }
    }
}

enum RequestRange {
    Closed { first: usize, last: usize },
    Open { first: usize },
}

impl RequestRange {
    fn new(req: &Request) -> Result<Option<Self>, Error> {
        let values = req.get_header_all("range").collect::<Vec<_>>();
        let value = match &values[..] {
            [] => return Ok(None),
            [value] => value.to_str().context("range header value")?,
            [_, _, ..] => return Err(anyhow!("multiple range fields")),
        };
        let range = match value.split_once("=") {
            Some(("bytes", range)) => range,
            _ => return Err(anyhow!("range not bytes")),
        };
        let req_range = match range.split_once("-") {
            Some(("", last)) if last.len() > 0 => {
                return Err(anyhow!("suffix range not supported"))
            }
            Some((first, "")) => {
                let first = first.parse().context("range lower bound")?;
                RequestRange::Open { first }
            }
            Some((first, last)) => {
                let first = first.parse().context("range lower bound")?;
                let last = last.parse().context("range upper bound")?;
                if last < first {
                    return Err(anyhow!("range upper bound lower than lower bound"));
                }
                RequestRange::Closed { first, last }
            }
            _ => return Err(anyhow!("cannot parse requested range")),
        };
        Ok(Some(req_range))
    }

    fn get_first(&self) -> usize {
        return match &self {
            RequestRange::Closed { first, .. } | RequestRange::Open { first } => *first,
        };
    }

    fn get_last(&self) -> Option<usize> {
        return match &self {
            RequestRange::Closed { last, .. } => Some(*last),
            RequestRange::Open { .. } => None,
        };
    }
}

struct ResolvedRange {
    first: usize,
    last: usize,
}

impl ResolvedRange {
    fn new(req_range: &Option<RequestRange>, complete_length: usize) -> Option<Self> {
        if let Some(req_range) = req_range {
            let first = req_range.get_first();
            if first >= complete_length {
                return None;
            }
            let last = req_range
                .get_last()
                .map(|x| min(x, complete_length - 1))
                .unwrap_or(complete_length - 1);
            Some(ResolvedRange { first, last })
        } else {
            Some(ResolvedRange {
                first: 0,
                last: complete_length - 1,
            })
        }
    }
}

struct ContentRange {
    first: usize,
    last: usize,
    complete_length: usize,
}

impl ContentRange {
    fn new(resp: &Response) -> Result<Self, Error> {
        let values = resp.get_header_all("content-range").collect::<Vec<_>>();
        let value = match &values[..] {
            [] => return Err(anyhow!("missing content-range")),
            [value] => value.to_str().context("content-range header value")?,
            [_, _, ..] => return Err(anyhow!("multiple content-range fields")),
        };
        let field = match value.split_once(" ") {
            Some(("bytes", range)) => range,
            _ => return Err(anyhow!("content-range not bytes")),
        };
        let (range, complete_length) = match field.split_once("/") {
            Some((_, "*")) => {
                return Err(anyhow!(
                    "unknown complete length in content-range not supported"
                ))
            }
            Some(("*", _)) => {
                return Err(anyhow!("unsatisfied range in content-range not supported"))
            }
            Some((range, complete_length)) => (
                range,
                complete_length
                    .parse()
                    .context("content-range complete length")?,
            ),
            _ => return Err(anyhow!("cannot parse content-range")),
        };
        if complete_length == 0 {
            return Err(anyhow!("zero complete length in content-range"));
        }
        let content_range = match range.split_once("-") {
            Some((first, last)) => {
                let first = first.parse().context("content-range lower bound")?;
                let last = last.parse().context("content-range upper bound")?;
                if last < first {
                    return Err(anyhow!("content-range upper bound lower than lower bound"));
                }
                if first >= complete_length {
                    return Err(anyhow!(
                        "content-range lower bound not lower than complete length"
                    ));
                }
                if last >= complete_length {
                    return Err(anyhow!(
                        "content-range upper bound not lower than complete length"
                    ));
                }
                ContentRange {
                    first,
                    last,
                    complete_length,
                }
            }
            _ => return Err(anyhow!("cannot parse range in content-range")),
        };
        Ok(content_range)
    }
}

struct Fragment {
    body: Body,
    first: usize,
    last: usize,
}

impl Fragment {
    fn new(body: Body, content_range: &ContentRange) -> Self {
        Self {
            body,
            first: content_range.first,
            last: content_range.last,
        }
    }
}

struct BodyStreamingState {
    position: usize,
    last: usize,
    resp_body: StreamingBody,
    block_size: usize,
    buf: Vec<u8>,
}

impl BodyStreamingState {
    fn new(range: &ResolvedRange, resp_body: StreamingBody, config: &Config) -> Self {
        BodyStreamingState {
            position: range.first,
            last: range.last,
            resp_body,
            block_size: config.block_size,
            buf: vec![0; config.read_chunk_size],
        }
    }

    fn send_fragment(&mut self, mut frag: Fragment) -> Result<(), Error> {
        if self.position < frag.first || self.position >= frag.last {
            return Err(anyhow!(
                "unexpected fragment {}-{} at position {}",
                frag.first,
                frag.last,
                self.position
            ));
        }
        if self.position > frag.first {
            let mut remainder = self.position - frag.first;
            while remainder > 0 {
                let toread = min(remainder, self.buf.len());
                let rsize = frag
                    .body
                    .read(&mut self.buf[..toread])
                    .context("reading fragment")?;
                if rsize == 0 {
                    return Err(anyhow!("truncated fragment"));
                }
                remainder -= rsize;
            }
        }
        if self.last >= frag.last {
            self.resp_body.append(frag.body);
            self.position = frag.last + 1;
        } else {
            let last = min(frag.last, self.last);
            let mut remainder = last - self.position + 1;
            while remainder > 0 {
                let toread = min(remainder, self.buf.len());
                let rsize = frag
                    .body
                    .read(&mut self.buf[..toread])
                    .context("reading fragment")?;
                if rsize == 0 {
                    return Err(anyhow!("truncated fragment"));
                }
                let mut wpos = 0;
                while wpos < rsize {
                    let wsize = self.resp_body.write_bytes(&self.buf[wpos..rsize]);
                    wpos += wsize;
                }
                remainder -= rsize;
            }
            self.position = last + 1;
        }
        Ok(())
    }

    fn frag_req_gen(&self) -> FragReqGen {
        FragReqGen {
            position: self.position,
            last: self.last,
            block_size: self.block_size,
        }
    }
}

struct FragReqGen {
    position: usize,
    last: usize,
    block_size: usize,
}

impl Iterator for FragReqGen {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        if self.position <= self.last {
            let pos = self.position;
            self.position += self.block_size;
            Some(format!("bytes={}-{}", pos, pos + self.block_size - 1))
        } else {
            None
        }
    }
}

fn doit(resp_header_sent: &mut bool) -> Result<Option<Response>, Error> {
    let mut req = Request::from_client();
    let config = Config::new(&req);
    let req_range = RequestRange::new(&req).ok().flatten();
    let header_only = match req.get_method() {
        &Method::HEAD => true,
        &Method::GET => false,
        _ => {
            return Ok(Some(
                Response::from_status(StatusCode::METHOD_NOT_ALLOWED)
                    .with_header("allow", "GET, HEAD")
                    .with_body_text_plain("Method not allowed\n"),
            ))
        }
    };
    if req.read_body_chunks(1).next().is_some() {
        return Ok(Some(
            Response::from_status(StatusCode::FORBIDDEN)
                .with_body_text_plain("Request body not allowed\n"),
        ));
    }

    let (mut resp, frag1, complete_length) = {
        let first = req_range.as_ref().map(RequestRange::get_first).unwrap_or(0);
        let first = first / config.block_size * config.block_size;
        let last = first + config.block_size - 1;
        let mut bereq = req.clone_without_body();
        bereq.set_pass(true);
        bereq.set_header("range", format!("bytes={}-{}", first, last));
        bereq.set_header("host", &config.backend_host);
        let mut beresp = bereq
            .send(&config.backend_name)
            .context("first backend request send")?;
        if beresp.get_status() != StatusCode::PARTIAL_CONTENT {
            return Ok(Some(beresp));
        }
        let content_range = ContentRange::new(&beresp).context("first backend response")?;
        if content_range.first != first || content_range.last > last {
            return Err(anyhow!(
                "fragment content range {}-{} unexpected for request range {}-{}",
                content_range.first,
                content_range.last,
                first,
                last,
            ));
        }
        beresp.remove_header("content-range");
        beresp.remove_header("content-length");
        beresp.remove_header("transfer-encoding");
        (
            beresp.clone_without_body(),
            Fragment::new(beresp.into_body(), &content_range),
            content_range.complete_length,
        )
    };

    let range = if let Some(range) = ResolvedRange::new(&req_range, complete_length) {
        range
    } else {
        return Ok(Some(
            Response::from_status(StatusCode::RANGE_NOT_SATISFIABLE)
                .with_header("content-range", format!("bytes */{}", complete_length))
                .with_body_text_plain("Range not satisfiable\n"),
        ));
    };

    if req_range.is_some() {
        resp.set_status(StatusCode::PARTIAL_CONTENT);
        resp.set_header(
            "content-range",
            format!("bytes {}-{}/{}", range.first, range.last, complete_length),
        );
    } else {
        resp.set_status(StatusCode::OK);
    }
    resp.set_header("content-length", (range.last - range.first + 1).to_string());
    resp.set_framing_headers_mode(fastly::http::FramingHeadersMode::ManuallyFromHeaders);

    let resp_body = resp.stream_to_client();
    *resp_header_sent = true;
    if header_only {
        return Ok(None);
    }

    let mut state = BodyStreamingState::new(&range, resp_body, &config);
    state
        .send_fragment(frag1)
        .context("sending first fragment")?;
    let mut frag_req_gen = state.frag_req_gen();
    let mut queue = VecDeque::new();

    loop {
        while queue.len() < config.parallelism {
            if let Some(range) = frag_req_gen.next() {
                let mut bereq = req.clone_without_body();
                bereq.set_pass(true);
                bereq.set_header("range", range);
                bereq.set_header("host", &config.backend_host);
                queue.push_back(
                    bereq
                        .send_async(&config.backend_name)
                        .context("backend request send_async")?,
                );
            } else {
                break;
            }
        }
        if let Some(promise) = queue.pop_front() {
            let beresp = promise.wait().context("backend request wait")?;
            if beresp.get_status() != StatusCode::PARTIAL_CONTENT {
                return Err(anyhow!(
                    "fragment status code {} rather than 206",
                    beresp.get_status()
                ));
            }
            let content_range = ContentRange::new(&beresp)?;
            if content_range.complete_length != complete_length {
                return Err(anyhow!(
                    "complete length inconsistent between fragments: {} vs {}",
                    content_range.complete_length,
                    complete_length
                ));
            }
            state.send_fragment(Fragment::new(beresp.into_body(), &content_range))?;
        } else {
            break;
        }
    }

    Ok(None)
}

fn main() -> () {
    let mut resp_header_sent = false;
    match doit(&mut resp_header_sent) {
        Ok(None) => {
            return;
        }
        Ok(Some(resp)) => resp,
        Err(e) => {
            let e = format!("{:#}\n", e);
            eprintln!("ERROR: {}", &e);
            if resp_header_sent {
                return;
            }
            Response::from_status(StatusCode::INTERNAL_SERVER_ERROR).with_body_text_plain(&e)
        }
    }
    .send_to_client();
}
