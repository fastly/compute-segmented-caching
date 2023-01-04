# Segmented Caching as a Compute@Edge Application

## Overview

It is a [Fastly Compute@Edge](https://developer.fastly.com/learning/compute/)
application that retrieves fixed-sized blocks of an object from a backend, and
then it assembles a response using the blocks.  Depending on the size of the
object and on which part of the object the client requests (`Range` in the
request), the response may be assembled from one or more blocks and some blocks
may be included in their entirety while the extremities may use partial blocks.

The blocks are individually cacheable.  Blocks have a fixed size (except for
the last block in the object), and their position in the object starts at
a multiple of the block size.

As C@E does not yet have direct programmatic access to caching, this C@E
application sits in front of a VCL service, which handles caching. The VCL
service sets `req.enable_segmented_caching` to true in `vcl_recv`.

For best results the block size used by the VCL service in the back should
match the block size used by the C@E service in the front. The Segmented
Caching feature in VCL then always assembles its response from a single block.
A description of Segmented Caching in our VCL offering is
[here](https://docs.fastly.com/en/guides/segmented-caching).  The default block
size is 1 MiB.

Unlike the Segmented Caching implementation in the Fastly Varnish, this C@E
application retrieves blocks in parallel (5 at a time by default).  That gives
it higher throughput, especially for cache misses, at the expense of higher
CPU utilization overall and higher origin load for misses.
