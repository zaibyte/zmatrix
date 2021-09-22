URPC
===

URPC implements xrpc by UNIX Domain Socket(UDS).

Unlike ORPC used in other Zai applications, URPC won't use checksum to check data & no timeout for requests for:

1. Internal communication is much reliable.
2. ECC memory could help check the data.
3. Client won't cancel request when there are too many requests which are waiting for response.

## Design

It's a standard Go net communication framework:

1. Rely on the ability of Go net which makes async epoll to be in sync way.
2. Two goroutines for one connection: one read, one write

With these optimizations:

1. sync.Pool for reduce GC overhead
2. async request & packet handle for avoiding potential blocking
3. customized binary protocol 

## Acknowledge

[gorpc](https://github.com/valyala/gorpc): urpc's basic network model is from it.