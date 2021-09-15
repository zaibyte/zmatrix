URPC
===

URPC implements xrpc by UNIX Domain Socket(UDS).

Unlike ORPC used in other Zai applications, URPC won't use checksum to check data & no timeout for requests for:

1. Internal communication is much reliable
2. ECC memory could help check the data

## Details of I/O mode

In Client Side(each connection):

    write -> waiting server -> read

In Server Side(each connection):

    read -> handler -> write

I choose blocking I/O for:

1. UDS is much faster than disk I/O (except Optane), the throughput won't be more than 1 million IOPS, the async way
   could not help much but brings extra synchronization overhead.
2. Actually it's not pure blocking either because the mechanism of Go chan & epoll.