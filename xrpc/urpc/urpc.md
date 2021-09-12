URPC
===

URPC implements xrpc by UNIX Domain Socket.

Unlike ORPC used in other Zai applications, URPC won't use checksum to check data & no timeout for requests for:

1. Internal communication is much reliable
2. ECC memory could help check the data