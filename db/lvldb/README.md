LvlDB
===

LvlDB implements db.DB with a multi-level database:

1. lvl0: KV database as write buffer layer
2. lvl1: KV database as read-only layer