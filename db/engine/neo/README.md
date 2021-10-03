Neo
===

Neo is the first generation database engine for zMatrix, it contains two levels of databases:

1. Write buffer level, for collecting new writing
2. Read only level, for persisting write buffer

## Write Buffer Level

PebbleDB is the core of write buffer.

## Read Only Level

It's maintained by hashing & several trie tree.

## Data Flow

