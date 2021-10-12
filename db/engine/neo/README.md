Neo
===

Neo is the first generation database engine for zMatrix, it contains two levels of databases:

1. Write buffer level, for collecting new writing
2. Read only level, for persisting write buffer

## Level0: Write Buffer Level

PebbleDB is the core of write buffer.

## Level1: Read Only Level

It's maintained by hashing & several trie tree.

## Data Flow

