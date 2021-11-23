Neo
===

Neo is the first generation database engine for zMatrix, it contains two levels of databases:

1. Write buffer level, for collecting new writing
2. Read only level, for persisting write buffer

## Level0: Write Buffer Level

All things of lv0 are based on PebbleDB. It's a reliable KV database, and which I've done was just a simple wrap, that's why there is no testing about it :D.

## Level1: Read Only Level

It's maintained by segment tree & several trie tree.

### Arch

### Local Storage

#### Segment

##### Block

Every item will be written to disk in block:

```shell
| key_len(1B) | value_len(4B) | key | value | ... |
```

There maybe multi items share the same offset, then sequential search the block.

## Data Flow

