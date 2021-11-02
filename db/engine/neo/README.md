Neo
===

Neo is the first generation database engine for zMatrix, it contains two levels of databases:

1. Write buffer level, for collecting new writing
2. Read only level, for persisting write buffer

## Level0: Write Buffer Level

PebbleDB is the core of write buffer.

## Level1: Read Only Level

It's maintained by segment tree & several trie tree.

### Arch

### Local Storage

#### Segment

##### Block

The min size of block is 8 KiB, it could contain one or more items (at most 8).

```shell
| cnt(2B) | hash(8B) * 8 | offset(2B)_size(4B) * 8 | keys_values...|
```

cnt is the actual items in this block. For most cases the cnt is 1.

hash is xxhash of item's key, will take fixed 8 slots.

offset is key_value's offset from the first byte in this block.

size is value size.

All above is header of block, and all of them are in little endian and will take 114 Bytes.

For keys_values:

```shell
| key1 | value1 | key2 | value2 | ... |
```


## Data Flow

