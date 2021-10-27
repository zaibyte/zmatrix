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

The major struct for block is the first block, first block will record the count of items in this block, and hashes of keys.

First Block:

```shell
| cnt(2B) | hash(8B) * cnt | offset(2B)_size(4B) * cnt | keys_values...|
```

cnt is the total items in this block. For most cases the cnt is 1, except many items could be put into single 8KB block.

hash is xxhash of item's key.

offset is key_value's offset from the first byte in this block.

size is value size.

All above is header of first block, and all of them are in little endian.

For keys_values:

```shell
| key1 | value1 | key2 | value2 | ... |
```


## Data Flow

