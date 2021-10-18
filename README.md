# zMatrix

zMatrix is an immutable key-value store focused on trillions micro objects random read performance.

## Introduction

It's one of the best local stores as a cache layer in Machine Learning Computing Node for keeping trillions vectors/matrices for:

1. Almost no read/write amplification.
2. Extremely low memory usage: no memory cache, bits for each entry index.
3. Smart I/O scheduler for squeezing disks performance but avoiding overload.
4. Except user-facing disk I/O operations, nothing is working on block mode (e.g., log)
5. Could be embedded into user applications or using Unix Domain Socket.
6. Supports multi namespaces.
7. Supports multi disks (I won't implement multi disks for one database in present, see this [issue](https://g.tesamc.com/IT/zmatrix/issues/4) for details).

Maybe is the best, not just one of the best.

## Architecture

## Usage

### Basic Concepts

1. Database: database is zMatrix's namespace. Any key is unique in a database. Each database has its own id `(uint32)` and
will be placed on a certain disk driver.

That's all! zMatrix is surprisingly simple for beginners!

### Best Practice

1. Using only one database unless you want to remove entire database or new one is too big to be placed in the same database (disk space is not enough)
2. zMatrix is only built for random access, it won't provide list operation for users. It's a good idea to make keys regular. e.g., <prefix_a>_<timestamp>
3. Sort keys in asc before setting to zMatrix. We could use regular keys and control the setting order to achieve this.

### Operation Guide

### Layout on local file system

#### Disks

```shell
<data_root>/disk_<disk_id>
```

#### Database

```shell
<data_root>/disk_<disk_id>/zmatrix/db_<db_id>
```

### Warn

#### Design for small entries only

DO NOT use it for big objects (> 4 MiB), it's user's responsibility to split big object.
Big object may block disk I/O too long and damage latency of other small requests hugely.

#### Design for short-lived entries only

DO Not use it for long-term storage. zMatrix has no physical/logical replicas and there is no completed Silent Data Corruption
protection for data storage in zMatrix.

### Other Limitations

1. Maximum key length is 255 bytes. (fixed on 8 bytes is highly recommended!)
2. Maximum database count is 16. Using prefix to separate different datasets if there is no enough database.