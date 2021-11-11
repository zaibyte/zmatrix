# zMatrix

zMatrix is an immutable key-value store focused on trillions micro objects random read performance on multi devices.

## Introduction

It's one of the best local stores as a cache layer in Machine Learning Computing Node for keeping trillions vectors/matrices for:

1. Low read/write amplification.
2. Extremely low memory usage: no memory cache, bits for each entry index.
3. Smart I/O scheduler for squeezing disks performance but avoiding overload.
4. Except user-facing disk I/O operations, nothing is working on block mode (e.g., log)
5. Could be embedded into user applications or using Unix Domain Socket.
6. Supports multi namespaces.
7. Supports multi disks (I won't implement multi disks for one database in present, see this [issue](https://g.tesamc.com/IT/zmatrix/issues/4) for details).

Maybe is the best, not just one of the best.

## Performance

Hundreds of thousands of IOPS for random read with low latency.

About 10 millions k-v pairs: Key is 8 Bytes. Value is 900 Bytes. (1.15Bytes for each k-v pair's memory index)

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/11/11 16:19:17 start to prepare read
2021/11/11 16:19:17 prepare with: 11930419 items
2021/11/11 16:45:11 prepare read done with batch set (512KB each batch), cost: 748.59s (cnt_too_many_req: 244, sleep_for_too_many_req: 732s) for 11930419 items (8B key + 900B value), QPS: 15937.23
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0x384, JobType:"embed", jobType:2, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:10240, GetThreads:128, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"", PrintLog:false}
-------------
summary
-------------
job time: 1753772.70600ms
get: 1310720MB
-------------
get ok: 60111159, failed: 0
iops
get avg: 300.56k/s
-------------
latency
-------------
get min: 65472, avg: 425576.30, max: 18137087
percentiles (nsec):
|  1.00th=[175615],  5.00th=[242431], 10.00th=[279039], 20.00th=[325119],
| 30.00th=[358911], 40.00th=[388351], 50.00th=[416255], 60.00th=[445183],
| 70.00th=[477183], 80.00th=[517375], 90.00th=[578559], 95.00th=[634367],
| 99.00th=[769023], 99.50th=[840703], 99.90th=[1085439], 99.95th=[1292287],
| 99.99th=[2990079]
```

### P.S.

1. The more data, the IOPS higher. Mechanism of parallelism inside NVMe device maybe the reason, need more research.
2. It could reach 500K IOPS with bigger data set.
3. Tested on one NVMe device (Intel P4510) with [zmperf](tools/zmperf)

## Architecture

## Usage

### Basic Concepts

1. Database: database is zMatrix's namespace. Any key is unique in a database. Each database has its own id `(uint32)` and
will be placed on a certain disk driver.

That's all! zMatrix is surprisingly simple for beginners!

### Best Practice

1. Using only one database unless got full error.
2. zMatrix is only built for random access, it won't provide list operation for users. It's a good idea to make keys regular. e.g., <prefix_a>_<timestamp>. 8 Bytes key is perfect.
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

zMatrix is amazing when hold billions files around KB.

Object cannot be bigger than 4 MiB.

#### Design for short-lived entries only

DO Not use it for long-term storage. zMatrix has no physical/logical replicas and there is no completed Silent Data Corruption
protection for data storage in zMatrix.

#### Design for Non Fault Tolerance

Cannot continue operating despite failures or malfunctions. Any serious(e.g., disk I/O) error will cause fatal and cannot give any promise after restart because lacking of mechanism of recovery.

### Other Limitations

1. Maximum key length is 255 bytes. (fixed on 8 bytes is highly recommended!)
2. Maximum capacity of each database is 8 TiB.
3. Maximum entries count of each database is 2^33 (8 billions)
4. Value could not be updated/removed.