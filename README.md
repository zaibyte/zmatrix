# zMatrix

zMatrix is an immutable key-value store focused on trillions micro objects random read performance on multi devices.

## Introduction

It's one of the best local stores as a cache layer in Machine Learning Computing Node for keeping trillions vectors/matrices for:

1. Low read/write amplification.
2. Extremely low memory usage: no memory cache, bits for each entry index.
3. Smart I/O scheduler for squeezing disks performance but avoiding overload.
4. Except user-facing disk I/O operations, nothing is working on block mode (e.g., log)
5. Could be embedded into user applications or using Unix Domain Socket.
6. Supports multi namespaces/databases.
7. Supports multi disks (I won't implement multi disks for one database in present, see this [issue](https://g.tesamc.com/IT/zmatrix/issues/4) for details).

Maybe is the best, not just one of the best.

## Performance

### Memory Overhead

zMatrix is using pure memory as index but only use incredible space, as [test](db/engine/neo/lv0_test.go) shows:

```shell
➜  neo git:(master) ✗ go test -v -run="IdxMemoryUsage"
=== RUN   TestIdxMemoryUsage
    lv1_test.go:39: 4194304 kv will take 4.55 bits for each key as index
--- PASS: TestIdxMemoryUsage (1.08s)
PASS
ok      g.tesamc.com/IT/zmatrix/db/engine/neo   3.534s
```

For 8 bytes length key, each kv will only take 4.55bits as location index (key to disk position).

### I/O

Hundreds of thousands of IOPS for random read with low latency.

For ~100 millions items (8B key, 187B value), 64 threads random read:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/11/24 18:45:20 start to prepare read
2021/11/24 18:45:20 prepare with: 114836108 items
2021/11/24 18:45:20 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"embed", jobType:2, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:64, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200000.29654ms
get: 1310720MB
-------------
get ok: 51885660, failed: 0
iops
get avg: 259.43k/s
-------------
latency
-------------
get min: 59648, avg: 246169.49, max: 7118847
percentiles (nsec):
|  1.00th=[120063],  5.00th=[150143], 10.00th=[167423], 20.00th=[189055],
| 30.00th=[205567], 40.00th=[220543], 50.00th=[235135], 60.00th=[250879],
| 70.00th=[269055], 80.00th=[292863], 90.00th=[332031], 95.00th=[375039],
| 99.00th=[502271], 99.50th=[555519], 99.90th=[697343], 99.95th=[797183],
| 99.99th=[2775039]
```

For ~100 millions items (8B key, 187B value), 128 threads random read:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/11/25 11:08:03 start to prepare read
2021/11/25 11:08:03 prepare with: 114836108 items
2021/11/25 11:08:03 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"embed", jobType:2, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:128, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200002.73022ms
get: 2621440MB
-------------
get ok: 64482769, failed: 0
iops
get avg: 322.41k/s
-------------
latency
-------------
get min: 67968, avg: 396576.94, max: 10657791
percentiles (nsec):
|  1.00th=[183551],  5.00th=[236927], 10.00th=[266495], 20.00th=[303615],
| 30.00th=[331519], 40.00th=[356351], 50.00th=[380671], 60.00th=[406527],
| 70.00th=[435967], 80.00th=[473599], 90.00th=[535551], 95.00th=[603647],
| 99.00th=[814079], 99.50th=[899071], 99.90th=[1106943], 99.95th=[1316863],
| 99.99th=[3071999]
```

### P.S.

1. The more data, the IOPS maybe higher. Mechanism of parallelism inside NVMe device maybe the reason, need more research.
2. Tested on one NVMe device (Intel P4510) with [zmperf](tools/zmperf)

## Architecture

TODO

## Usage

### Basic Concepts

1. Database: database is zMatrix's namespace. Any key is unique in a database. Each database has its own id `(uint32)` and
will be placed on a certain disk driver.

That's all! zMatrix is surprisingly simple for beginners!

### Best Practice

1. Using only one database unless got database full error.
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
3. Maximum entries count of each database is 2^32 (4 billions)
4. Value could not be updated/removed.