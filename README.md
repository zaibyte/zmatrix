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
7. Supports multi disks (I won't implement multi disks for one database in present).

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
ok      github.com/zaibyte/zmatrix/db/engine/neo   3.534s
```

For 8 bytes length key, each kv will only take 4.55bits as location index (key to disk position).

### I/O

Hundreds of thousands of IOPS for random read with low latency.

For ~100 millions items (8B key, 187B value), 1 threads random read:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/12/01 13:06:10 start to prepare read
2021/12/01 13:06:10 prepare with: 114836108 items
2021/12/01 13:06:10 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"embed", jobType:2, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:1, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"/home/xxx/zmatrix.uds", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200000.10966ms
get: 20480MB
-------------
get ok: 1972560, failed: 0
iops
get avg: 9.86k/s
-------------
latency
-------------
get min: 52416, avg: 101174.33, max: 4155391
percentiles (nsec):
|  1.00th=[58751],  5.00th=[60479], 10.00th=[69119], 20.00th=[76351],
| 30.00th=[78143], 40.00th=[95103], 50.00th=[107647], 60.00th=[108671],
| 70.00th=[109759], 80.00th=[112703], 90.00th=[130751], 95.00th=[161279],
| 99.00th=[189951], 99.50th=[214399], 99.90th=[256511], 99.95th=[281599],
| 99.99th=[439551]
```

For ~100 millions items (8B key, 187B value), 128 threads random read:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/12/01 12:04:21 start to prepare read
2021/12/01 12:04:21 prepare with: 114836108 items
2021/12/01 12:04:21 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"embed", jobType:2, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:128, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"/home/xxx/zmatrix.uds", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200000.40487ms
get: 2621440MB
-------------
get ok: 67277058, failed: 0
iops
get avg: 336.38k/s
-------------
latency
-------------
get min: 59456, avg: 373780.78, max: 13484031
percentiles (nsec):
|  1.00th=[130111],  5.00th=[167295], 10.00th=[194687], 20.00th=[236159],
| 30.00th=[270591], 40.00th=[303359], 50.00th=[336895], 60.00th=[374783],
| 70.00th=[421375], 80.00th=[484607], 90.00th=[591359], 95.00th=[700415],
| 99.00th=[969727], 99.50th=[1106943], 99.90th=[1618943], 99.95th=[2029567],
| 99.99th=[3424255]
```
For ~100 millions items (8B key, 187B value), 1 threads random read through UDS:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/12/01 13:43:01 start to prepare read
2021/12/01 13:43:01 prepare with: 114836108 items
2021/12/01 13:43:01 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"rpc", jobType:1, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:1, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"/home/xxx/zmatrix.uds", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200000.04186ms
get: 20480MB
-------------
get ok: 1234937, failed: 0
iops
get avg: 6.17k/s
-------------
latency
-------------
get min: 88192, avg: 161461.58, max: 4452351
percentiles (nsec):
|  1.00th=[108799],  5.00th=[118143], 10.00th=[124927], 20.00th=[132863],
| 30.00th=[141567], 40.00th=[155263], 50.00th=[161407], 60.00th=[166527],
| 70.00th=[172031], 80.00th=[179455], 90.00th=[197759], 95.00th=[222847],
| 99.00th=[260735], 99.50th=[280319], 99.90th=[326399], 99.95th=[349183],
| 99.99th=[486911]
```

For ~100 millions items (8B key, 187B value), 128 threads random read through UDS:

```shell
xxx@tes_of03:~$ ./zmp -c zmp.toml
2021/12/01 13:52:59 start to prepare read
2021/12/01 13:52:59 prepare with: 114836108 items
2021/12/01 13:52:59 prepare read done with batch set (512KB each batch), cost: 0.00s (cnt_too_many_req: 0, sleep_for_too_many_req: 0s) for 114836108 items (8B key + 187B value), QPS: +Inf
config
-------------
&zmperf.Config{DataRoot:"/home/xxx/zmatrix", ValSize:0xbb, JobType:"rpc", jobType:1, JobTime:200000000000, SkipTime:10000000000, MBPerGetThread:20480, GetThreads:128, IOThreads:128, NopSched:false, IsDoNothing:false, ServerAddr:"/home/xxx/zmatrix.uds", PrintLog:false, PrepareDone:true, IgnoreError:false}
-------------
summary
-------------
job time: 200000.46817ms
get: 2621440MB
-------------
get ok: 54856739, failed: 0
iops
get avg: 274.28k/s
-------------
latency
-------------
get min: 80064, avg: 459232.58, max: 19939327
percentiles (nsec):
|  1.00th=[139391],  5.00th=[171391], 10.00th=[195071], 20.00th=[243199],
| 30.00th=[301055], 40.00th=[360447], 50.00th=[420607], 60.00th=[483839],
| 70.00th=[553983], 80.00th=[640511], 90.00th=[770559], 95.00th=[888831],
| 99.00th=[1156095], 99.50th=[1285119], 99.90th=[1812479], 99.95th=[2310143],
| 99.99th=[3713023]
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

### Build

run `make`

### Best Practice

1. Using only one database unless got database full error.
2. zMatrix is only built for random access, it won't provide list operation for users. It's a good idea to make keys regular. e.g., <prefix_a>_<timestamp>. 8 Bytes key is perfect.
3. Sort keys in asc before setting to zMatrix. We could use regular keys and control the setting order to achieve this.

### Run Deamon

```zmatrix -c config.toml```

### Go

In Go, you could choose embed or RPC model. See [zmperf](tools/zmperf) as example.

### Other Languages

I've implemented a binding lib for zMatrix's client by [a static lib](binding/zmc.so).

Any language which supports binding C could use it.

Or you could choose to develop a client which satisfies the RPC protocol.

#### Python

See [example](examples/client.py) for details.

```shell
xxx@tes_of03:~$ python3 client.py
value_size = 5
get key: hello, got value: b'world'
```

### Operation Guide

### Layout on local file system

disk must be mounted on `disk_<disk_id>`

You could get disk_id by:

e.g.,

```shell
sudo blkid /dev/nvme1n1

/dev/nvme1n1: UUID="b173907c-6b96-4b59-bd20-146e586d10cb" TYPE="xfs"
```

And the UUID is the disk_id.

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