# HedgeDB

<p align="center">
<img src="resources/logo.png" width="100%">
</p>

HedgeDB is a prototype of a larger-than-memory, persisted and embeddable key-value storage, inspired by [BadgerDB](https://github.com/hypermodeinc/badger) and [RocksDB](https://github.com/facebook/rocksdb), optimized for modern high throughput SSDs. It aims to be performance oriented with low memory footprint.

**Disclaimer**: as you might expect from a prototype it was not extensively tested, nor the code can be considered production ready. _Also, the code itself might have gotten a bit messy. Hopefully, I'll take care of this in a second time._

So far it is only Linux compatible as it heavily leverage [liburing](https://github.com/axboe/liburing), amongst other Linux syscalls.

## Features

### Implemented

- **Exclusive UUIDv4 support**: So far, this storage is strictly optimized just to accept UUIDv4 (16 bytes) keys. The Index design follows the assumption that the keys are randomly distrubuted over the key space. In future might support smaller or up to 48 bytes sized keys.

- **Separated Index and Value files**: Following the state of the art design of [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), the HedgeDB's LSM tree is implemented through the separation between Index and Value files, allowing for an efficient look-up and compaction.

- **Online and latch-free compaction**: Index compaction is automatically triggered when certain criteria are met. Also, when the compaction is running, the database will keep being available.

- **Fiber-style job scheduling**: A brand new and custom `executor` has been implemented for maximizing the SSD usage, leveraging concurrency and C++20 coroutines to fully utilize a modern NVMe SSD while exposing an API that is familiar to many developers.

#### TODO

- [ ] **Values garbage collection**: (in progress) space from deleted values is not freed yet.

- [ ] **Key/Value update**: Key update feature should follow the deletion right away, but a value update was not implemented yet.

- [ ] **Multi-threaded executor**: the fiber executor runs every operation on a single thread, but one thread is not enough to fully stress a modern NVME.

- [ ] **Write-ahead log & crash recovery**: if any fault occur during write, there might be some data loss and/or the database will end up in an undefined state. This feature is fundamental for a production-ready storage.

- [ ] **General purpose API**: currently the `database` class API is only usable within a `async::task<void>` (which can be spawned on the `executor`); a callback-based API would improve the database usability.

## How to compile it

Other than `liburing` (which you can install with `sudo apt install liburing`), HedgeDB just needs to be compiled with a modern C++ compiler.

However I've been working on:

- Linux kernel `6.14.0-27-generic` on Ubuntu 24.04.02 LTS

- `gcc 13.3.0`

- `liburing 2.5`

One of the core design element is to avoid 3rd party dependencies as much as possible. In any case, I'm including everything within this repository and I will try to keep with this.

For compiling with `cmake`:

```bash
cmake . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev && cmake --build build -j$(nproc)
```

Of course you can change build type to `Debug`, or change the number of jobs.

## Benchmarks

You can run the `database_test` for a write/read test with 75M 1KB records. This test is going to be time-measured, though.

Might be useful to raise the number of file descriptors, though:

```bash
ulimit -n 1048576
```

Then, for starting the test:

```bash
$ ./build/database_test
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from test_suite/database_test
[ RUN      ] test_suite/database_test.database_comprehensive_test/N_75000000_P_1024_C_1000000
[LOGGER] Launching io executor. Queue depth: 128 Max buffered tasks: 512
[database] Flushing mem index to "/tmp/db/indices" with number of items: 1000232
[database] Starting compaction job
...
...
...
Total duration for insertion: 318637 ms
Average duration per insertion: 4.2485 us
Insertion bandwidth: 235.377 MB/s
[database] Starting compaction job
Total duration for compaction: 8959.92 ms
Total duration for retrieval: 202777 ms
Average duration per retrieval: 2.70369 us
Retrieval bandwidth: 369.865 MB/s
Closing uring_reader.
[       OK ] test_suite/database_test.database_comprehensive_test/N_75000000_P_1024_C_1000000 (530521 ms)
[----------] 1 test from test_suite/database_test (530521 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (530521 ms total)
[  PASSED  ] 1 test.
```

To summarize, running on a `13th Gen Intel(R) Core(TM) i7-13700H` with 32GB and a `SAMSUNG MZVL21T0HDLU-00BLL` SSD:

- **~370.000** reads/s
- **~235.000** write/s including compaction

For comparison, here's a single threaded `fiotest` 30-seconds run on my machine:

```bash
/tmp$ fio --name=randread     --filename=testfile     --rw=randread     --bs=4k     --iodepth=128     --numjobs=1     --direct=1     --size=20G     --time_based     --runtime=30s     --ioengine=io_uring --group_reporting
randread: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=io_uring, iodepth=128
fio-3.36
Starting 1 process
Jobs: 1 (f=1): [r(1)][100.0%][r=1407MiB/s][r=360k IOPS][eta 00m:00s]6s]
randread: (groupid=0, jobs=1): err= 0: pid=2485307: Sat Aug 30 17:17:26 2025
  read: IOPS=357k, BW=1396MiB/s (1463MB/s)(40.9GiB/30001msec)
    slat (nsec): min=1088, max=139801, avg=2491.74, stdev=1488.34
    clat (usec): min=53, max=9306, avg=355.50, stdev=44.14
     lat (usec): min=54, max=9311, avg=357.99, stdev=44.18
    clat percentiles (usec):
     |  1.00th=[  314],  5.00th=[  322], 10.00th=[  326], 20.00th=[  334],
     | 30.00th=[  338], 40.00th=[  343], 50.00th=[  351], 60.00th=[  355],
     | 70.00th=[  363], 80.00th=[  375], 90.00th=[  392], 95.00th=[  412],
     | 99.00th=[  457], 99.50th=[  478], 99.90th=[  578], 99.95th=[  685],
     | 99.99th=[  996]
   bw (  MiB/s): min= 1277, max= 1423, per=100.00%, avg=1395.90, stdev=41.65, samples=59
   iops        : min=327128, max=364334, avg=357351.17, stdev=10661.98, samples=59
  lat (usec)   : 100=0.01%, 250=0.02%, 500=99.69%, 750=0.27%, 1000=0.01%
  lat (msec)   : 2=0.01%, 10=0.01%
  cpu          : usr=15.45%, sys=84.47%, ctx=1798, majf=0, minf=148
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwts: total=10719278,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128

Run status group 0 (all jobs):
   READ: bw=1396MiB/s (1463MB/s), 1396MiB/s-1396MiB/s (1463MB/s-1463MB/s), io=40.9GiB (43.9GB), run=30001-30001msec

Disk stats (read/write):
    dm-1: ios=10682351/239, sectors=85458816/2160, merge=0/0, ticks=852438/100, in_queue=852538, util=99.70%, aggrios=10719288/241, aggsectors=85754312/2184, aggrmerge=0/0, aggrticks=851889/100, aggrin_queue=851989, aggrutil=99.64%
    dm-0: ios=10719288/241, sectors=85754312/2184, merge=0/0, ticks=851889/100, in_queue=851989, util=99.64%, aggrios=10719288/198, aggsectors=85754312/2184, aggrmerge=0/43, aggrticks=811677/104, aggrin_queue=811821, aggrutil=79.33%
  nvme0n1: ios=10719288/198, sectors=85754312/2184, merge=0/43, ticks=811677/104, in_queue=811821, util=79.33%
```
