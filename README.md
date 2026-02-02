# HedgeDB - _Built for the Hardware_

<p align="center">
<img src="resources/logo.png" width="100%">
</p>

HedgeDB is a prototype of a larger-than-memory, persisted and embeddable key-value storage, inspired by [RocksDB](https://github.com/facebook/rocksdb), optimized for modern high throughput NVME SSDs. It aims to be performance oriented with low memory footprint.

**Disclaimer**: as you might expect from a prototype it was not extensively tested, nor the code can be considered production ready.

So far it is only Linux compatible as it heavily leverage [liburing](https://github.com/axboe/liburing), amongst other Linux syscalls; also having [tcmalloc](https://github.com/google/tcmalloc) installed heavily is recommended.

## Features & design principles

- **Fiber-style job scheduling**: A brand new and custom `executor` has been implemented to provide an abstraction layer over the [liburing](https://github.com/axboe/liburing) for maximizing the available hardware IOPS capacity; C++20 coroutines and other concurrency facilities have been exploited to strees a modern NVMe SSD at full capacity, while keeping a more linear coding style, instead of unravel the typical callback jungle.

- **Log Structured Merge (LSM) Tree**: The index is basically a LSM Trees: the NVMEs are great at executing random scattered operations, but they nonetheless enjoy sequential workloads. Also, the LSM Tree-based index architecture has been proved to be flexible over different workload or even device (such as HDD) types.  

- **Separated Index and Value files**: Following the state of the art design of [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), the HedgeDB's LSM tree is implemented through the separation between Index and Value files, allowing for an efficient look-up and compaction.

- **Latch-free**: Write and read paths are designed to be almost lock-free: value table instantiation, data flush, compaction, cache access, every operation is written to be lock free or to at least with very little contention.

- **Lightweight dependencies**: One of the core principles of this project is being self-contained as much as possible, escaping the _dependency hell™_ that plagues software today.

- **Memory Aware design**: Memory consumption and performance should be predictable: the user is able to control the trade-off between memory consumption and performance. Also, the database is meant to be transparent in regards of the OS buffered I/O (files can be opened with `O_DIRECT`).

### TODOs: _(in random order of priority)_

- [ ] **Abitrary sized-keys**: 16 byte keys might be a commond use case, but it's too limiting; for the sake of prototyping I stuck to this case.

- [ ] **Values garbage collection**: space from deleted values is never reclaimed.

- [ ] **Key/Value update**: Key update should follow the same logic for Key deletion, but it hasn't been tested yet.

- [x] **Multi-threaded executor**: the fiber executor runs every operation on a single thread, but one thread is not enough to fully stress a modern NVME.

- [ ] **Write-ahead Log (WAL) & crash recovery**: if any fault occur during write, there might be some data loss and/or the database will end up in an undefined state. This feature is fundamental for a production-ready storage.

- [x] **General purpose API**: currently the `database` class API is only usable within a `async::task<void>` (which can be spawned on the `executor`); a callback-based API would improve the database usability.

- [ ] **Add arguments to the example**: just for sandboxing & testing.

- [ ] Optional **Quotient/Bloom filters** in front of each `sorted_index`: by using such filters, we can reduce write amplification (by accepting more tables in the same group with minor compaction pressure) and keeping a low read amplification, by trading-off some memory.

- [x] RC-CLOCK cache (Reference-Counted CLOCK) cache (currently only implemented for the Index).

- [ ] Merge procedure refactoring: I believe it is the most complicated code within this project: soon or later, it must to be refactored.

- [ ] Ranged iterators: the use case I focused on is random look-ups; although, ranged search is supported from every database and should be supported here too.

## Compile & install

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

Finally, you can install it in the `./hedgedb` directory with:

```bash
cmake --install build --prefix hedgedb
```

## Running the example

After having built and installed the library, you can build and run the example:

```bash
cd example
cmake . -B build && cmake --build build
ulimit -n 1048576 
./build/example
```

It will attempt to create the db in `/tmp/db`. It will delete the directory at the next start.

From the example, you can also understand the basic DB operators.

_You might need to run the example with `sudo`_

## Benchmarks

You can run the `database_test` for a write/read test with 80M of 1KB records.

Might be useful to raise the number of file descriptors, though:

```bash
ulimit -n 1048576
```

For starting a benchmark with 80M key (16 bytes) values (1024 bytes):

```bash
$ ./build/database_test

[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from test_suite/database_test
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[LOGGER] Launching io executor. Queue depth: 64 Max buffered tasks: 256
[...]
[ RUN      ] test_suite/database_test.database_comprehensive_test/N_80000000_P_1024_C_2000000
Using CLOCK cache. Allocated space: 3221.23MB
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
Total duration for key generation: 2541.9 ms
Generated 1280 MB of keys. This data will be held in memory.
[database] Flushing mem index 0 to "/tmp/db/indices" with 2020508 keys
[database] Prepared new value table in 1.709 ms
[database] Mem index flushed in 501.826 ms
[database] Compaction job submitted to background worker.
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Flushing mem index 2 to "/tmp/db/indices" with 1991067 keys
[database] Mem index flushed in 489.38 ms
[database] Compaction job submitted to background worker.
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Flushing mem index 4 to "/tmp/db/indices" with 1999363 keys
[database] Prepared new value table in 2.205 ms
[database] Mem index flushed in 425.114 ms
[database] Compaction job submitted to background worker.
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Total duration for compaction: 127.042 ms, MB written: 181.174, throughput: 1426.1 MB/s
[database] Flushing mem index 6 to "/tmp/db/indices" with 2005272 keys
[database] Prepared new value table in 2.16 ms
[database] Mem index flushed in 466.305 ms
[database] Compaction job submitted to background worker.
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Total duration for compaction: 37.546 ms, MB written: 16.085, throughput: 428.408 MB/s
[database] Flushing mem index 8 to "/tmp/db/indices" with 1998949 keys
[database] Mem index flushed in 419.819 ms
[database] Compaction job submitted to background worker.
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Total duration for compaction: 43.858 ms, MB written: 24.15, throughput: 550.641 MB/s
[...]
Total duration for insertion: 49385.1 ms
Average duration per insertion: 0.617313 us
Insertion bandwidth: 1684.72 MB/s
Insertion throughput: 1619922 items/s
Deleted keys: 0
Triggering full compaction
[database] [Compaction job submitted to background worker.database
] Starting compaction job
[database] Compaction job completed successfully
[database] Total duration for compaction: 2257.95 ms, MB written: 2349.24, throughput: 1040.43 MB/s
Total duration for a full compaction: 2258.08 ms
Compaction throughput: 1040.37 MB/s
Insertion throughput including compaction:1549092 items/s
Shufflin keys for randread test...
Syncing FDs...
Sleeping 1 seconds before starting retrieval to cool down...
Read test - before cache warm up
Starting readback now with 10000000 keys.
Total duration for retrieval: 11339.7 ms
Average duration per retrieval: 1.13397 us
Retrieval throughput: 881854 items/s
Retrieval bandwidth: 903.019 MB/s
Read test - after cache warm up
[----------] 1 test from test_suite/database_test (83020 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (83022 ms total)
[  PASSED  ] 1 test.
federico@federico-ThinkPad:~/db/HedgeDB$ 
```

To summarize, running on a `13th Gen Intel(R) Core(TM) i7-13700H` with 20 threads, 32GB of ram and a `SAMSUNG MZVL21T0HDLU-00BLL` Nvme:

- **~881K** reads/s (nb, 3GB are destinated for caching the index)
- **~1549K** write/s

For comparison, here's a `fio` 30-seconds `randread` run (16 jobs) on my machine: it records
**896k IOPS**.

```bash
$ fio --name=randread     --filename=testfile     --rw=randread     --bs=4k     --iodepth=64     --numjobs=16     --direct=1     --size=40G     --time_based     --runtime=30s     --ioengine=io_uring --group_reporting
randread: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=io_uring, iodepth=64
...
fio-3.36
Starting 16 processes
Jobs: 16 (f=16): [r(16)][100.0%][r=3498MiB/s][r=896k IOPS][eta 00m:00s]
randread: (groupid=0, jobs=16): err= 0: pid=1310102: Fri Jan 30 00:24:02 2026
  read: IOPS=894k, BW=3493MiB/s (3662MB/s)(102GiB/30003msec)
    slat (nsec): min=1201, max=1467.6k, avg=3551.93, stdev=2278.32
    clat (usec): min=81, max=7675, avg=1141.10, stdev=706.33
     lat (usec): min=84, max=7677, avg=1144.65, stdev=706.28
    clat percentiles (usec):
     |  1.00th=[  210],  5.00th=[  306], 10.00th=[  400], 20.00th=[  562],
     | 30.00th=[  717], 40.00th=[  857], 50.00th=[  996], 60.00th=[ 1139],
     | 70.00th=[ 1336], 80.00th=[ 1598], 90.00th=[ 2057], 95.00th=[ 2540],
     | 99.00th=[ 3556], 99.50th=[ 3949], 99.90th=[ 4883], 99.95th=[ 5211],
     | 99.99th=[ 5932]
   bw (  MiB/s): min= 3348, max= 3637, per=100.00%, avg=3494.56, stdev= 3.95, samples=944
   iops        : min=857246, max=931242, avg=894606.98, stdev=1011.23, samples=944
  lat (usec)   : 100=0.01%, 250=2.44%, 500=13.55%, 750=16.40%, 1000=17.98%
  lat (msec)   : 2=38.64%, 4=10.54%, 10=0.46%
  cpu          : usr=6.49%, sys=24.93%, ctx=15800422, majf=0, minf=1163
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=26826207,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=3493MiB/s (3662MB/s), 3493MiB/s-3493MiB/s (3662MB/s-3662MB/s), io=102GiB (110GB), run=30003-30003msec

Disk stats (read/write):
    dm-1: ios=26728546/70, sectors=213828368/552, merge=0/0, ticks=30398263/49, in_queue=30398312, util=100.00%, aggrios=26826215/70, aggsectors=214609720/552, aggrmerge=0/0, aggrticks=30498256/49, aggrin_queue=30498305, aggrutil=100.00%
    dm-0: ios=26826215/70, sectors=214609720/552, merge=0/0, ticks=30498256/49, in_queue=30498305, util=100.00%, aggrios=26826211/63, aggsectors=214609720/552, aggrmerge=4/7, aggrticks=30401085/46, aggrin_queue=30401138, aggrutil=84.50%
  nvme0n1: ios=26826211/63, sectors=214609720/552, merge=4/7, ticks=30401085/46, in_queue=30401138, util=84.50%
```
