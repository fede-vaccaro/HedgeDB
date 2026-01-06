# HedgeDB - _Built for the Hardware_

<p align="center">
<img src="resources/logo.png" width="100%">
</p>

HedgeDB is a prototype of a larger-than-memory, persisted and embeddable key-value storage, inspired by [BadgerDB](https://github.com/hypermodeinc/badger) and [RocksDB](https://github.com/facebook/rocksdb), optimized for modern high throughput SSDs. It aims to be performance oriented with low memory footprint.

**Disclaimer**: as you might expect from a prototype it was not extensively tested, nor the code can be considered production ready.

So far it is only Linux compatible as it heavily leverage [liburing](https://github.com/axboe/liburing), amongst other Linux syscalls; also [tcmalloc](https://github.com/google/tcmalloc) is recommended.

## Features & design principles

- **Fiber-style job scheduling**: A brand new and custom `executor` has been implemented to provide an abstraction layer over the [liburing](https://github.com/axboe/liburing) for maximizing the available hardware IOPS; C++20 coroutines and other concurrency facilities have been exploited to strees a modern NVMe SSD at full capacity, while still exposing an API that is familiar to many developers.

- **Separated Index and Value files**: Following the state of the art design of [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), the HedgeDB's LSM tree is implemented through the separation between Index and Value files, allowing for an efficient look-up and compaction.

- **Latch-free**: Write and read paths are designed to be almost lock-free: value table instantiation, data flush, compaction, cache access, every operation is written to be lock free or at least to lock for very few operations.

- **Lightweight dependencies**: One of the core principles of this project is being self-contained as much as possible, escaping the dependency hell that plagues software today.

- **Memory Aware design**: Memory consumption and performance should be predictable: the user is able to control the trade-off between memory consumption and performance. Also, the database is meant to be transparent in regards of the OS buffered I/O (files can be opened with `O_DIRECT`).

### TODOs: _(in random order of priority)_

- [ ] **Abitrary sized-keys**: 16 byte keys might be a commond use case, but it's too limiting; for the sake of prototyping I sticked to this case

- [ ] **Values garbage collection**: space from deleted values is never reclaimed.

- [ ] **Key/Value update**: Key update should follow the same logic for Key deletion, but it's not been tested.

- [x] **Multi-threaded executor**: the fiber executor runs every operation on a single thread, but one thread is not enough to fully stress a modern NVME.

- [ ] **Write-ahead Log (WAL) & crash recovery**: if any fault occur during write, there might be some data loss and/or the database will end up in an undefined state. This feature is fundamental for a production-ready storage.

- [x] **General purpose API**: currently the `database` class API is only usable within a `async::task<void>` (which can be spawned on the `executor`); a callback-based API would improve the database usability.

- [ ] **Add arguments to the example**: just for sandboxing & testing.

- [ ] Optional **Bloom filters** in front of each `sorted_index`: by using such filters, we can reduce write amplification (by accepting more tables in the same group with minor compaction pressure) and keeping a low read amplification, by trading-off some memory.

- [x] RC-CLOCK cache (Reference-Counted CLOCK) cache (currently only implemented for the Index).

- [ ] Merge procedure refactoring: I believe it is the most complicated code within this project: it must to be refactored soon or later.

- [ ] Ranged iterators: the use case I focused on is random look-ups; although, ranged search is supported from every database and could be supported here too.

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

You can run the `database_test` for a write/read test with 75M 1KB records. This test is going to be time-measured, though.

Might be useful to raise the number of file descriptors, though:

```bash
ulimit -n 1048576
```

For starting a benchmark with 80M key (16 bytes) values (1024 bytes):

```bash
Running main() from /home/federico/db/HedgeDB/build/_deps/googletest-src/googletest/src/gtest_main.cc
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
[ RUN      ] test_suite/database_test.database_comprehensive_test/N_80000000_P_1024_C_2000000
Using CLOCK cache. Allocated space: 3072MB
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
[LOGGER] Launching io executor. Queue depth: 32 Max buffered tasks: 128
Total duration for key generation: 2573.61 ms
Generated 1220.7 MB of keys. This data will be held in memory.
[database] Flushing mem index to "/tmp/db/indices"
[database] Mem index flushed in 466.112 ms
[database] Starting compaction job
[...]
Total duration for insertion: 112339 ms
Average duration per insertion: 1.40423 us
Insertion bandwidth: 695.441 MB/s
Insertion throughput: 712131 items/s
Deleted keys: 0
[database] Starting compaction job
[database] Compaction job completed successfully
[database] Total duration for compaction: 2478.88 ms
Total duration for a full compaction: 2478.91 ms
Shufflin keys for randread test...
Syncing FDs...
Sleeping 1 seconds before starting retrieval to cool down...
Read test - before cache warm up
Starting readback now with 10000000 keys.
Total duration for retrieval: 12112.1 ms
Average duration per retrieval: 1.21121 us
Retrieval throughput: 825619 items/s
Retrieval bandwidth: 825.619 MB/s
Read test - after cache warm up
Starting readback now with 10000000 keys.
Total duration for retrieval: 12011.1 ms
Average duration per retrieval: 1.20111 us
Retrieval throughput: 832565 items/s
Retrieval bandwidth: 832.566 MB/s
[       OK ] test_suite/database_test.database_comprehensive_test/N_80000000_P_1024_C_2000000 (147868 ms)
[----------] 1 test from test_suite/database_test (147868 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (147870 ms total)
[  PASSED  ] 1 test.
```

To summarize, running on a `13th Gen Intel(R) Core(TM) i7-13700H` with 8 threads, 32GB of ram and a `SAMSUNG MZVL21T0HDLU-00BLL` SSD:

- **~825.000** reads/s (3GB for index cache)
- **~712.000** write/s

For comparison, here's a single threaded `fio` 30-seconds run on my machine: it records
**841k IOPS** on my device.

```bash
/tmp$ fio --name=randread     --filename=testfile     --rw=randread     --bs=4k     --iodepth=128     --numjobs=8     --direct=1     --size=20G     --time_based     --runtime=30s     --ioengine=io_uring --group_reporting
randread: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=io_uring, iodepth=128
...
fio-3.36
Starting 8 processes
Jobs: 8 (f=8): [r(8)][100.0%][r=3287MiB/s][r=841k IOPS][eta 00m:00s]
randread: (groupid=0, jobs=8): err= 0: pid=550915: Mon Dec 22 20:10:24 2025
  read: IOPS=840k, BW=3283MiB/s (3443MB/s)(96.2GiB/30002msec)
    slat (nsec): min=1194, max=1038.6k, avg=3510.93, stdev=2160.09
    clat (usec): min=111, max=9587, avg=1214.27, stdev=288.30
     lat (usec): min=124, max=9594, avg=1217.78, stdev=288.29
    clat percentiles (usec):
     |  1.00th=[  330],  5.00th=[  717], 10.00th=[  979], 20.00th=[ 1057],
     | 30.00th=[ 1106], 40.00th=[ 1156], 50.00th=[ 1205], 60.00th=[ 1254],
     | 70.00th=[ 1303], 80.00th=[ 1385], 90.00th=[ 1532], 95.00th=[ 1680],
     | 99.00th=[ 2008], 99.50th=[ 2147], 99.90th=[ 2507], 99.95th=[ 2704],
     | 99.99th=[ 4686]
   bw (  MiB/s): min= 3164, max= 3355, per=100.00%, avg=3284.92, stdev= 4.26, samples=472
   iops        : min=810168, max=859132, avg=840940.37, stdev=1090.28, samples=472
  lat (usec)   : 250=0.36%, 500=2.22%, 750=2.81%, 1000=6.42%
  lat (msec)   : 2=87.14%, 4=1.04%, 10=0.01%
  cpu          : usr=9.23%, sys=42.22%, ctx=9872036, majf=0, minf=1113
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.1%
     issued rwts: total=25216190,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=128

Run status group 0 (all jobs):
   READ: bw=3283MiB/s (3443MB/s), 3283MiB/s-3283MiB/s (3443MB/s-3443MB/s), io=96.2GiB (103GB), run=30002-30002msec

Disk stats (read/write):
    dm-1: ios=25127635/59, sectors=201022488/496, merge=0/0, ticks=30366683/39, in_queue=30366722, util=99.70%, aggrios=25216452/59, aggsectors=201733024/496, aggrmerge=0/0, aggrticks=30463880/39, aggrin_queue=30463919, aggrutil=99.64%
    dm-0: ios=25216452/59, sectors=201733024/496, merge=0/0, ticks=30463880/39, in_queue=30463919, util=99.64%, aggrios=25216377/54, aggsectors=201733024/496, aggrmerge=75/5, aggrticks=30379911/22, aggrin_queue=30379936, aggrutil=87.73%
  nvme0n1: ios=25216377/54, sectors=201733024/496, merge=75/5, ticks=30379911/22, in_queue=30379936, util=87.73%
```
