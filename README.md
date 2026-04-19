# HedgeDB - _Built for the Hardware_

<p align="center">
<img src="resources/logo.png" width="100%">
</p>

HedgeDB is a prototype of a larger-than-memory, persisted and embeddable key-value storage, inspired by [RocksDB](https://github.com/facebook/rocksdb), optimized for modern high throughput NVME SSDs. It aims to be performance oriented with low memory footprint.

**Disclaimer**: as you might expect from a prototype it was not extensively tested, nor the code can be considered production ready.

So far it is only Linux compatible as it heavily leverage [liburing](https://github.com/axboe/liburing), amongst other Linux syscalls; also having [tcmalloc](https://github.com/google/tcmalloc) installed heavily is recommended.

## Features & design principles

- **io_uring-native coroutine executor**: HedgeDB is built from the ground up around [liburing](https://github.com/axboe/liburing) and C++20 coroutines via the [TooManyCooks](https://github.com/tzcnt/TooManyCooks) work-stealing scheduler. Every I/O operation is a coroutine that suspends on an `io_uring` submission and resumes on its completion — no callbacks jungle, no blocking threads. Each executor thread owns its own `io_uring` ring, so submissions and completions never contend across threads. The result is a write path that reaches NVMe IOPS limits without sacrificing code readability.

- **Partitioned LSM tree with compaction**: The focus of HedgeDB is storing keys with uniform distribution (UUIDs or hashes prefixes). The optimizaion we leaned into is dividing the key space into `2^N` independent partitions (default N=4). Each partition maintains its own level hierarchy in isolation. Background compaction jobs on different partitions are able to fully run in parallel, with very limited shared state. This approach is comparable to sharding, but allows for more flexibility.

- **Size-tiered style compaction** Size-Tiered compaction is used for contained write-amplification. Quotient filter is used to limit unnecessary IOs on read path.

- **Per-thread write path with no hot-path contention**: Each writer thread has its own own WAL file. After the memtable reaches the configured size limit, it gets swapped (double-buffering) to an empty one and the
task of flushing the new one gets deferred to the flusher thread-pool.

- **Direct I/O with a managed index cache**: `O_DIRECT` is available for IO operations, bypassing the OS page cache entirely. This gives the database predictable and transparent memory usage and predictable latency, avoiding IO stalls from huge flushes.

### Future features or improvements

- [ ] Hyper-Clock Cache

- [ ] Key-Value separation

- [ ]

## Dependencies

- Linux kernel `6.14.0-27-generic` or greater

- `gcc 13.3.0` or greater

- `liburing 2.14` (you can launch `install_liburing.sh`)

- `tcmalloc 2.15-3-build1` (`sudo apt install libgoogle-perftools-dev`)

## Benchmarks

`benchtool` is the unified benchmark CLI. Build it with the regular cmake build, then:

```bash
ulimit -n 1048576

# write 10M keys with 100-byte values into /tmp/bench_db
./build/benchtool -m write -n 10000000 -v 100 -p /tmp/bench_db

# read them back (loads existing db at path)
./build/benchtool -m read  -n 10000000 -v 100 -p /tmp/bench_db

# 50/50 mixed read+write
./build/benchtool -m rw    -n 10000000 -v 100 -p /tmp/bench_db

# range scans (small / medium / large tiers)
./build/benchtool -m range -n 10000 -p /tmp/bench_db
```

Flags:

| Flag | Long form | Default | Description |
|------|-----------|---------|-------------|
| `-n` | `--num_ops` | `1000000` | number of operations |
| `-v` | `--vsize` | `100` | value size in bytes |
| `-m` | `--mode` | `write` | `write` \| `read` \| `rw` \| `range` |
| `-p` | `--path` | `/tmp/bench_db` | database directory |

`read`, `rw`, and `range` modes load an existing database from `--path`; if none is found they exit with an error.

### Sample output (24 byte keys, 100 byte values, 100M records, i7-13700H + Samsung 980 Pro 1TB NVMe)

Load:

```bash
$ ./build/benchtool -n 100000000 -m load
=== benchtool ===
mode=load  n=100000000  vsize=100  path="/tmp/bench_db"

--- load ---
Duration:   34023.68 ms
Throughput: 2939129 ops/s
```

Readback:

```bash
$ ./build/benchtool -n 100000000 -m read
=== benchtool ===
mode=read  n=100000000  vsize=100  path="/tmp/bench_db"
[memtable] WAL replay: replayed 141808 entries from 12 WAL files

--- read ---
Duration:   121941.03 ms
Throughput: 820068 ops/s
```

Mixed 50 writes/50 reads:

```bash
$ ./build/benchtool -n 100000000 -m rw
=== benchtool ===
mode=rw  n=100000000  vsize=100  path="/tmp/bench_db"
[memtable] WAL replay: replayed 141808 entries from 12 WAL files

--- rw mixed ---
Duration:   75104.25 ms
Throughput: 1331482 ops/s
```

To summarize, running on a `13th Gen Intel(R) Core(TM) i7-13700H` with 20 threads, 32GB of ram and a `SAMSUNG 980 Pro 1TB`

- **2939K** write/s
- **~820K** read/s
- **~1331K** ops/s

For comparison, here's a `fio` 30-seconds `randread` run (12 jobs) on my machine: it measures
**866k IOPS**. HedgeDB almost gets 100% efficiency.

```bash
$ fio --name=randread     --filename=testfile     --rw=randread     --bs=4k     --iodepth=64     --numjobs=12     --direct=1     --size=20G     --time_based     --runtime=120s     --ioengine=io_uring --group_reporting
randread: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=io_uring, iodepth=64
...
fio-3.36
Starting 12 processes
randread: Laying out IO file (1 file / 20480MiB)
Jobs: 12 (f=12): [r(12)][100.0%][r=4242MiB/s][r=1086k IOPS][eta 00m:00s]
randread: (groupid=0, jobs=12): err= 0: pid=832213: Sun Apr 19 21:38:49 2026
  read: IOPS=866k, BW=3382MiB/s (3546MB/s)(396GiB/120001msec)
    slat (nsec): min=1211, max=27198k, avg=10424.90, stdev=54713.72
    clat (usec): min=14, max=48346, avg=871.67, stdev=554.87
     lat (usec): min=48, max=48349, avg=882.09, stdev=558.72
    clat percentiles (usec):
     |  1.00th=[  289],  5.00th=[  537], 10.00th=[  611], 20.00th=[  676],
     | 30.00th=[  725], 40.00th=[  775], 50.00th=[  807], 60.00th=[  840],
     | 70.00th=[  881], 80.00th=[  922], 90.00th=[ 1004], 95.00th=[ 1221],
     | 99.00th=[ 3687], 99.50th=[ 4228], 99.90th=[ 7570], 99.95th=[ 9372],
     | 99.99th=[13566]
   bw (  MiB/s): min= 2492, max= 4344, per=100.00%, avg=3397.51, stdev=36.02, samples=2856
   iops        : min=638187, max=1112272, avg=869760.18, stdev=9221.09, samples=2856
  lat (usec)   : 20=0.01%, 50=0.01%, 100=0.02%, 250=0.69%, 500=3.16%
  lat (usec)   : 750=31.23%, 1000=54.50%
  lat (msec)   : 2=8.15%, 4=1.68%, 10=0.54%, 20=0.04%, 50=0.01%
  cpu          : usr=13.47%, sys=67.12%, ctx=10814654, majf=0, minf=911
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=103881968,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=3382MiB/s (3546MB/s), 3382MiB/s-3382MiB/s (3546MB/s-3546MB/s), io=396GiB (426GB), run=120001-120001msec

Disk stats (read/write):
    dm-1: ios=103738927/725, sectors=829911416/8168, merge=0/0, ticks=38362185/886, in_queue=38363088, util=100.00%, aggrios=103881973/730, aggsectors=831055784/8168, aggrmerge=0/0, aggrticks=38361092/887, aggrin_queue=38361996, aggrutil=100.00%
    dm-0: ios=103881973/730, sectors=831055784/8168, merge=0/0, ticks=38361092/887, in_queue=38361996, util=100.00%, aggrios=103881973/590, aggsectors=831055784/8168, aggrmerge=0/140, aggrticks=23677369/257, aggrin_queue=23677701, aggrutil=89.92%
  nvme0n1: ios=103881973/590, sectors=831055784/8168, merge=0/140, ticks=23677369/257, in_queue=23677701, util=89.92%
```
