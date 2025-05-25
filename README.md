## HedgehogDB

<p align="center">
<img src="resources/logo.png" width="50%">
</p>

HedgehogDB is a prototype, write-intensive key-value store inspired by [BadgerDB](https://github.com/hypermodeinc/badger) and [RocksDB](https://github.com/facebook/rocksdb), specifically optimized for Hard Disk Drives (HDDs). It aims for simplicity in its design and implementation, with also the goal of having low (or at least, predictable) memory footprint.


**Disclaimer**: as you might expect from a prototype it was not extensively tested, nor the code is threadsafe or production ready.

So far it is only Linux compatible.

## Features

- **Exclusive UUIDv4 support**: It is optimized just to accept UUIDv4 (only 16 bytes) keys. 
- **Separate Index and Values**: Index and Values are stored in separate files. This allows the index to be held in memory (2M entries fits in 64MB). However, when flushed to the storage, the keys are sorted to allow a quick look-up (through binary-search).
- **Crash recovery**: When the `insert` operation is called, data are not buffered and are immediately written on disk (still leveraging the support of Linux Async I/O)
- **HDD Optimized**: Designed with sequential write patterns in mind, which are efficient for HDDs.
- Memtable & Sorted String Table (SST) Architecture:
  - `memtable_db`: Handles incoming writes, storing key-value pointers in an unordered map and appending data to a value log.
  - `sortedstring_db`: Represents a flushed, immutable dataset where keys are sorted for efficient reads using memory-mapped files and binary search.
  - **Bloom filter**: when the DB growths in number of entries, the bloom filters are leveraged to speed-up the look-up operation
  - **Compaction**: it is not automatically triggered, in order to let the resources focus on the writes, and it is meant only to free up some space.
  - **Tombstone Mechanism**: Manages deleted keys to ensure they are not retrieved and are eventually removed during compaction.
  - **Memory-Mapped** Files: Utilizes `mmap` for efficient access to the sorted index file in sortedstring_db.

### How to compile it

For trying a single table:

```
g++ -std=c++20 -O2 main.cc database.cc MurmurHash3.cc bloom_filter.cc tables.cc -o hedgehog_table -I. -I./include
```

For running it:

```
./hedgehog_table <NUM_ITEMS> <BASE_PATH> <VALUE_SIZE_BYTES>
```

Possible output (on NVME) when writing 2M values of 50KB:

```
mkdir /tmp/hedgehog/
$ ./hedgehog_table 2000000 /tmp/hedgehog 51200
version 0.0.1a
N_FILES: 2000000
VALUE_SIZE: 51200
Key generated. n keys: 2000000
Elapsed time for writes: 27389 ms
Bandwidth: 3565.53 MB/s
Elapsed time for flush: 1885 ms
Elapsed time for reads: 112 ms
Reads/sec 8937.5
Deleted key: 48cc58b6-7bfb-42c8-a1b1-ea98ae0b4ffb
Deleted key: a7afbb40-2914-4697-9717-5857460ef120
Deleted key: 9382f649-8eca-4345-a42a-ff1f6377fbc1
Deleted key: 4c7f93ee-2e0a-4045-ad21-724d5ea403b2
Deleted key: f7e81d08-b514-415c-b10e-0aab0aeb1f5c
Elapsed time for deletes: 0 ms
```

For trying the database wrapper:

```
g++ -std=c++20 -O2 main_database.cc database.cc MurmurHash3.cc bloom_filter.cc tables.cc -o hedgehog_db -I. -I./include
```
