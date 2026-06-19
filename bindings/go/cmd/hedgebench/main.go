// Command hedgebench is a load/read throughput benchmark for the HedgeDB Go bindings,
// mirroring the C++ benchtool: 24-byte keys, configurable value size, uniformly
// distributed keys, and the make_db_config() engine settings.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"hedgedb"

	"golang.org/x/sys/unix"
)

const (
	keySize = 24
	nValues = 1024
)

// splitmix64 gives a fast, well-distributed 64-bit value from an index.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// writeKey fills the first 8 bytes of a 24-byte key with hash(i); the rest stay zero
// (matching benchtool's make_key layout). buf must be pre-zeroed and reused per worker.
func writeKey(buf []byte, i uint64) {
	binary.LittleEndian.PutUint64(buf[:8], splitmix64(i+1))
}

func valueSlot(i uint64) uint64 { return splitmix64(i+1) % nValues }

func report(label string, n, vsize int, elapsed float64, errs int64) {
	mb := float64(n) * float64(vsize+keySize) / 1e6 / elapsed
	fmt.Printf("\n--- %s ---\n", label)
	fmt.Printf("Duration:   %.1f ms\n", elapsed*1000)
	fmt.Printf("Throughput: %d ops/s\n", uint64(float64(n)/elapsed))
	fmt.Printf("Bandwidth:  %.1f MB/s\n", mb)
	fmt.Printf("Errors:     %d\n", errs)
}

// runWrite drives the load phase with a fixed pool of writers. Each db.Put is wrapped in
// LockOSThread/UnlockOSThread for the duration of the call only; writers are not pinned.
func runWrite(db *hedgedb.DB, n, writers, vsize int, values [][]byte) {
	var errs int64
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := make([]byte, keySize)
			var local int64
			for i := w; i < n; i += writers {
				idx := uint64(i)
				writeKey(key, idx)
				if err := db.Put(key, values[valueSlot(idx)]); err != nil {
					local++
				}
			}
			atomic.AddInt64(&errs, local)
		}(w)
	}
	wg.Wait()
	report("load", n, vsize, time.Since(start).Seconds(), errs)
}

// runRead drives the read phase: many goroutines call db.Get, which dispatches to the reactors.
func runRead(db *hedgedb.DB, n, concurrency, vsize int) {
	var errs int64
	var wg sync.WaitGroup
	start := time.Now()

	for tid := 0; tid < concurrency; tid++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			key := make([]byte, keySize)
			var local int64
			for i := tid; i < n; i += concurrency {
				writeKey(key, uint64(i))
				if v, err := db.Get(key); err != nil || len(v) != vsize {
					local++
				}
			}
			atomic.AddInt64(&errs, local)
		}(tid)
	}
	wg.Wait()
	report("read", n, vsize, time.Since(start).Seconds(), errs)
}

func pinToCPU(cpu int) {
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	_ = unix.SchedSetaffinity(0, &set) // best effort; 0 = calling thread
}

func main() {
	n := flag.Int("n", 50_000_000, "number of objects")
	vsize := flag.Int("v", 100, "value size in bytes")
	path := flag.String("p", "/home/feder/HedgeDB/benchdb_go", "db path (use a real disk, not tmpfs)")
	mode := flag.String("m", "loadread", "load | read | loadread")
	numCtx := flag.Int("ctx", runtime.NumCPU()/2, "number of reactors")
	qd := flag.Int("qd", 16, "io_uring queue depth per reactor")
	conc := flag.Int("c", 0, "read submitter goroutines (0 = ctx*qd*8)")
	writers := flag.Int("w", runtime.NumCPU()/2, "pinned writer goroutines (load phase)")
	pin := flag.Bool("pin", true, "pin reactors/writers to cores")
	directIO := flag.Bool("directio", false, "use O_DIRECT for SST io")
	flag.Parse()

	if *numCtx < 1 {
		*numCtx = 1
	}
	if *writers < 1 {
		*writers = 1
	}
	concurrency := *conc
	if concurrency <= 0 {
		concurrency = *numCtx * *qd * 8
	}

	// Pregenerate NVALUES distinct random values (benchtool style).
	values := make([][]byte, nValues)
	for s := range values {
		b := make([]byte, *vsize)
		rng := rand.New(rand.NewSource(int64(s)))
		rng.Read(b)
		values[s] = b
	}

	var affinity []int
	if *pin {
		affinity = make([]int, *numCtx)
		for i := range affinity {
			affinity[i] = i // first half of the cores: 0..numCtx-1
		}
	}

	opts := hedgedb.Options{
		NumCtx:           *numCtx,
		QueueDepth:       uint32(*qd),
		RequestQueueSize: concurrency,
		NumWriterThreads: *writers, // one writer slot per pinned writer goroutine
		CPUAffinity:      affinity,
		// Mirror benchtool make_db_config():
		MemtableBudgetBytes:      32 << 20,
		PartitionExponent:        4,
		BucketRatio:              1.50,
		CompactionReadAheadBytes: 2 << 20,
		MaxPendingFlushes:        8,
		MinMergeWidth:            8,
		MaxMergeWidth:            32,
		DisableL0Backpressure:    true,
		DisableDirectIO:          !*directIO,
		// WAL on, auto-compaction on (engine defaults)
	}

	create := *mode == "load" || *mode == "loadread"
	if create {
		os.RemoveAll(*path) // make_new requires the path not to exist
	}

	db, err := hedgedb.Open(*path, create, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("HedgeDB Go bench: n=%d vsize=%d ctx=%d qd=%d writers=%d readConc=%d pin=%v path=%s\n",
		*n, *vsize, *numCtx, *qd, *writers, concurrency, *pin, *path)

	if *mode == "load" || *mode == "loadread" {
		runWrite(db, *n, *writers, *vsize, values)
		fmt.Println("\nWaiting for compactions...")
		tc := time.Now()
		db.WaitForCompactions()
		fmt.Printf("compaction drain: %.1f ms\n", time.Since(tc).Seconds()*1000)
	}
	if *mode == "read" || *mode == "loadread" {
		runRead(db, *n, concurrency, *vsize)
	}
}
