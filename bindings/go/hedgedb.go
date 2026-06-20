// Package hedgedb provides Go bindings for HedgeDB via its C API.
//
// Each underlying db_ctx is a per-thread io_uring reactor and must be driven from a
// single OS thread, so every Ctx runs one goroutine pinned with runtime.LockOSThread
// that owns all C calls for that ctx. User goroutines submit work over a channel and
// block on a 1-buffered reply channel; the owner goroutine ticks the reactor and
// delivers outcomes by sequence number.
package hedgedb

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
// -ljemalloc is listed before the implicit -lc so its malloc/free interpose libc's for the whole
// process (the engine's C++ allocations go through jemalloc; Go's own heap uses mmap regardless).
#cgo LDFLAGS: -L${SRCDIR}/../../build -lhedgedb_c -lstdc++ -Wl,-rpath,${SRCDIR}/../../build
#include <stdlib.h>
#include "hedge_c.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	ErrKeyNotFound = errors.New("hedgedb: key not found")
	ErrDeleted     = errors.New("hedgedb: key deleted")
	ErrBusy        = errors.New("hedgedb: busy")
)

func statusErr(code int) error {
	if code == int(C.HEDGE_OK) {
		return nil
	}
	return statusError(code)
}

func statusError(code int) error {
	switch code {
	case int(C.HEDGE_ERR_KEY_NOT_FOUND):
		return ErrKeyNotFound
	case int(C.HEDGE_ERR_DELETED):
		return ErrDeleted
	case int(C.HEDGE_ERR_BUSY):
		return ErrBusy
	default:
		return fmt.Errorf("hedgedb: error code %d", code)
	}
}

// Options configures a DB. Zero/false fields fall back to engine defaults.
type Options struct {
	NumCtx           int    // number of per-thread reactors (default 4)
	QueueDepth       uint32 // io_uring queue depth per ctx (default 64)
	NumWriterThreads int    // memtable writer slots; forced up to at least NumCtx
	RequestQueueSize int    // per-ctx request buffer (default 1024)

	// Engine (db_config) knobs; 0/false means "use the engine default".
	MemtableBudgetBytes      int
	PartitionExponent        int
	BucketRatio              float64
	CompactionReadAheadBytes int
	MaxPendingFlushes        int
	MinMergeWidth            int
	MaxMergeWidth            int
	IndexPageCacheBytes      int
	NumBackgroundWorkers     int
	DisableDirectIO          bool
	DisableWAL               bool
	DisableAutoCompaction    bool
	DisableL0Backpressure    bool

	// CPUAffinity optionally pins reactor i to CPUAffinity[i] (one entry per ctx).
	CPUAffinity []int
}

type result struct {
	value []byte
	err   error
}

// request is a pending read handed to a reactor's owner goroutine. Writes do not use this.
type request struct {
	key   []byte
	reply chan result
}

// DB is a handle to a shared HedgeDB store fronted by a fixed pool of reactors.
type DB struct {
	cdb  *C.hedge_db
	ctxs []*Ctx
	next uint64
}

// Ctx owns a single db_ctx and the goroutine that drives it.
type Ctx struct {
	db    *DB
	cctx  *C.hedge_ctx
	cpu   int // CPU to pin to, or -1
	reqCh chan *request
	quit  chan struct{}
	done  chan struct{}
}

// Open creates (create=true, path must not exist) or loads (create=false) a database.
func Open(path string, create bool, opts Options) (*DB, error) {
	if opts.NumCtx <= 0 {
		opts.NumCtx = 4
	}
	if opts.QueueDepth == 0 {
		opts.QueueDepth = 64
	}
	if opts.RequestQueueSize <= 0 {
		opts.RequestQueueSize = 1024
	}
	writerThreads := opts.NumWriterThreads
	if writerThreads < opts.NumCtx {
		writerThreads = opts.NumCtx // keep one writer slot per reactor (rw_sync / WAL invariant)
	}

	ccfg := C.hedge_config{
		num_writer_threads:          C.size_t(writerThreads),
		memtable_budget_bytes:       C.size_t(opts.MemtableBudgetBytes),
		num_partition_exponent:      C.size_t(opts.PartitionExponent),
		bucket_ratio:                C.double(opts.BucketRatio),
		compaction_read_ahead_bytes: C.size_t(opts.CompactionReadAheadBytes),
		max_pending_flushes:         C.size_t(opts.MaxPendingFlushes),
		min_merge_width:             C.size_t(opts.MinMergeWidth),
		max_merge_width:             C.size_t(opts.MaxMergeWidth),
		index_page_cache_bytes:      C.size_t(opts.IndexPageCacheBytes),
		num_background_workers:      C.size_t(opts.NumBackgroundWorkers),
		disable_direct_io:           boolToInt(opts.DisableDirectIO),
		disable_wal:                 boolToInt(opts.DisableWAL),
		disable_auto_compaction:     boolToInt(opts.DisableAutoCompaction),
		disable_l0_backpressure:     boolToInt(opts.DisableL0Backpressure),
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	cdb := C.hedge_open(cpath, boolToInt(create), &ccfg)
	if cdb == nil {
		return nil, errors.New("hedgedb: hedge_open failed")
	}

	db := &DB{cdb: cdb}
	for i := 0; i < opts.NumCtx; i++ {
		cpu := -1
		if i < len(opts.CPUAffinity) {
			cpu = opts.CPUAffinity[i]
		}
		c := &Ctx{
			db:    db,
			cpu:   cpu,
			reqCh: make(chan *request, opts.RequestQueueSize),
			quit:  make(chan struct{}),
			done:  make(chan struct{}),
		}
		ready := make(chan error, 1)
		go c.loop(opts.QueueDepth, ready)
		if err := <-ready; err != nil {
			db.shutdown() // tears down the ctxs started so far and closes cdb
			return nil, fmt.Errorf("hedgedb: ctx %d: %w", i, err)
		}
		db.ctxs = append(db.ctxs, c)
	}
	return db, nil
}

// Close stops all reactors and releases the store.
func (db *DB) Close() error {
	db.shutdown()
	return nil
}

// WaitForCompactions blocks until background compactions finish.
func (db *DB) WaitForCompactions() { C.hedge_wait_for_compactions(db.cdb) }

func (db *DB) shutdown() {
	for _, c := range db.ctxs {
		close(c.quit)
	}
	for _, c := range db.ctxs {
		<-c.done
	}
	C.hedge_close(db.cdb)
}

func (db *DB) pick() *Ctx {
	i := atomic.AddUint64(&db.next, 1)
	return db.ctxs[i%uint64(len(db.ctxs))]
}

// Put stores key->val. Synchronous: runs entirely on the calling goroutine (no reactor hop).
// The caller MUST run on one of a bounded, stable set of OS threads no larger than
// Options.NumWriterThreads — i.e. call it from a fixed pool of runtime.LockOSThread-pinned
// goroutines. See hedge_put in the C header.
func (db *DB) Put(key, val []byte) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	st := C.hedge_put(db.cdb, bytePtr(key), C.size_t(len(key)), bytePtr(val), C.size_t(len(val)))
	return statusErr(int(st))
}

// Remove deletes key. Same threading contract as Put.
func (db *DB) Remove(key []byte) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	st := C.hedge_remove(db.cdb, bytePtr(key), C.size_t(len(key)))
	return statusErr(int(st))
}

// Get returns the value for key, or ErrKeyNotFound / ErrDeleted. Reads use a reactor, so they
// are dispatched to the owning goroutine and resolved asynchronously.
func (db *DB) Get(key []byte) ([]byte, error) {
	req := reqPool.Get().(*request)
	req.key = key
	db.pick().reqCh <- req
	r := <-req.reply
	req.key = nil
	reqPool.Put(req)
	return r.value, r.err
}

// reqPool recycles requests (and their 1-buffered reply channels) so the hot read path
// allocates nothing per Get. Reuse is safe: the owner buffers exactly one reply and drops
// the request from `pending` before the caller receives it and returns the request here.
var reqPool = sync.Pool{
	New: func() any { return &request{reply: make(chan result, 1)} },
}

func (c *Ctx) loop(queueDepth uint32, ready chan<- error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(c.done)

	// if c.cpu >= 0 {
	// 	_ = pinToCPU(c.cpu) // best effort
	// }

	c.cctx = C.hedge_ctx_create(c.db.cdb, C.uint32_t(queueDepth))
	if c.cctx == nil {
		ready <- errors.New("hedge_ctx_create failed")
		return
	}
	ready <- nil
	defer C.hedge_ctx_destroy(c.cctx)

	pending := make(map[uint64]chan result)
	var oc C.hedge_outcome
	quitting := false

	for {
		// Submit everything currently queued (stop accepting new work once quitting).

		if len(pending) < 128 {
		drain:
			for !quitting {
				select {
				case req := <-c.reqCh:
					c.handle(req, pending)
				case <-c.quit:
					quitting = true
				default:
					break drain
				}
			}

		}

		if int(C.hedge_tick(c.cctx)) > 0 {
			// tick() returned progress, continue to pop outcomes
		}

		for C.hedge_outcome_pop(c.cctx, &oc) != 0 {
			seq := uint64(oc.seq)
			if ch, ok := pending[seq]; ok {
				ch <- toResult(&oc)
				delete(pending, seq)
			} else if oc.value != nil {
				C.hedge_value_free(oc.value)
			}
		}

		if quitting && len(pending) == 0 {
			return
		}

		// If no progress was made and nothing is pending, park until new work or quit.
		// If there is pending work, we yield to allow other goroutines to run while waiting on I/O.
		if len(pending) == 0 && !quitting && int(C.hedge_tick(c.cctx)) == 0 {
			select {
			case req := <-c.reqCh:
				c.handle(req, pending)
			case <-c.quit:
				quitting = true
			}
		}
	}
}

// handle submits one read on the ctx's owning thread; it is resolved later from the outcome
// queue by sequence number. Only reads flow through here — writes call hedge_put directly.
func (c *Ctx) handle(req *request, pending map[uint64]chan result) {
	seq := C.hedge_submit_get(c.cctx, bytePtr(req.key), C.size_t(len(req.key)))
	pending[uint64(seq)] = req.reply
}

func toResult(oc *C.hedge_outcome) result {
	var r result
	if oc.status != C.HEDGE_OK {
		r.err = statusError(int(oc.status))
	}
	if oc.value != nil {
		if n := C.hedge_value_len(oc.value); n > 0 {
			// r.value = C.GoBytes(unsafe.Pointer(C.hedge_value_data(oc.value)), C.int(n))
			r.value = []byte{}
		} else {
			r.value = []byte{}
		}
		C.hedge_value_free(oc.value)
	}
	return r
}

func bytePtr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

func boolToInt(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// pinToCPU sets the calling thread's affinity to a single CPU. The caller (loop)
// has already locked the goroutine to its OS thread for the reactor's lifetime.
func pinToCPU(cpu int) error {
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	return unix.SchedSetaffinity(0, &set) // 0 = the calling thread
}
