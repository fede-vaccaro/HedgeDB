package hedgedb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func newTestDB(t *testing.T, opts Options) (*DB, func()) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store") // must not exist for create=true
	db, err := Open(dir, true, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, func() { db.Close() }
}

func TestPutGetRemove(t *testing.T) {
	const writers = 4
	db, cleanup := newTestDB(t, Options{NumCtx: 4, NumWriterThreads: writers})
	defer cleanup()

	const n = 2000

	// Writes must come from a bounded set of OS-thread-pinned goroutines (<= NumWriterThreads).
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			runtime.LockOSThread()
			for i := w; i < n; i += writers {
				k := []byte(fmt.Sprintf("key-%06d", i))
				v := []byte(fmt.Sprintf("value-%d", i))
				if err := db.Put(k, v); err != nil {
					t.Errorf("put %d: %v", i, err)
				}
			}
		}(w)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i))
		want := []byte(fmt.Sprintf("value-%d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %d: got %q want %q", i, got, want)
		}
	}

	if err := db.Remove([]byte("key-000000")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := db.Get([]byte("key-000000")); err == nil {
		t.Fatalf("expected error after remove, got nil")
	}

	if _, err := db.Get([]byte("does-not-exist")); err == nil {
		t.Fatalf("expected error for missing key, got nil")
	}
}
