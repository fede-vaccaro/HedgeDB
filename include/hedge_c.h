#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct hedge_db hedge_db;
    typedef struct hedge_ctx hedge_ctx;
    typedef struct hedge_value hedge_value;

    // Status codes carried in hedge_outcome.status. 0 means success; the error codes mirror
    // hedge::errc shifted by +1 so that GENERIC_ERROR does not collide with HEDGE_OK.
    typedef enum
    {
        HEDGE_OK = 0,
        HEDGE_ERR_GENERIC = 1,
        HEDGE_ERR_VALUE_TABLE_NO_SPACE = 2,
        HEDGE_ERR_KEY_NOT_FOUND = 3,
        HEDGE_ERR_DELETED = 4,
        HEDGE_ERR_BUSY = 5,
        HEDGE_ERR_BUFFER_FULL = 6,
        HEDGE_ERR_END_OF_SCAN = 7,
        HEDGE_ERR_SKIP = 8,
    } hedge_status_code;

    typedef struct
    {
        uint64_t seq;        // sequence number returned by the matching hedge_submit_* call
        int32_t status;      // hedge_status_code; 0 = ok
        hedge_value* value;  // get only; NULL for put/remove and on error; free with hedge_value_free
    } hedge_outcome;

    // Database configuration. A 0 / false field means "use the engine default", except where noted.
    typedef struct
    {
        size_t num_writer_threads;          // 0 = default (static pool thread count)
        size_t memtable_budget_bytes;       // 0 = default
        size_t num_partition_exponent;      // 0 = default
        double bucket_ratio;                // 0 = default
        size_t compaction_read_ahead_bytes; // 0 = default
        size_t max_pending_flushes;         // 0 = default
        size_t min_merge_width;             // 0 = default
        size_t max_merge_width;             // 0 = default
        size_t index_page_cache_bytes;      // 0 = default (disabled)
        size_t num_background_workers;      // 0 = default (auto)
        int disable_direct_io;              // 0 => direct io ON (default)
        int disable_wal;                    // 0 => WAL ON (default)
        int disable_auto_compaction;        // 0 => auto compaction ON (default)
        int disable_l0_backpressure;        // != 0 => no L0-count write backpressure
    } hedge_config;

    // --- Database lifecycle (shared across threads) ---
    // create != 0 -> create a new db (path must not exist); create == 0 -> load existing.
    // cfg may be NULL to use all engine defaults. Returns NULL on failure.
    hedge_db* hedge_open(const char* path, int create, const hedge_config* cfg);
    void hedge_close(hedge_db* db);

    // Block until background compactions finish. Call from any thread.
    void hedge_wait_for_compactions(hedge_db* db);

    // --- Per-thread reactor ---
    // hedge_ctx_create and ALL subsequent calls for that ctx (submit/tick/pop/destroy) MUST run
    // on the same OS thread. Create every ctx once at startup and keep it for the process lifetime.
    hedge_ctx* hedge_ctx_create(hedge_db* db, uint32_t queue_depth);
    void hedge_ctx_destroy(hedge_ctx* ctx);

    // Synchronous writes (the engine write path is blocking and does not use a reactor ring).
    // Return a hedge_status_code (0 = ok). The caller's OS thread must belong to a bounded,
    // stable set no larger than num_writer_threads (each distinct thread claims a writer slot:
    // rw_sync stripe + value arena + WAL file). In Go: call from a fixed pool of
    // runtime.LockOSThread-pinned goroutines.
    int hedge_put(hedge_db* db, const uint8_t* key, size_t klen,
                  const uint8_t* val, size_t vlen);
    int hedge_remove(hedge_db* db, const uint8_t* key, size_t klen);

    // Submit an async read. Returns a sequence number; the result lands in the outcome queue.
    uint64_t hedge_submit_get(hedge_ctx* ctx, const uint8_t* key, size_t klen);

    // Drive one iteration. Returns the number of completions reaped (continuations made runnable);
    // keep ticking while > 0, back off when it returns 0.
    size_t hedge_tick(hedge_ctx* ctx);

    // Pop one outcome. Returns 1 and fills *out if one was available, 0 if the queue is empty.
    int hedge_outcome_pop(hedge_ctx* ctx, hedge_outcome* out);

    // --- Value handle (owns the bytes the engine returned; zero-copy view) ---
    const uint8_t* hedge_value_data(const hedge_value* v);
    size_t hedge_value_len(const hedge_value* v);
    void hedge_value_free(hedge_value* v);  // safe to call from any thread

#ifdef __cplusplus
}
#endif
