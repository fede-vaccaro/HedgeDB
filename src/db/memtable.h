#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <span>
#include <utility>
#include <vector>

#include "arena_allocator.h"
#include "async/rw_sync.h"
#include "cache.h"
#include "io/io_executor.h"
#include "logger.h"
#include "single_buffer_arena_allocator.h"
#include "skiplist.h"
#include "sst.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/channel.hpp"
#include "tmc/ex_cpu_st.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/task.hpp"
#include "types.h"
#include "wal.h"

namespace tmc
{
    class ex_cpu;
}

namespace hedge::db
{
    struct memtable_config
    {
        size_t memory_budget_cap = 32 * MiB;
        bool auto_compaction = true;
        bool use_odirect = true;
        size_t num_writer_threads = std::thread::hardware_concurrency();
        bool use_wal = true;
        /* bool use_fsync = false; // NOT IMPLEMENTED */
        bool fdatasync_flushed_sst = true;
        size_t max_pending_flushes = 4;
    };

    // Currently unused
    // The arena can be used for allocating skiplist's nodes (a node contains the sequence number, key and a span referencing the value)
    struct memory_arena
    {
        single_buffer_arena_allocator _arena;
        memory_arena(size_t budget) : _arena(budget) {}
    };

    // Wrapper around folly's skiplist
    class skiplist_wrapper : public skiplist_t
    {
        using Accessor = skiplist_t::Accessor;
        Accessor _accessor;
        std::atomic_uint64_t* _seq_nr;

    public:
        skiplist_wrapper(std::atomic_uint64_t* seq_nr, size_t /*budget*/)
            : skiplist_t(24, std::allocator<std::byte>{}),
              _accessor(this),
              _seq_nr(seq_nr)
        {
        }

        std::pair<bool, uint64_t> insert(const key_t& key, std::span<const std::byte> value);

        [[nodiscard]] std::optional<std::span<const std::byte>> get(const key_t& key) const;

        Accessor accessor() { return Accessor(this); }

        [[nodiscard]] uint64_t seq_nr() const
        {
            return this->_seq_nr->load(std::memory_order::relaxed);
        }
    };

    // Memtable object including WAL and per-thread value arenas
    struct write_buffer : skiplist_wrapper
    {
        alignas(CACHE_LINE_SIZE) std::atomic_size_t bytes_written{0};
        std::vector<std::unique_ptr<hedge::db::arena_allocator<std::byte>>> value_arenas; // One-per-thread, the values get stored here
        std::unique_ptr<wal> _wal;

        write_buffer(std::atomic_uint64_t* seq_nr,
                     size_t skiplist_memory_budget, // Unused
                     size_t n_threads,
                     size_t value_memory_budget,
                     std::unique_ptr<wal> wal_slot = nullptr)
            : skiplist_wrapper(seq_nr, skiplist_memory_budget), _wal(std::move(wal_slot))
        {
            value_arenas.reserve(n_threads);
            for(auto i = 0UL; i < n_threads; ++i)
                value_arenas.emplace_back(std::make_unique<hedge::db::arena_allocator<std::byte>>(value_memory_budget));
        }
    };

    // Memtable class represents the data ingress frontend for HedgeDB.
    //
    // It is responsible for writing onto the in-memory write buffer and the WAL and for managing the buffer flushes and
    // pushing newly generated SSTs to L0.
    // From the API perspective, it allows to write (::put_async), read (::get) and acquiring a snapshot of the memtable
    class memtable
    {
    public:
        // async::rw_sync is a fast synchronization mechanism between concurrent writers and a single reader (the flusher)
        // It prevents the reader (the flusher thread) starts from flushing the memtable if any writer did not finish writing.
        using rw_sync_buffer_t = async::rw_sync<write_buffer>;
        using rw_sync_buffer_ptr_t = std::shared_ptr<rw_sync_buffer_t>;

    private:
        memtable_config _cfg;

        // Shared stuff & params from ctor
        size_t _num_partition_exponent{};
        std::filesystem::path _indices_path{};

        // DB state & callbacks
        std::atomic_size_t* _flush_epoch{};
        std::function<tmc::task<void>(std::vector<sst>)> _push_new_ssts_callback;
        std::function<void()> _schedule_compaction_callback;
        tmc::atomic_condvar<bool>* _compaction_backpressure{};

        // Page cache
        std::shared_ptr<db::sharded_page_cache> _cache{};

        // File descriptor to the WAL files directory, needed for fsyncing when pool is created
        std::optional<int> _wal_dir_fd{};

        // Pool of reusable WAL slots
        tmc::chan_tok<std::unique_ptr<wal>> _wal_ch;

        // Global sequence number shared across all memtable instances
        alignas(CACHE_LINE_SIZE) std::atomic_uint64_t _seq_nr{0};

        // Current memtable and pipelined
        alignas(CACHE_LINE_SIZE) tmc::atomic_condvar<rw_sync_buffer_ptr_t> _table{nullptr};
        alignas(CACHE_LINE_SIZE) tmc::atomic_condvar<rw_sync_buffer_ptr_t> _pipelined_table{nullptr}; // For double buffering
        alignas(CACHE_LINE_SIZE) std::atomic_bool _flush_mutex;                                       // One thread at a time takes the responsability of flushing when

        // Pending flushes
        alignas(CACHE_LINE_SIZE) std::atomic_size_t _table_switch_epoch;
        alignas(CACHE_LINE_SIZE) mutable std::shared_mutex _pending_flushes_mutex;
        tmc::semaphore _pending_flush_slots;
        std::condition_variable_any _pending_flushes_cv_sync; // Only used when waiting for every flush to complete
        std::map<size_t, rw_sync_buffer_ptr_t> _pending_flushes;

        // Executors
        std::shared_ptr<tmc::semaphore> _can_write = std::make_shared<tmc::semaphore>(1);
        std::unique_ptr<tmc::ex_cpu_st> _flusher;
        std::shared_ptr<io::io_executor> _flush_executor;

        // Logger
        logger _logger;

    public:
        inline static std::atomic_size_t HALT_COUNTER{0}; // For debugging/statistics

        memtable(const memtable_config& cfg,
                 size_t num_partition_exponent,
                 std::filesystem::path indices_path,
                 std::atomic_size_t* flush_epoch_ptr,
                 std::shared_ptr<io::io_executor> flusher_executor,
                 std::function<tmc::task<void>(std::vector<sst>)> push_new_ssts_callback,
                 std::function<void()> schedule_compaction_callback,
                 std::shared_ptr<db::sharded_page_cache> page_cache,
                 tmc::atomic_condvar<bool>* compaction_backpressure = nullptr);

        ~memtable();

        // This method needs to be asynchronous (::task<...>) because
        // in case of pressure (arriving from the pending compactions or pending flushes)
        // it cooperatively yields (to other task or threads)
        tmc::task<hedge::status> put_async(const key_t& key, std::span<const std::byte> value, hedge::value_type value_type);

        std::optional<value_t> get(const key_t& key) const;

        std::future<void> wait_for_flush();

        struct snapshot
        {
            uint64_t seq_nr;
            rw_sync_buffer_ptr_t curr;
            std::map<size_t, rw_sync_buffer_ptr_t> pending_flushes;
        };

        // MVCC snapshot for consistent range scans
        snapshot acquire_snapshot();

        hedge::status replay_wal(std::optional<uint64_t> skip_up_to_seq_nr = std::nullopt);

    private:
        static constexpr size_t VALUE_DATA_ALIGNMENT = 16; // Deprecated, might use actual alignment (8 bytes)

        [[nodiscard]] tmc::task<std::shared_ptr<rw_sync_buffer_t>> _make_memtable();
        tmc::task<bool> _flush(rw_sync_buffer_ptr_t expected_table);
        tmc::task<void> _flush_inner(size_t curr_flush_epoch,
                                     rw_sync_buffer_ptr_t memtable_to_flush,
                                     std::shared_ptr<tmc::semaphore> can_write,
                                     std::shared_ptr<tmc::semaphore> next_can_write);
    };

} // namespace hedge::db
