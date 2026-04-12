#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "arena_allocator.h"
#include "cache.h"
#include "io/io_executor.h"
#include "logger.h"
#include "rw_sync.h"
#include "single_buffer_arena_allocator.h"
#include "skiplist.h"
#include "sst.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/ex_braid.hpp"
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
        size_t max_inserts_cap = 2'000'000;
        size_t memory_budget_cap = 32 * 1024 * 1024;
        bool auto_compaction = true;
        bool use_odirect = true;
        size_t num_writer_threads = 256; // (quite) safe upper bound
        // size_t flush_io_workers = 4;
        bool use_wal = true;
        /* bool use_fsync = false; // NOT IMPLEMENTED */
        bool fdatasync_flushed_sst = true;
        size_t starting_wal_epoch = 0;
        size_t max_pending_flushes = 4;
    };

    struct memtable_arena_holder
    {
        single_buffer_arena_allocator _arena;
        memtable_arena_holder(size_t budget) : _arena(budget) {}
    };

    class memtable_impl3_t : public skiplist_t
    {
        using Accessor = skiplist_t::Accessor;
        Accessor _accessor;
        std::atomic_uint64_t* _seq_nr;

    public:
        memtable_impl3_t(std::atomic_uint64_t* seq_nr, size_t /*budget*/)
            : skiplist_t(24, std::allocator<uint8_t>{}),
              _accessor(this),
              _seq_nr(seq_nr)
        {
        }

        std::pair<bool, uint64_t> insert(const key_t& key, std::span<const uint8_t> value);

        std::optional<std::span<const uint8_t>> get(const key_t& key) const;

        Accessor accessor() { return Accessor(this); }

        [[nodiscard]] uint64_t seq_nr() const
        {
            return this->_seq_nr->load(std::memory_order::relaxed);
        }
    };

    // Memtable for keys and pointers to values (any sub-type)
    // The arena is dedicated for storing values in mem
    struct memtable_with_arena_t : memtable_impl3_t
    {
        alignas(64) std::atomic_size_t bytes_written{0};
        std::vector<std::unique_ptr<hedge::db::arena_allocator<uint8_t>>> value_arenas; // one per writer thread, not shared between threads
        std::optional<wal> _wal;

        memtable_with_arena_t(std::atomic_uint64_t* seq_nr,
                              const std::filesystem::path& base_path,
                              bool use_wal,
                              size_t flush_iteration,
                              size_t memtable_memory_budget,
                              size_t n_threads,
                              size_t value_memory_budget)
            : memtable_impl3_t(seq_nr, memtable_memory_budget)
        {
            value_arenas.reserve(n_threads);
            for(auto i = 0UL; i < n_threads; ++i)
                value_arenas.emplace_back(std::make_unique<hedge::db::arena_allocator<uint8_t>>(value_memory_budget));

            if(use_wal)
            {
                this->_wal.emplace(wal::config{
                    .base_path = base_path,
                    .epoch = flush_iteration,
                    .n_threads = n_threads,
                    .file_size_hint = value_memory_budget / n_threads});
            }
        }
    };

    class memtable
    {
    public:
        using rw_sync_table_t = async::rw_sync<memtable_with_arena_t>;
        using rw_sync_table_ptr_t = std::shared_ptr<rw_sync_table_t>;

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

        // Global sequence number shared across all memtable instances
        alignas(64) std::atomic_uint64_t _seq_nr{0};

        // Current memtable and pipelined
        alignas(64) tmc::atomic_condvar<rw_sync_table_ptr_t> _table{nullptr};
        alignas(64) std::atomic_bool _flush_mutex;
        alignas(64) std::atomic_size_t _wal_epoch;
        alignas(64) std::atomic<rw_sync_table_ptr_t> _pipelined_table;

        // Pending flushes
        alignas(64) std::atomic_size_t _table_switch_epoch;
        alignas(64) mutable std::shared_mutex _pending_flushes_mutex;
        tmc::semaphore _pending_flush_slots;
        std::condition_variable_any _pending_flushes_cv_sync;
        std::map<size_t, rw_sync_table_ptr_t> _pending_flushes;
        std::unique_ptr<tmc::ex_cpu_st> _flusher;
        std::optional<tmc::ex_braid> _braid;
        std::atomic_bool _running{true};
        std::shared_ptr<io::io_executor> _flush_executor;
        logger _logger;

    public:
        inline static std::atomic_size_t BACKPRESSURE{0};

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

        tmc::task<hedge::status> put_async(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type);

        std::optional<value_t> get(const key_t& key) const;

        std::future<void> wait_for_flush();

        struct snapshot
        {
            uint64_t seq_nr;
            rw_sync_table_ptr_t curr;
            std::map<size_t, rw_sync_table_ptr_t> pending_flushes;
        };

        snapshot acquire_snapshot();

        hedge::status replay_wal();

        [[nodiscard]] size_t wal_epoch() const
        {
            return this->_wal_epoch.load(std::memory_order::relaxed);
        }

    private:
        [[nodiscard]] std::shared_ptr<rw_sync_table_t> _make_memtable();

        tmc::task<bool> _flush(rw_sync_table_ptr_t expected_table);
        tmc::task<void> _flush_inner(size_t curr_flush_epoch, rw_sync_table_ptr_t memtable_to_flush);
    };

} // namespace hedge::db
