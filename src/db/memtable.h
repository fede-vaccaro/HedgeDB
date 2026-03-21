#pragma once

#include <atomic>
#include <cassert>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "async/io_executor.h"
#include "btree.h"
#include "cache.h"
#include "logger.h"
#include "rw_sync.h"
#include "single_buffer_arena_allocator.h"
#include "skiplist.h"
#include "sst.h"
#include "types.h"
#include "worker.h"

namespace hedge::db
{

    struct memtable_config
    {
        size_t max_inserts_cap = 2'000'000;
        size_t memory_budget_cap = 32 * 1024 * 1024;
        bool auto_compaction = true;
        bool use_odirect = true;
        size_t num_writer_threads = 256; // (quite) safe upper bound
        size_t flush_io_workers = 4;
        bool use_wal = true;
        /* bool use_fsync = false; // NOT IMPLEMENTED */
        bool fdatasync_flushed_sst = true;
    };

    struct memtable_arena_holder
    {
        single_buffer_arena_allocator arena_;
        memtable_arena_holder(size_t budget) : arena_(budget) {}
    };

    class memtable_impl3_t : private memtable_arena_holder, public skiplist_t
    {
        using Accessor = skiplist_t::Accessor;
        Accessor _accessor;
        alignas(64) uint32_t seq_nr;

    public:
        memtable_impl3_t(size_t budget)
            : memtable_arena_holder(budget),
              skiplist_t(24, StdArenaAllocator<uint8_t>(this->arena_)),
              _accessor(this)
        {
        }

        std::pair<bool, uint32_t> insert(const key_t& key, std::span<const uint8_t> value);

        std::optional<std::span<const uint8_t>> get(const key_t& key) const;

        Accessor accessor() { return Accessor(this); }
    };

    // Memtable for keys and pointers to values (any sub-type)
    // The arena is dedicated for storing values in mem
    struct memtable_with_arena_t : memtable_impl3_t
    {
        alignas(64) std::atomic_size_t bytes_written{0};
        std::vector<std::unique_ptr<hedge::db::arena_allocator<uint8_t, false>>> value_arenas; // one per writer thread, not shared between threads
        // single_buffer_arena_allocator value_arena; // single arena for values, shared between threads with atomic allocation
        std::vector<hedge::fs::file> per_thread_wals;

        memtable_with_arena_t(const std::filesystem::path& base_path,
                              bool use_wal,
                              size_t flush_iteration,
                              size_t memtable_memory_budget,
                              size_t n_threads,
                              size_t value_memory_budget)
            : memtable_impl3_t(memtable_memory_budget) /*, value_arena(value_memory_budget)*/
        {
            value_arenas.reserve(n_threads);
            for(auto i = 0UL; i < n_threads; ++i)
            {
                value_arenas.emplace_back(std::make_unique<hedge::db::arena_allocator<uint8_t, false>>(value_memory_budget));
            }

            if(!use_wal)
                return;

            per_thread_wals.reserve(n_threads);
            for(auto i = 0U; i < n_threads; ++i)
            {
                const auto initial_wal_size = static_cast<size_t>((static_cast<double>(value_memory_budget) / n_threads));
                const auto wal_path = std::format("wal.t{}.{}", i, flush_iteration);
                auto maybe_wal = fs::file::from_path(base_path / wal_path, fs::file::open_mode::write_append_new, false, std::nullopt);

                if(!maybe_wal.has_value())
                    throw std::runtime_error("could not open wal " + (base_path / wal_path).string() + " : " + maybe_wal.error().to_string());

                fallocate(maybe_wal.value().fd(), FALLOC_FL_KEEP_SIZE, 0, initial_wal_size); // ignore res

                per_thread_wals.emplace_back(std::move(maybe_wal.value()));
            }
        }
    };

    using memtable_impl_t = btree<uuid_t, value_ptr_t, std::less<>, false>; // READ_ONLY=false

    using frozen_memtable_impl_t = btree<key_t, value_ptr_t, std::less<>, false>; // READ_ONLY=true -> When using this version, if we know that this type will be read only, every lock will be skipped for performance

    class memtable
    {
        memtable_config _cfg;

        // Shared stuff & params from ctor
        size_t _num_partition_exponent{};
        std::filesystem::path _indices_path{};

        // DB state & callbacks
        std::atomic_size_t* _flush_epoch{};
        std::function<void(std::vector<sst>, std::span<std::shared_ptr<async::executor_context>>)> _push_new_indices;
        std::function<void()> _trigger_compaction_callback;
        std::atomic_bool* _compaction_backpressure{};

        // Page cache
        std::shared_ptr<db::sharded_page_cache> _cache{};

        // Current memtable and pipelined
        using rw_sync_table_t = async::rw_sync<memtable_with_arena_t>;
        using rw_sync_table_ptr_t = std::shared_ptr<rw_sync_table_t>;

        alignas(64) std::atomic<rw_sync_table_ptr_t> _table;
        alignas(64) std::atomic_bool _flush_mutex;
        alignas(64) std::atomic_size_t _wal_epoch{0};
        alignas(64) std::atomic<rw_sync_table_ptr_t> _pipelined_table;

        // Pending flushes
        static constexpr size_t MAX_PENDING_FLUSHES = 8;
        alignas(64) std::atomic_size_t _table_switch_epoch;
        alignas(64) mutable std::shared_mutex _pending_flushes_mutex;
        alignas(64) std::condition_variable_any _pending_flushes_cv;
        std::map<size_t, rw_sync_table_ptr_t> _pending_flushes;

        async::worker _flusher;
        std::thread _table_maker;
        std::atomic_bool _running{true};
        std::vector<std::shared_ptr<async::executor_context>> _flush_executor_pool;
        std::vector<std::unique_ptr<async::worker>> _flush_worker_pool;
        logger _logger;

    public:
        inline static std::atomic_size_t BACKPRESSURE{0};

        memtable() = default;

        memtable(const memtable_config& cfg,
                 size_t num_partition_exponent,
                 std::filesystem::path indices_path,
                 std::atomic_size_t* flush_epoch_ptr,
                 std::function<void(std::vector<sst>, std::span<std::shared_ptr<async::executor_context>>)> push_new_indices,
                 std::function<void()> compaction_callback,
                 std::shared_ptr<db::sharded_page_cache> page_cache,
                 std::atomic_bool* compaction_backpressure = nullptr);

        ~memtable();

        async::task<hedge::status> put_async(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type);

        hedge::status put(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type);

        std::optional<value_t> get(const key_t& key) const;

        std::future<void> wait_for_flush();

        hedge::status replay_wal();

        [[nodiscard]] auto make_memtable() const
        {
            return std::make_shared<rw_sync_table_t>(
                this->_cfg.num_writer_threads,
                this->_indices_path,
                this->_cfg.use_wal,
                const_cast<std::atomic_size_t*>(&this->_wal_epoch)->fetch_add(1, std::memory_order::relaxed),
                this->_cfg.memory_budget_cap * 2,
                this->_cfg.num_writer_threads,
                this->_cfg.memory_budget_cap);
        }

    private:
        hedge::status _append_to_wal(int32_t fd, uint32_t seq_nr, const key_t& key, std::span<const uint8_t> value);

        bool _flush(rw_sync_table_ptr_t expected_table);
    };

} // namespace hedge::db
