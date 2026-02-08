#pragma once

#include <atomic>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "async/spinlock.h"
#include "btree.h"
#include "cache.h"
#include "logger.h"
#include "rw_sync.h"
#include "sorted_index.h"
#include "types.h"
#include "worker.h"

namespace hedge::db
{

    struct memtable_config
    {
        size_t max_inserts_cap = 2'000'000;
        size_t memory_budget_cap = 64 * 1024 * 1024;
        bool auto_compaction = true;
        bool use_odirect = false;
        size_t num_writer_threads = 256; // (quite) safe upper bound 
    };

    using memtable_impl_t = btree<key_t, value_ptr_t, std::less<>, false>; // READ_ONLY=false

    using frozen_memtable_impl_t = btree<key_t, value_ptr_t, std::less<>, false>; // READ_ONLY=true -> When using this version, if we know that this type will be read only, every lock will be skipped for performance

    class memtable
    {
        memtable_config _cfg;

        // Shared stuff & params from ctor
        size_t _num_partition_exponent{};
        std::filesystem::path _indices_path{};

        // DB state & callbacks
        std::atomic_size_t* _flush_epoch{};
        std::function<void(std::vector<sorted_index>)> _push_new_indices;
        std::function<void()> _trigger_compaction_callback;

        // Page cache
        std::shared_ptr<db::shared_page_cache> _cache{};

        // Current memtable and pipelined
        using rw_sync_table_t = async::rw_sync<memtable_impl_t>;
        using rw_sync_table_ptr_t = std::shared_ptr<rw_sync_table_t>;

        alignas(64) std::atomic<rw_sync_table_ptr_t> _table;
        alignas(64) std::atomic<rw_sync_table_ptr_t> _pipelined_table;

        // Pending flushes
        alignas(64) std::shared_mutex _pending_flushes_mutex;
        std::map<size_t, rw_sync_table_ptr_t> _pending_flushes;

        async::worker _flusher;
        logger _logger;

    public:
        memtable() = default;

        memtable(const memtable_config& cfg,
                 size_t num_partition_exponent,
                 std::filesystem::path indices_path,
                 std::atomic_size_t* flush_epoch_ptr,
                 std::function<void(std::vector<sorted_index>)> push_new_indices,
                 std::function<void()> compaction_callback,
                 std::shared_ptr<db::shared_page_cache> page_cache);

        void put(const key_t& key, const value_ptr_t& value);

        std::optional<value_ptr_t> get(const key_t& key);

        std::future<void> wait_for_flush();

    private:
        bool _flush();
    };

} // namespace hedge::db
