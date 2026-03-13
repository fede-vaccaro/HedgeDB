#pragma once

#include <atomic>
#include <cassert>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <vector>

struct CheckStream
{
    // bool condition;
    explicit CheckStream(bool /*cond*/) /*: condition(cond) */ {}
    template <typename T>
    CheckStream& operator<<(const T& /*msg*/)
    {
        // if(!condition)
        //     std::cerr << msg;
        return *this;
    }
    ~CheckStream()
    {
        // if(!condition)
        // {
        //     std::cerr << std::endl;
        //     std::abort();
        // }
    }
};

#ifndef CHECK_EQ
#define CHECK_EQ(a, b) CheckStream((a) == (b))
#endif
#ifndef DCHECK
#define DCHECK(x) CheckStream(!!(x))
#endif
#ifndef DCHECK_GT
#define DCHECK_GT(a, b) CheckStream((a) > (b))
#endif
#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) CheckStream((a) == (b))
#endif
#ifndef CHECK_LE
#define CHECK_LE(a, b) CheckStream((a) <= (b))
#endif
#ifndef FOLLY_UNLIKELY
#define FOLLY_UNLIKELY(x) (x)
#endif
#ifndef FOLLY_LIKELY
#define FOLLY_LIKELY(x) (x)
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include "db/folly/concurrent_skip_list/concurrent_skiplist.h"
#pragma GCC diagnostic pop

#include "async/io_executor.h"
#include "btree.h"
#include "cache.h"
#include "db/arena_allocator.h"
#include "logger.h"
#include "rw_sync.h"
#include "single_buffer_arena_allocator.h"
#include "sst.h"
#include "types.h"
#include "worker.h"

namespace hedge::db
{
    // Adapter for single_buffer_arena_allocator to std::allocator interface
    template <typename T>
    class StdArenaAllocator
    {
    public:
        using value_type = T;

        StdArenaAllocator(single_buffer_arena_allocator& arena) : arena_(&arena) {}

        template <typename U>
        StdArenaAllocator(const StdArenaAllocator<U>& other) : arena_(other.arena_) {}

        T* allocate(size_t n)
        {
            auto span = arena_->allocate(n * sizeof(T));
            if(span.empty())
            {
                // std::cout << "Arena out of memory when trying to allocate " << n * sizeof(T) << " bytes" << std::endl;
                throw std::bad_alloc();
            }
            return reinterpret_cast<T*>(span.data());
        }

        void deallocate(T*, size_t) noexcept
        {
            // No-op for arena
        }

        bool operator==(const StdArenaAllocator& other) const { return arena_ == other.arena_; }
        bool operator!=(const StdArenaAllocator& other) const { return arena_ != other.arena_; }

        template <typename U>
        friend class StdArenaAllocator;

    private:
        single_buffer_arena_allocator* arena_;
    };

    struct memtable_config
    {
        size_t max_inserts_cap = 2'000'000;
        size_t memory_budget_cap = 32 * 1024 * 1024;
        bool auto_compaction = true;
        bool use_odirect = true;
        size_t num_writer_threads = 256; // (quite) safe upper bound
        size_t flush_io_workers = 4;
    };

    struct memtable_entry
    {
        key_t key;
        mutable std::span<const uint8_t> value;

        memtable_entry() = default;
        memtable_entry(key_t k, std::span<const uint8_t> v) : key(k), value(v) {}
    };

    struct memtable_cmp
    {
        bool operator()(const memtable_entry& a, const memtable_entry& b) const
        {
            return a.key < b.key;
        }
        bool operator()(const key_t& a, const memtable_entry& b) const
        {
            return a < b.key;
        }
        bool operator()(const memtable_entry& a, const key_t& b) const
        {
            return a.key < b;
        }
    };

    using skiplist_t = folly::ConcurrentSkipList<memtable_entry, memtable_cmp, StdArenaAllocator<uint8_t>>;

    struct memtable_arena_holder
    {
        single_buffer_arena_allocator arena_;
        memtable_arena_holder(size_t budget) : arena_(budget) {}
    };

    class memtable_impl3_t : private memtable_arena_holder, public skiplist_t
    {
        using Accessor = skiplist_t::Accessor;
        Accessor _accessor;

    public:
        memtable_impl3_t(size_t budget)
            : memtable_arena_holder(budget),
              skiplist_t(24, StdArenaAllocator<uint8_t>(this->arena_)),
              _accessor(this)
        {
        }

        bool insert(const key_t& key, std::span<const uint8_t> value)
        {
            try
            {
                auto res = this->_accessor.insert(memtable_entry(key, value));
                if(!res.second)
                {
                    // Key exists, update value
                    // value in memtable_entry is mutable
                    res.first->value = value;
                }
                return true;
            }
            catch(const std::bad_alloc&)
            {
                return false;
            }
        }

        std::optional<std::span<const uint8_t>> get(const key_t& key) const
        {
            Accessor acc(const_cast<memtable_impl3_t*>(this));
            auto it = acc.find(memtable_entry(key, {}));
            if(it != acc.end())
            {
                return it->value;
            }
            return std::nullopt;
        }

        Accessor accessor() { return Accessor(this); }
    };

    // Memtable for keys and pointers to values (any sub-type)
    // The arena is dedicated for storing values in mem
    struct memtable_with_arena_t : memtable_impl3_t
    {
        // std::vector<std::unique_ptr<hedge::db::arena_allocator<uint8_t, false>>> value_arenas; // one per writer thread, not shared between threads
        single_buffer_arena_allocator value_arena;                                             // single arena for values, shared between threads with atomic allocation

        memtable_with_arena_t(size_t memtable_memory_budget, size_t n_threads, size_t value_memory_budget)
            : memtable_impl3_t(memtable_memory_budget), value_arena(value_memory_budget)
        {
        }
    };

    // using memtable_impl2_t = btree<key_t, value_ptr_t, std::less<>>; // READ_ONLY=false

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
        std::function<void(std::vector<sst>)> _push_new_indices;
        std::function<void()> _trigger_compaction_callback;
        std::atomic_bool* _compaction_backpressure{};

        // Page cache
        std::shared_ptr<db::sharded_page_cache> _cache{};

        // Current memtable and pipelined
        using rw_sync_table_t = async::rw_sync<memtable_with_arena_t>;
        using rw_sync_table_ptr_t = std::shared_ptr<rw_sync_table_t>;

        alignas(64) std::atomic<rw_sync_table_ptr_t> _table;
        alignas(64) std::atomic_bool _flush_mutex;
        alignas(64) std::atomic<rw_sync_table_ptr_t> _pipelined_table;

        // Pending flushes
        static constexpr size_t MAX_PENDING_FLUSHES = -1;
        alignas(64) mutable std::shared_mutex _pending_flushes_mutex;
        alignas(64) std::condition_variable_any _pending_flushes_cv;
        std::map<size_t, rw_sync_table_ptr_t> _pending_flushes;

        async::worker _flusher;
        std::thread _table_maker;
        std::atomic_bool _running{true};
        std::vector<std::shared_ptr<async::executor_context>> _flush_executor_pool;
        logger _logger;

    public:
        inline static std::atomic_size_t BACKPRESSURE{0};

        memtable() = default;

        memtable(const memtable_config& cfg,
                 size_t num_partition_exponent,
                 std::filesystem::path indices_path,
                 std::atomic_size_t* flush_epoch_ptr,
                 std::function<void(std::vector<sst>)> push_new_indices,
                 std::function<void()> compaction_callback,
                 std::shared_ptr<db::sharded_page_cache> page_cache,
                 std::atomic_bool* compaction_backpressure = nullptr);

        ~memtable();

        void put(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type);

        std::optional<value_t> get(const key_t& key) const;

        std::future<void> wait_for_flush();

        [[nodiscard]] auto make_memtable() const
        {
            return std::make_shared<rw_sync_table_t>(this->_cfg.num_writer_threads, this->_cfg.memory_budget_cap * 2, this->_cfg.num_writer_threads, this->_cfg.memory_budget_cap);
        }

    private:
        bool _flush();
    };

} // namespace hedge::db
