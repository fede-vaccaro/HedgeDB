// Benchmark: executor overhead on a concurrent skiplist insert.
//
// Mirrors the database write path (500K keys, 100-byte values, no flush)
// but removes all database machinery (rw_sync, value arenas, wait_group,
// value_table writes). This isolates the cost of:
//   - Coroutine frame allocation / deallocation
//   - MPSC queue push (main) + pop (executor)
//   - Executor _in_progress_tasks map operations
//   - The actual ConcurrentSkipList::insert() call

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <span>

// Macros required by Folly's ConcurrentSkipList internals
struct _NullStream
{
    template <typename T>
    _NullStream& operator<<(const T&) { return *this; }
};
#ifndef CHECK_EQ
#define CHECK_EQ(a, b) _NullStream()
#endif
#ifndef DCHECK
#define DCHECK(x) _NullStream()
#endif
#ifndef DCHECK_GT
#define DCHECK_GT(a, b) _NullStream()
#endif
#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) _NullStream()
#endif
#ifndef CHECK_LE
#define CHECK_LE(a, b) _NullStream()
#endif
#ifndef FOLLY_UNLIKELY
#define FOLLY_UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif
#ifndef FOLLY_LIKELY
#define FOLLY_LIKELY(x) __builtin_expect(!!(x), 1)
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include "db/folly/concurrent_skip_list/concurrent_skiplist.h"
#pragma GCC diagnostic pop

#include "async/io_executor.h"
#include "async/task.h"
#include "async/wait_group.h"
#include "single_buffer_arena_allocator.h"
#include "types.h"

using hedge::single_buffer_arena_allocator;

// ---------------------------------------------------------------------------
// Skiplist setup — mirrors memtable_impl3_t from src/db/memtable.h
// ---------------------------------------------------------------------------

template <typename T>
class StdArenaAllocator
{
public:
    using value_type = T;

    explicit StdArenaAllocator(single_buffer_arena_allocator& arena) : arena_(&arena) {}

    template <typename U>
    StdArenaAllocator(const StdArenaAllocator<U>& other) : arena_(other.arena_) {}

    T* allocate(size_t n)
    {
        auto span = arena_->allocate(n * sizeof(T));
        if(span.empty())
            throw std::bad_alloc();
        return reinterpret_cast<T*>(span.data());
    }

    void deallocate(T*, size_t) noexcept {}

    bool operator==(const StdArenaAllocator& o) const { return arena_ == o.arena_; }
    bool operator!=(const StdArenaAllocator& o) const { return arena_ != o.arena_; }

    template <typename U>
    friend class StdArenaAllocator;

private:
    single_buffer_arena_allocator* arena_;
};

struct memtable_entry
{
    hedge::key_t key;
    mutable std::span<const uint8_t> value;

    memtable_entry() = default;
    memtable_entry(hedge::key_t k, std::span<const uint8_t> v) : key(k), value(v) {}
};

struct memtable_cmp
{
    bool operator()(const memtable_entry& a, const memtable_entry& b) const { return a.key < b.key; }
    bool operator()(const hedge::key_t& a, const memtable_entry& b) const { return a < b.key; }
    bool operator()(const memtable_entry& a, const hedge::key_t& b) const { return a.key < b; }
};

using skiplist_t = folly::ConcurrentSkipList<memtable_entry, memtable_cmp, StdArenaAllocator<uint8_t>>;

struct memtable_arena_holder
{
    single_buffer_arena_allocator arena_;
    explicit memtable_arena_holder(size_t budget) : arena_(budget) {}
};

// Owns the arena + skiplist, providing a shared Accessor (same as memtable_impl3_t).
struct bench_skiplist : private memtable_arena_holder, public skiplist_t
{
    using Accessor = skiplist_t::Accessor;
    Accessor _accessor;

    explicit bench_skiplist(size_t budget)
        : memtable_arena_holder(budget),
          skiplist_t(24, StdArenaAllocator<uint8_t>(this->arena_)),
          _accessor(this)
    {
    }

    bool insert(const hedge::key_t& key, std::span<const uint8_t> value)
    {
        try
        {
            auto res = this->_accessor.insert(memtable_entry(key, value));
            if(!res.second)
                res.first->value = value; // update existing
            return true;
        }
        catch(const std::bad_alloc&)
        {
            return false;
        }
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static hedge::key_t make_key(uint64_t i)
{
    std::array<uint8_t, 16> bytes{};
    for(int j = 0; j < 8; ++j)
        bytes[15 - j] = static_cast<uint8_t>((i >> (j * 8)) & 0xFF);
    return hedge::key_t(bytes);
}

// ---------------------------------------------------------------------------
// Coroutine task — one per insert, mirrors make_put_task in database.spec.cc
// ---------------------------------------------------------------------------

static const uint8_t DUMMY_VALUE[100]{};

hedge::async::task<void> make_insert_task(bench_skiplist* sl, size_t i, hedge::async::wait_group* wg)
{
    sl->insert(make_key(i), std::span<const uint8_t>(DUMMY_VALUE, sizeof(DUMMY_VALUE)));
    wg->decr();
    co_return;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
    constexpr size_t N = 500'000;
    constexpr size_t N_EXECUTORS = 12;
    constexpr uint32_t QUEUE_DEPTH = 64;
    constexpr size_t ARENA_BYTES = 256UL * 1024 * 1024; // 256 MB, same ballpark as memtable budget

    hedge::async::executor_pool::init_static_pool(N_EXECUTORS, QUEUE_DEPTH);

    bench_skiplist sl(ARENA_BYTES);
    auto wg = hedge::async::wait_group::make_shared();
    wg->set(N);

    auto t0 = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N; ++i)
    {
        auto task = make_insert_task(&sl, i, wg.get());
        hedge::async::executor_pool::executor_from_static_pool()->submit_io_task(std::move(task));
    }

    wg->wait();

    auto t1 = std::chrono::high_resolution_clock::now();

    hedge::async::executor_pool::shutdown_static_pool();

    double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
    double throughput = N / elapsed_s;
    double bandwidth_mb = (throughput * (sizeof(DUMMY_VALUE) + sizeof(hedge::key_t))) / (1024.0 * 1024.0);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Inserted " << N << " keys via executor in " << elapsed_s * 1000.0 << " ms\n";
    std::cout << "Throughput: " << static_cast<size_t>(throughput) << " items/s\n";
    std::cout << "Bandwidth:  " << bandwidth_mb << " MB/s\n";
    std::cout << "Skiplist size: " << sl.size() << "\n";
}
