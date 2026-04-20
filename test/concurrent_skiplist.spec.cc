#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

// NOLINTBEGIN

// Helper to eat stream output from CHECK macros
struct NullStream
{
    template <typename T>
    NullStream& operator<<(const T&) { return *this; }
};

// Define macros expected by Folly code
#ifndef DCHECK
#define DCHECK(x)  \
    if(!(x))       \
    {              \
        assert(x); \
    }              \
    NullStream()
#endif

#ifndef CHECK_OP
#define CHECK_OP(a, b, op) \
    if(!((a)op(b)))        \
    {                      \
        assert((a)op(b));  \
    }                      \
    NullStream()
#endif

#ifndef CHECK_EQ
#define CHECK_EQ(a, b) CHECK_OP(a, b, ==)
#endif

#ifndef CHECK_NE
#define CHECK_NE(a, b) CHECK_OP(a, b, !=)
#endif

#ifndef CHECK_LE
#define CHECK_LE(a, b) CHECK_OP(a, b, <=)
#endif

#ifndef CHECK_LT
#define CHECK_LT(a, b) CHECK_OP(a, b, <)
#endif

#ifndef CHECK_GE
#define CHECK_GE(a, b) CHECK_OP(a, b, >=)
#endif

#ifndef CHECK_GT
#define CHECK_GT(a, b) CHECK_OP(a, b, >)
#endif

#ifndef DCHECK_GT
#define DCHECK_GT(a, b) CHECK_GT(a, b)
#endif

#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) CHECK_EQ(a, b)
#endif

#ifndef FOLLY_UNLIKELY
#define FOLLY_UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include "../src/db/folly/concurrent_skip_list/concurrent_skiplist.h"
#pragma GCC diagnostic pop
#include "../src/single_buffer_arena_allocator.h"
#include "async/rw_sync.h"
#include "types.h"

using namespace hedge;

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

// Data structure to hold Key and Value
struct NodeData
{
    hedge::key_t key;
    hedge::value_ptr_t value;

    NodeData() = default;
    NodeData(hedge::key_t k, hedge::value_ptr_t v) : key(k), value(v) {}

    // For search purposes
    explicit NodeData(hedge::key_t k) : key(k), value() {}

    bool operator<(const NodeData& other) const
    {
        return key < other.key;
    }

    bool operator==(const NodeData& other) const
    {
        return key == other.key;
    }
};

struct NodeDataCompare
{
    bool operator()(const NodeData& a, const NodeData& b) const
    {
        return a.key < b.key;
    }
};

namespace
{
    // Helper to create deterministic keys
    hedge::key_t make_key(uint64_t i)
    {
        std::array<std::byte, 16> bytes = {0};
        for(int j = 0; j < 8; ++j)
        {
            bytes[15 - j] = (i >> (j * 8)) & 0xFF;
        }
        return hedge::key_t(bytes);
    }

    hedge::value_ptr_t make_val(uint64_t i)
    {
        return hedge::value_ptr_t(i * 10, 100, 1);
    }
} // namespace

TEST(ConcurrentSkipListTest, Initialization)
{
    single_buffer_arena_allocator arena(1024 * 1024); // 1MB
    StdArenaAllocator<std::byte> alloc(arena);

    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(12, alloc);

    EXPECT_TRUE(sl->empty());
    EXPECT_EQ(sl->size(), 0);
}

TEST(ConcurrentSkipListTest, InsertOrdered)
{
    single_buffer_arena_allocator arena(1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(12, alloc);
    auto accessor = SkipList::Accessor(sl);

    EXPECT_TRUE(accessor.add(NodeData(make_key(10), make_val(10))));
    EXPECT_TRUE(accessor.add(NodeData(make_key(5), make_val(5))));
    EXPECT_TRUE(accessor.add(NodeData(make_key(20), make_val(20))));

    EXPECT_EQ(accessor.size(), 3);

    auto it = accessor.begin();
    EXPECT_EQ(it->key, make_key(5));
    ++it;
    EXPECT_EQ(it->key, make_key(10));
    ++it;
    EXPECT_EQ(it->key, make_key(20));
}

TEST(ConcurrentSkipListTest, LargeInsertRandom)
{
    single_buffer_arena_allocator arena(32 * 1024 * 1024); // 32MB should be enough for 10k nodes
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);
    auto accessor = SkipList::Accessor(sl);

    const int N = 10000;
    std::vector<hedge::key_t> data;
    std::mt19937 rng(42);
    uuids::uuid_random_generator gen(rng);

    for(int i = 0; i < N; ++i)
    {
        auto u = gen();
        auto b = u.as_bytes();
        data.push_back(hedge::key_t(b.data(), b.size()));
    }

    for(const auto& x : data)
    {
        EXPECT_TRUE(accessor.add(NodeData(x, hedge::value_ptr_t(100, 10, 1))));
    }

    EXPECT_EQ(accessor.size(), N);
}

TEST(ConcurrentSkipListTest, IntegrityCheck)
{
    single_buffer_arena_allocator arena(16 * 1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);
    auto accessor = SkipList::Accessor(sl);

    const int N = 2000;
    std::vector<hedge::key_t> expected;
    for(int i = 0; i < N; ++i)
    {
        accessor.add(NodeData(make_key(i), make_val(i)));
        expected.push_back(make_key(i));
    }

    std::vector<hedge::key_t> actual;
    for(auto it = accessor.begin(); it != accessor.end(); ++it)
    {
        actual.push_back(it->key);
    }

    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(actual, expected);
}

TEST(ConcurrentSkipListTest, BenchmarkInsert200K)
{
    // 200K nodes.
    single_buffer_arena_allocator arena(256 * 1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);
    auto accessor = SkipList::Accessor(sl);

    const size_t N = 200000;

    auto start = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N; ++i)
    {
        if(!accessor.add(NodeData(make_key(i), make_val(i))))
        {
            std::cerr << "Insertion failed at " << i << std::endl;
            std::abort();
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    double ops = N / elapsed.count();
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "[ PERF     ] Inserted " << N << " items in " << elapsed.count() << "s. M OPS/s: " << ops / 1'000'000 << std::endl;
}

TEST(ConcurrentSkipListTest, ConcurrentInsertCorrectness)
{
    single_buffer_arena_allocator arena(256 * 1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);

    const int N = 100000;
    const int NUM_THREADS = 8;
    std::vector<std::thread> threads;

    auto worker = [&](int start, int end)
    {
        auto accessor = SkipList::Accessor(sl);
        for(int i = start; i < end; ++i)
        {
            if(!accessor.add(NodeData(make_key(i), make_val(i))))
                std::abort();
        }
    };

    int items_per_thread = N / NUM_THREADS;
    for(int i = 0; i < NUM_THREADS; ++i)
    {
        int start = i * items_per_thread;
        int end = (i == NUM_THREADS - 1) ? N : (i + 1) * items_per_thread;
        threads.emplace_back(worker, start, end);
    }

    for(auto& t : threads)
        t.join();

    // Verify
    auto accessor = SkipList::Accessor(sl);
    EXPECT_EQ(accessor.size(), N);

    std::vector<hedge::key_t> actual;
    for(auto it = accessor.begin(); it != accessor.end(); ++it)
    {
        actual.push_back(it->key);
    }

    ASSERT_EQ(actual.size(), N);

    // Check sortedness and values
    for(size_t i = 0; i < actual.size(); ++i)
    {
        if(i > 0)
        {
            ASSERT_FALSE(actual[i] < actual[i - 1]) << "Not sorted at index " << i;
        }
        ASSERT_EQ(actual[i], make_key(i)) << "Value mismatch at index " << i;
    }
}

TEST(ConcurrentSkipListTest, Find)
{
    single_buffer_arena_allocator arena(1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(12, alloc);
    auto accessor = SkipList::Accessor(sl);

    accessor.add(NodeData(make_key(10), make_val(10)));
    accessor.add(NodeData(make_key(5), make_val(5)));
    accessor.add(NodeData(make_key(20), make_val(20)));
    accessor.add(NodeData(make_key(15), make_val(15)));

    // Find existing
    auto it = accessor.find(NodeData(make_key(10)));
    ASSERT_NE(it, accessor.end());
    EXPECT_EQ(it->key, make_key(10));
    EXPECT_EQ(it->value, make_val(10));

    it = accessor.find(NodeData(make_key(5)));
    ASSERT_NE(it, accessor.end());
    EXPECT_EQ(it->key, make_key(5));

    // Find non-existing
    it = accessor.find(NodeData(make_key(999)));
    EXPECT_EQ(it, accessor.end());
}

TEST(ConcurrentSkipListTest, IteratorTraversal)
{
    single_buffer_arena_allocator arena(1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(12, alloc);
    auto accessor = SkipList::Accessor(sl);

    const int N = 100;
    std::vector<int> data_ints;
    for(int i = 0; i < N; ++i)
        data_ints.push_back(i);
    std::mt19937 g(42);
    std::shuffle(data_ints.begin(), data_ints.end(), g);

    for(int x : data_ints)
    {
        EXPECT_TRUE(accessor.add(NodeData(make_key(x), make_val(x))));
    }

    int count = 0;
    int expected_key_int = 0;
    for(auto it = accessor.begin(); it != accessor.end(); ++it)
    {
        EXPECT_EQ(it->key, make_key(expected_key_int));
        EXPECT_EQ(it->value, make_val(expected_key_int));
        expected_key_int++;
        count++;
    }
    EXPECT_EQ(count, N);
}

TEST(ConcurrentSkipListTest, EmptyIterator)
{
    single_buffer_arena_allocator arena(1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(12, alloc);
    auto accessor = SkipList::Accessor(sl);

    auto it = accessor.begin();
    EXPECT_EQ(it, accessor.end());

    it = accessor.find(NodeData(make_key(10)));
    EXPECT_EQ(it, accessor.end());
}

TEST(ConcurrentSkipListTest, ConcurrentReadWrite)
{
    single_buffer_arena_allocator arena(64 * 1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);

    const int N = 40000;
    const int WRITERS = 4;
    const int READERS = 4;
    const int ITEMS_PER_THREAD = N / WRITERS;

    std::atomic<bool> done{false};
    std::vector<std::thread> threads;

    // Writers
    for(int i = 0; i < WRITERS; ++i)
    {
        threads.emplace_back([&, i]()
                             {
            auto accessor = SkipList::Accessor(sl);
            int start = i * ITEMS_PER_THREAD;
            int end = start + ITEMS_PER_THREAD;
            for(int j = start; j < end; ++j)
            {
                if(!accessor.add(NodeData(make_key(j), make_val(j)))) std::abort();
                if(j % 100 == 0) std::this_thread::yield();
            } });
    }

    // Readers
    for(int i = 0; i < READERS; ++i)
    {
        threads.emplace_back([&]()
                             {
            auto accessor = SkipList::Accessor(sl);
            std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> dist(0, N - 1);
            while(!done.load(std::memory_order_acquire))
            {
                int val = dist(rng);
                accessor.find(NodeData(make_key(val))); // Just exercise the read path
            } });
    }

    // Wait for writers
    for(int i = 0; i < WRITERS; ++i)
    {
        threads[i].join();
    }

    done.store(true, std::memory_order_release);

    // Wait for readers
    for(int i = WRITERS; i < WRITERS + READERS; ++i)
    {
        threads[i].join();
    }

    // Verify all present
    auto accessor = SkipList::Accessor(sl);
    for(int i = 0; i < N; ++i)
    {
        auto it = accessor.find(NodeData(make_key(i)));
        ASSERT_NE(it, accessor.end()) << "Missing value " << i;
    }
}

TEST(ConcurrentSkipListTest, ConcurrentConsistencyAndVisibility)
{
    single_buffer_arena_allocator arena(128 * 1024 * 1024);
    StdArenaAllocator<std::byte> alloc(arena);
    using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
    auto sl = SkipList::createInstance(24, alloc);

    const size_t NUM_WRITERS = 4;
    const size_t NUM_READERS = 4;
    const size_t OPS_PER_WRITER = 100000;

    struct alignas(64) ThreadLog
    {
        std::vector<hedge::key_t> values;
        std::atomic<size_t> published_count{0};

        ThreadLog() { values.resize(OPS_PER_WRITER); }
    };

    std::vector<ThreadLog> logs(NUM_WRITERS);
    std::atomic<bool> done{false};
    std::vector<std::thread> threads;

    // Writers
    for(size_t i = 0; i < NUM_WRITERS; ++i)
    {
        threads.emplace_back([&, i]()
                             {
            auto accessor = SkipList::Accessor(sl);
            std::mt19937_64 rng(i + 100); // Distinct seeds
            std::uniform_int_distribution<size_t> dist; // 0 to MAX_UINT64

            for(size_t j = 0; j < OPS_PER_WRITER; ++j)
            {
                size_t val_int = dist(rng);
                // Ensure value is EVEN for positive tests
                if (val_int % 2 != 0) val_int++; 

                hedge::key_t k = make_key(val_int);
                hedge::value_ptr_t v = make_val(val_int);

                if(!accessor.add(NodeData(k, v))) std::abort();
                
                // Publish value
                logs[i].values[j] = k;
                
                // Ensure insert is fully visible before publishing
                std::atomic_thread_fence(std::memory_order_release);
                logs[i].published_count.store(j + 1, std::memory_order_release);
                
                if(j % 100 == 0) std::this_thread::yield();
            } });
    }

    // Readers
    for(size_t i = 0; i < NUM_READERS; ++i)
    {
        threads.emplace_back([&, i]()
                             {
            auto accessor = SkipList::Accessor(sl);
            std::mt19937_64 rng(i + 200);
            std::uniform_int_distribution<size_t> writer_dist(0, NUM_WRITERS - 1);
            std::uniform_int_distribution<size_t> val_dist;

            while(!done.load(std::memory_order_acquire))
            {
                // Test 1: Immediate visibility (Strict)
                // Check the most recently added item from a random writer
                size_t w_idx = writer_dist(rng);
                size_t count = logs[w_idx].published_count.load(std::memory_order_acquire);
                
                // Enforce global visibility order
                std::atomic_thread_fence(std::memory_order_acquire);

                if(count > 0)
                {
                    hedge::key_t val = logs[w_idx].values[count - 1];
                    auto it = accessor.find(NodeData(val));
                    ASSERT_NE(it, accessor.end()) << "Latest value not found";
                }

                // Test 2: Random historical visibility (Relaxed)
                if(count > 0)
                {
                    std::uniform_int_distribution<size_t> idx_dist(0, count - 1);
                    size_t idx = idx_dist(rng);
                    hedge::key_t val = logs[w_idx].values[idx];
                    auto it = accessor.find(NodeData(val));
                    ASSERT_NE(it, accessor.end()) << "Historical value not found";
                }

                // Test 3: Negative Lookup
                // Check a random ODD number (should not exist)
                size_t odd_val_int = val_dist(rng);
                if (odd_val_int % 2 == 0) odd_val_int++;
                
                hedge::key_t odd_key = make_key(odd_val_int);
                
                auto it = accessor.find(NodeData(odd_key));
                ASSERT_EQ(it, accessor.end()) << "Found value that was never inserted";
            } });
    }

    // Wait for writers
    for(size_t i = 0; i < NUM_WRITERS; ++i)
    {
        threads[i].join();
    }

    // Give readers a moment to verify final state then stop
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    done.store(true, std::memory_order_release);

    for(size_t i = NUM_WRITERS; i < NUM_WRITERS + NUM_READERS; ++i)
    {
        threads[i].join();
    }

    // Final single-threaded verification of all inserted values
    auto accessor = SkipList::Accessor(sl);
    for(const auto& log : logs)
    {
        size_t count = log.published_count.load(std::memory_order_relaxed);
        for(size_t k = 0; k < count; ++k)
        {
            hedge::key_t val = log.values[k];
            auto it = accessor.find(NodeData(val));
            ASSERT_NE(it, accessor.end()) << "Final check: Missing value";
        }
    }
}

namespace
{

    void run_benchmark(size_t total_items, int num_threads)
    {
        single_buffer_arena_allocator arena(static_cast<size_t>(total_items) * 128); // Estimate size
        StdArenaAllocator<std::byte> alloc(arena);
        using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
        auto sl = SkipList::createInstance(24, alloc);

        std::vector<hedge::key_t> keys;
        keys.reserve(total_items);
        std::mt19937 rng(42);
        uuids::uuid_random_generator gen(rng);
        for(size_t i = 0; i < total_items; ++i)
        {
            auto u = gen();
            auto b = u.as_bytes();
            keys.push_back(hedge::key_t(b.data(), b.size()));
        }

        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};
        auto accessor = SkipList::Accessor(sl);

        auto worker = [&](size_t start, size_t end)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            } // Spin wait
            for(size_t i = start; i < end; ++i)
            {
                if(!accessor.add(NodeData(keys[i], make_val(i))))
                    std::abort();
            }
        };

        size_t items_per_thread = total_items / num_threads;
        for(int i = 0; i < num_threads; ++i)
        {
            size_t start = i * items_per_thread;
            size_t end = (i == num_threads - 1) ? total_items : (i + 1) * items_per_thread;
            threads.emplace_back(worker, start, end);
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        start_flag.store(true, std::memory_order_release);

        for(auto& t : threads)
            t.join();

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end_time - start_time;
        double ops = total_items / elapsed.count();

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "[ PERF     ] Items: " << total_items << ", Threads: " << num_threads
                  << ", Time: " << elapsed.count() << "s, M OPS/s: " << ops / 1'000'000 << std::endl;
    }

    void run_read_benchmark(size_t tree_size, size_t total_lookups, int num_threads)
    {
        single_buffer_arena_allocator arena(static_cast<size_t>(tree_size) * 128);
        StdArenaAllocator<std::byte> alloc(arena);
        using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
        auto sl = SkipList::createInstance(24, alloc);

        {
            auto accessor = SkipList::Accessor(sl);
            // 1. Populate tree
            for(size_t i = 0; i < tree_size; ++i)
            {
                if(!accessor.add(NodeData(make_key(i), make_val(i))))
                    std::abort();
            }
        }

        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        size_t lookups_per_thread = total_lookups / num_threads;

        auto worker = [&](size_t count)
        {
            auto accessor = SkipList::Accessor(sl);
            std::mt19937_64 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<size_t> dist(0, tree_size - 1);

            while(!start_flag.load(std::memory_order_acquire))
            {
            }

            for(size_t i = 0; i < count; ++i)
            {
                // We use volatile to ensure the call isn't optimized away (though unlikely for a non-trivial function)
                bool found = (accessor.find(NodeData(make_key(dist(rng)))) != accessor.end());
                if(!found)
                {
                    // Should theoretically not happen if populated 0..N-1 and we query that range
                    // But we won't assert here to avoid polluting benchmark with branch misses/aborts
                }
                (void)found;
            }
        };

        for(int i = 0; i < num_threads; ++i)
        {
            threads.emplace_back(worker, lookups_per_thread);
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        start_flag.store(true, std::memory_order_release);

        for(auto& t : threads)
            t.join();

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end_time - start_time;
        double ops = total_lookups / elapsed.count();

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "[ READ PERF] Tree Size: " << tree_size << ", Threads: " << num_threads
                  << ", Lookups: " << total_lookups
                  << ", Time: " << elapsed.count() << "s, M OPS/s: " << ops / 1'000'000 << std::endl;
    }

    void run_rw_sync_benchmark(size_t total_items, int num_threads)
    {
        single_buffer_arena_allocator arena(static_cast<size_t>(total_items) * 128);
        StdArenaAllocator<std::byte> alloc(arena);
        using SkipList = folly::ConcurrentSkipList<NodeData, NodeDataCompare, StdArenaAllocator<std::byte>>;
        auto sl = SkipList::createInstance(24, alloc);

        std::vector<hedge::key_t> keys;
        keys.reserve(total_items);
        std::mt19937 rng(42);
        uuids::uuid_random_generator gen(rng);
        for(size_t i = 0; i < total_items; ++i)
        {
            auto u = gen();
            auto b = u.as_bytes();
            keys.push_back(hedge::key_t(b.data(), b.size()));
        }

        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};
        auto accessor = SkipList::Accessor(sl);

        // rw_sync with one slot per thread, mirroring memtable::put
        hedge::async::rw_sync<std::monostate> rw_gate(num_threads);

        auto worker = [&](size_t start, size_t end, size_t thread_idx)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            }
            for(size_t i = start; i < end; ++i)
            {
                auto guard = rw_gate.acquire_writer(thread_idx % num_threads);
                if(!guard)
                    std::abort();
                if(!accessor.add(NodeData(keys[i], make_val(i))))
                    std::abort();
                // ~guard() → seq_cst store(0)
            }
        };

        size_t items_per_thread = total_items / num_threads;
        for(int i = 0; i < num_threads; ++i)
        {
            size_t start = i * items_per_thread;
            size_t end = (i == num_threads - 1) ? total_items : (i + 1) * items_per_thread;
            threads.emplace_back(worker, start, end, static_cast<size_t>(i));
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        start_flag.store(true, std::memory_order_release);

        for(auto& t : threads)
            t.join();

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end_time - start_time;
        double ops = total_items / elapsed.count();

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "[ RW_SYNC  ] Items: " << total_items << ", Threads: " << num_threads
                  << ", Time: " << elapsed.count() << "s, M OPS/s: " << ops / 1'000'000 << std::endl;
    }

} // namespace

TEST(ConcurrentSkipListTest, ScalingBenchmark)
{
    std::vector<size_t> items_counts = {150000, 2000000};
    std::vector<int> thread_counts = {1, 2, 4, 8, 10, 12, 16};

    for(size_t n : items_counts)
    {
        for(int t : thread_counts)
        {
            run_benchmark(n, t);
        }
        std::cout << "---------------------------------------------------" << std::endl;
    }
}

TEST(ConcurrentSkipListTest, ReadScalingBenchmark)
{
    size_t tree_size = 1000000;      // 1M items
    size_t total_lookups = 10000000; // 10M lookups
    std::vector<int> thread_counts = {1, 2, 4, 8, 12, 16};

    for(int t : thread_counts)
    {
        run_read_benchmark(tree_size, total_lookups, t);
    }
}

TEST(ConcurrentSkipListTest, RwSyncOverheadBenchmark)
{
    std::vector<size_t> items_counts = {150000, 2000000};
    std::vector<int> thread_counts = {1, 2, 4, 8, 10, 12, 16};

    for(size_t n : items_counts)
    {
        for(int t : thread_counts)
        {
            run_benchmark(n, t);
            run_rw_sync_benchmark(n, t);
        }
        std::cout << "---------------------------------------------------" << std::endl;
    }
}

// NOLINTEND