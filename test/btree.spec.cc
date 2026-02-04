#include "db/btree.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <iomanip>
#include <random>
#include <thread>
#include <vector>

using namespace hedge::db;

TEST(BTreeNodeTest, Initialization)
{
    btree<int>::node n;
    EXPECT_TRUE(n._header._is_leaf);
    EXPECT_EQ(n._header._count, 0);
    EXPECT_EQ(n._header._head_idx, btree<int>::NULL_IDX);
    EXPECT_FALSE(n.is_full());
}

TEST(BTreeNodeTest, InsertOrdered)
{
    btree<int>::node n;
    EXPECT_TRUE(n.insert(10));
    EXPECT_TRUE(n.insert(5));
    EXPECT_TRUE(n.insert(20));

    auto values = n.get_values();
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values[0], 5);
    EXPECT_EQ(values[1], 10);
    EXPECT_EQ(values[2], 20);
}

TEST(BTreeNodeTest, Capacity)
{
    btree<long>::node n;
    // Fill the node
    size_t count = 0;
    while(!n.is_full())
    {
        EXPECT_TRUE(n.insert(count++));
    }
    EXPECT_EQ(n._header._count, btree<long>::node::CAPACITY);
    EXPECT_TRUE(n.is_full());

    // Check values
    auto values = n.get_values();
    EXPECT_EQ(values.size(), btree<long>::node::CAPACITY);
    for(size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(values[i], i);
    }
}

TEST(BTreeTest, InsertRootSplit)
{
    btree<int> tree;
    size_t capacity = btree<int>::node::CAPACITY;

    // Fill root
    for(size_t i = 0; i < capacity; ++i)
    {
        EXPECT_TRUE(tree.insert(i));
    }

    // Next insert should cause split
    EXPECT_TRUE(tree.insert(capacity));

    auto root = tree.get_root();
    EXPECT_FALSE(root->_header._is_leaf);
    EXPECT_EQ(root->_header._count, 1); // Median promoted

    // Check if values are preserved
    EXPECT_NE(root->_header._left_child, nullptr);
    EXPECT_NE(root->_entries[root->_header._head_idx]._child, nullptr);
}

TEST(BTreeTest, LargeInsertRandom)
{
    btree<int> tree;
    std::vector<int> data;
    const int N = 10000;

    for(int i = 0; i < N; ++i)
    {
        data.push_back(i);
    }

    std::mt19937 g(42);
    std::shuffle(data.begin(), data.end(), g);

    for(int x : data)
    {
        EXPECT_TRUE(tree.insert(x));
    }

    // Basic verification: root should be internal
    EXPECT_FALSE(tree.get_root()->_header._is_leaf);
}

// Helper to collect all values from tree in order
template <typename T>
void collect_values(typename btree<T>::node* n, std::vector<T>& out)
{
    if(n->_header._is_leaf)
    {
        auto v = n->get_values();
        out.insert(out.end(), v.begin(), v.end());
    }
    else
    {
        if(n->_header._left_child)
            collect_values<T>(n->_header._left_child, out);

        uint32_t curr = n->_header._head_idx;
        while(curr != btree<T>::NULL_IDX)
        {
            out.push_back(n->_entries[curr]._data);
            if(n->_entries[curr]._child)
                collect_values<T>(n->_entries[curr]._child, out);
            curr = n->_entries[curr]._next;
        }
    }
}

TEST(BTreeTest, IntegrityCheck)
{
    btree<int> tree;
    const int N = 2000;
    std::vector<int> expected;
    for(int i = 0; i < N; ++i)
    {
        EXPECT_TRUE(tree.insert(i));
        expected.push_back(i);
    }

    std::vector<int> actual;
    collect_values<int>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(actual, expected);
}

TEST(BTreeTest, IntegrityCheckRandom)
{
    btree<int> tree;
    const int N = 2000;
    std::vector<int> input;
    for(int i = 0; i < N; ++i)
        input.push_back(i);

    std::mt19937 g(123);
    std::shuffle(input.begin(), input.end(), g);

    for(int x : input)
    {
        EXPECT_TRUE(tree.insert(x));
    }

    std::sort(input.begin(), input.end());

    std::vector<int> actual;
    collect_values<int>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), input.size());
    EXPECT_EQ(actual, input);
}

TEST(BTreeTest, BenchmarkInsert200K)
{
    btree<size_t> tree;
    const size_t N = 200000;

    auto start = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N; ++i)
    {
        if (!tree.insert(i)) {
             std::cerr << "Allocation failed at " << i << std::endl;
             std::abort();
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    double ops = N / elapsed.count();
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "[ PERF     ] Inserted " << N << " items in " << elapsed.count() << "s. M OPS/s: " << ops / 1'000'000 << std::endl;
}

TEST(BTreeTest, ConcurrentInsertCorrectness)
{
    btree<int> tree;
    const int N = 100000;
    const int NUM_THREADS = 8;
    std::vector<std::thread> threads;

    auto worker = [&](int start, int end)
    {
        for(int i = start; i < end; ++i)
        {
            if(!tree.insert(i)) std::abort();
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
    std::vector<int> actual;
    collect_values<int>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), N);

    // Check sortedness
    for(size_t i = 1; i < actual.size(); ++i)
    {
        ASSERT_GE(actual[i], actual[i - 1]) << "Not sorted at index " << i;
    }

    // Check values (since we inserted 0..N-1)
    for(int i = 0; i < N; ++i)
    {
        ASSERT_EQ(actual[i], i) << "Value mismatch at index " << i;
    }
}

TEST(BTreeTest, ConcurrentReadWrite)
{
    btree<int> tree;
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
            int start = i * ITEMS_PER_THREAD;
            int end = start + ITEMS_PER_THREAD;
            for(int j = start; j < end; ++j)
            {
                if(!tree.insert(j)) std::abort();
                if(j % 100 == 0) std::this_thread::yield();
            } });
    }

    // Readers
    for(int i = 0; i < READERS; ++i)
    {
        threads.emplace_back([&]()
                             {
            std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> dist(0, N - 1);
            while(!done.load(std::memory_order_acquire))
            {
                int val = dist(rng);
                tree.contains(val); // Just exercise the read path
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
    for(int i = 0; i < N; ++i)
    {
        ASSERT_TRUE(tree.contains(i)) << "Missing value " << i;
    }
}

TEST(BTreeTest, ConcurrentConsistencyAndVisibility)
{
    btree<size_t> tree;
    const size_t NUM_WRITERS = 4;
    const size_t NUM_READERS = 4;
    const size_t OPS_PER_WRITER = 100000;

    struct alignas(64) ThreadLog
    {
        std::vector<size_t> values;
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
            std::mt19937_64 rng(i + 100); // Distinct seeds
            std::uniform_int_distribution<size_t> dist; // 0 to MAX_UINT64

            for(size_t j = 0; j < OPS_PER_WRITER; ++j)
            {
                size_t val = dist(rng);
                // Ensure value is EVEN for positive tests
                if (val % 2 != 0) val++; 

                if(!tree.insert(val)) std::abort();
                
                // Publish value
                logs[i].values[j] = val;
                
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
                    size_t val = logs[w_idx].values[count - 1];
                    ASSERT_TRUE(tree.contains(val)) << "Latest value not found: " << val;
                }

                // Test 2: Random historical visibility (Relaxed)
                if(count > 0)
                {
                    std::uniform_int_distribution<size_t> idx_dist(0, count - 1);
                    size_t idx = idx_dist(rng);
                    size_t val = logs[w_idx].values[idx];
                    ASSERT_TRUE(tree.contains(val)) << "Historical value not found: " << val;
                }

                // Test 3: Negative Lookup
                // Check a random ODD number (should not exist)
                size_t odd_val = val_dist(rng);
                if (odd_val % 2 == 0) odd_val++;
                
                // There is a microscopic chance of collision if we used full range randoms,
                // but since we strictly write EVENS, any ODD number should NOT be there.
                ASSERT_FALSE(tree.contains(odd_val)) << "Found value that was never inserted: " << odd_val;
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
    for(const auto& log : logs)
    {
        size_t count = log.published_count.load(std::memory_order_relaxed);
        for(size_t k = 0; k < count; ++k)
        {
            size_t val = log.values[k];
            ASSERT_TRUE(tree.contains(val)) << "Final check: Missing value " << val;
        }
    }
}

namespace
{
    void run_benchmark(size_t total_items, int num_threads)
    {
        btree<size_t> tree;
        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        auto worker = [&](size_t start, size_t end)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            } // Spin wait
            for(size_t i = start; i < end; ++i)
            {
                if(!tree.insert(i)) std::abort();
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
        btree<size_t> tree;
        // 1. Populate tree
        for(size_t i = 0; i < tree_size; ++i)
        {
            if(!tree.insert(i)) std::abort();
        }

        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        size_t lookups_per_thread = total_lookups / num_threads;

        auto worker = [&](size_t count)
        {
            std::mt19937_64 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<size_t> dist(0, tree_size - 1);

            while(!start_flag.load(std::memory_order_acquire))
            {
            }

            for(size_t i = 0; i < count; ++i)
            {
                // We use volatile to ensure the call isn't optimized away (though unlikely for a non-trivial function)
                bool found = tree.contains(dist(rng));
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

    void run_readonly_benchmark(size_t tree_size, size_t total_lookups, int num_threads)
    {
        btree<size_t> tree;
        // 1. Populate tree
        for(size_t i = 0; i < tree_size; ++i)
        {
            if(!tree.insert(i)) std::abort();
        }

        // CAST TO READ ONLY
        auto* ro_tree = reinterpret_cast<btree<size_t, true>*>(&tree);

        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        size_t lookups_per_thread = total_lookups / num_threads;

        auto worker = [&](size_t count)
        {
            std::mt19937_64 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<size_t> dist(0, tree_size - 1);

            while(!start_flag.load(std::memory_order_acquire))
            {
            }

            for(size_t i = 0; i < count; ++i)
            {
                bool found = ro_tree->contains(dist(rng));
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
        std::cout << "[ READ ONLY] Tree Size: " << tree_size << ", Threads: " << num_threads
                  << ", Lookups: " << total_lookups
                  << ", Time: " << elapsed.count() << "s, M OPS/s: " << ops / 1'000'000 << std::endl;
    }
    void run_deallocation_benchmark(size_t total_items, int num_threads)
    {
        // Increase budget for large test
        // 5M items = ~160MB of nodes. 1GB default is fine.
        auto* tree = new btree<size_t>();
        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        auto worker = [&](size_t start, size_t end)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            } // Spin wait
            for(size_t i = start; i < end; ++i)
            {
                if(!tree->insert(i)) std::abort();
            }
        };

        size_t items_per_thread = total_items / num_threads;
        for(int i = 0; i < num_threads; ++i)
        {
            size_t start = i * items_per_thread;
            size_t end = (i == num_threads - 1) ? total_items : (i + 1) * items_per_thread;
            threads.emplace_back(worker, start, end);
        }

        start_flag.store(true, std::memory_order_release);

        for(auto& t : threads)
            t.join();

        // Measurement starts here
        auto start_time = std::chrono::high_resolution_clock::now();
        delete tree;
        auto end_time = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed = end_time - start_time;

        std::cout << std::fixed << std::setprecision(4);
        std::cout << "[ DEALLOC  ] Items: " << total_items
                  << ", Time: " << elapsed.count() << "s" << std::endl;
    }
} // namespace

TEST(BTreeTest, DeallocationBenchmark)
{
    std::vector<size_t> items_counts = {1000000, 2000000, 5000000};
    int threads = 8; // Fixed threads for population

    for(size_t n : items_counts)
    {
        run_deallocation_benchmark(n, threads);
    }
}

TEST(BTreeTest, ScalingBenchmark)
{
    std::vector<size_t> items_counts = {200000, 500000, 1000000, 2000000};
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

TEST(BTreeTest, ReadScalingBenchmark)
{
    size_t tree_size = 1000000;      // 1M items
    size_t total_lookups = 10000000; // 10M lookups
    std::vector<int> thread_counts = {1, 2, 4, 8, 12, 16};

    for(int t : thread_counts)
    {
        run_read_benchmark(tree_size, total_lookups, t);
    }
}

TEST(BTreeTest, ReadOnlyScalingBenchmark)
{
    size_t tree_size = 1000000;      // 1M items
    size_t total_lookups = 10000000; // 10M lookups
    std::vector<int> thread_counts = {1, 2, 4, 8, 12, 16};

    for(int t : thread_counts)
    {
        run_readonly_benchmark(tree_size, total_lookups, t);
    }
}
