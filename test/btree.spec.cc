#include <algorithm>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "db/btree.h"
#include "types.h"

using namespace hedge::db;

namespace
{
    // Helper to create determinstic keys from integers for testing
    hedge::key_t make_key(uint64_t i)
    {
        std::array<uint8_t, 16> bytes = {0};
        // Fill last 8 bytes with integer in big-endian order to preserve sort order
        for(int j = 0; j < 8; ++j)
        {
            bytes[15 - j] = (i >> (j * 8)) & 0xFF;
        }
        return hedge::key_t(bytes);
    }

    // Helper to create values
    hedge::value_ptr_t make_val(uint64_t i)
    {
        return hedge::value_ptr_t(i * 10, 100, 1);
    }
} // namespace

TEST(BTreeNodeTest, Initialization)
{
    btree<hedge::key_t, hedge::value_ptr_t>::node n;
    EXPECT_TRUE(n._header._is_leaf);
    EXPECT_EQ(n._header._count, 0);
    EXPECT_EQ(n._header._head_idx, (btree<hedge::key_t, hedge::value_ptr_t>::NULL_IDX));
    EXPECT_FALSE(n.is_full());
}

TEST(BTreeNodeTest, InsertOrdered)
{
    btree<hedge::key_t, hedge::value_ptr_t>::node n;
    EXPECT_TRUE(n.insert(make_key(10), make_val(10)));
    EXPECT_TRUE(n.insert(make_key(5), make_val(5)));
    EXPECT_TRUE(n.insert(make_key(20), make_val(20)));

    auto values = n.get_values();
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values[0].first, make_key(5));
    EXPECT_EQ(values[1].first, make_key(10));
    EXPECT_EQ(values[2].first, make_key(20));
}

TEST(BTreeNodeTest, Capacity)
{
    btree<hedge::key_t, hedge::value_ptr_t>::node n;
    // Fill the node
    size_t count = 0;
    while(!n.is_full())
    {
        EXPECT_TRUE(n.insert(make_key(count), make_val(count)));
        count++;
    }
    EXPECT_EQ(n._header._count, (btree<hedge::key_t, hedge::value_ptr_t>::node::CAPACITY));
    EXPECT_TRUE(n.is_full());

    // Check values
    auto values = n.get_values();
    EXPECT_EQ(values.size(), (btree<hedge::key_t, hedge::value_ptr_t>::node::CAPACITY));
    for(size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(values[i].first, make_key(i));
        EXPECT_EQ(values[i].second, make_val(i));
    }
}

TEST(BTreeTest, InsertRootSplit)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    size_t capacity = btree<hedge::key_t, hedge::value_ptr_t>::node::CAPACITY;

    // Fill root
    for(size_t i = 0; i < capacity; ++i)
    {
        EXPECT_TRUE(tree.insert(make_key(i), make_val(i)));
    }

    // Next insert should cause split
    EXPECT_TRUE(tree.insert(make_key(capacity), make_val(capacity)));

    auto root = tree.get_root();
    EXPECT_FALSE(root->_header._is_leaf);
    EXPECT_EQ(root->_header._count, 1); // Median promoted

    // Check if values are preserved
    EXPECT_NE(root->_header._left_child, nullptr);
    EXPECT_NE(root->_entries[root->_header._head_idx]._child, nullptr);
}

TEST(BTreeTest, LargeInsertRandom)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    std::vector<hedge::key_t> data;
    const int N = 10000;

    std::mt19937 rng(42);
    uuids::uuid_random_generator gen(rng);

    for(int i = 0; i < N; ++i)
    {
        data.push_back(gen());
    }

    for(const auto& x : data)
    {
        EXPECT_TRUE(tree.insert(x, hedge::value_ptr_t(100, 10, 1)));
    }

    // Basic verification: root should be internal
    EXPECT_FALSE(tree.get_root()->_header._is_leaf);
}

// Helper to collect all values from tree in order
template <typename K, typename V>
void collect_values(typename btree<K, V>::node* n, std::vector<K>& out)
{
    if(n->_header._is_leaf)
    {
        auto v = n->get_values();
        for(const auto& pair : v)
        {
            out.push_back(pair.first);
        }
    }
    else
    {
        if(n->_header._left_child)
            collect_values<K, V>(n->_header._left_child, out);

        uint32_t curr = n->_header._head_idx;
        while(curr != btree<K, V>::NULL_IDX)
        {
            out.push_back(n->_entries[curr].key);
            if(n->_entries[curr]._child)
                collect_values<K, V>(n->_entries[curr]._child, out);
            curr = n->_entries[curr]._next;
        }
    }
}

TEST(BTreeTest, IntegrityCheck)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    const int N = 2000;
    std::vector<hedge::key_t> expected;
    for(int i = 0; i < N; ++i)
    {
        EXPECT_TRUE(tree.insert(make_key(i), make_val(i)));
        expected.push_back(make_key(i));
    }

    std::vector<hedge::key_t> actual;
    collect_values<hedge::key_t, hedge::value_ptr_t>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(actual, expected);
}

TEST(BTreeTest, IntegrityCheckRandom)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    const int N = 2000;
    std::vector<int> input_ints;
    for(int i = 0; i < N; ++i)
        input_ints.push_back(i);

    std::mt19937 g(123);
    std::shuffle(input_ints.begin(), input_ints.end(), g);

    for(int x : input_ints)
    {
        EXPECT_TRUE(tree.insert(make_key(x), make_val(x)));
    }

    // Expected keys are sorted
    std::sort(input_ints.begin(), input_ints.end());
    std::vector<hedge::key_t> expected;
    for(int x : input_ints)
        expected.push_back(make_key(x));

    std::vector<hedge::key_t> actual;
    collect_values<hedge::key_t, hedge::value_ptr_t>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(actual, expected);
}

TEST(BTreeTest, BenchmarkInsert200K)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    const size_t N = 200000;

    auto start = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N; ++i)
    {
        if(!tree.insert(make_key(i), make_val(i)))
        {
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
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    const int N = 100000;
    const int NUM_THREADS = 8;
    std::vector<std::thread> threads;

    auto worker = [&](int start, int end)
    {
        for(int i = start; i < end; ++i)
        {
            if(!tree.insert(make_key(i), make_val(i)))
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
    std::vector<hedge::key_t> actual;
    collect_values<hedge::key_t, hedge::value_ptr_t>(tree.get_root(), actual);

    ASSERT_EQ(actual.size(), N);

    // Check sortedness
    for(size_t i = 1; i < actual.size(); ++i)
    {
        ASSERT_FALSE(actual[i] < actual[i - 1]) << "Not sorted at index " << i;
    }

    // Check values (since we inserted 0..N-1)
    for(int i = 0; i < N; ++i)
    {
        ASSERT_EQ(actual[i], make_key(i)) << "Value mismatch at index " << i;
    }
}

TEST(BTreeTest, ConcurrentReadWrite)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
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
                if(!tree.insert(make_key(j), make_val(j))) std::abort();
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
                tree.get(make_key(val)); // Just exercise the read path
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
        ASSERT_TRUE(tree.get(make_key(i))) << "Missing value " << i;
    }
}

TEST(BTreeTest, ConcurrentConsistencyAndVisibility)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
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
            std::mt19937_64 rng(i + 100); // Distinct seeds
            std::uniform_int_distribution<size_t> dist; // 0 to MAX_UINT64

            for(size_t j = 0; j < OPS_PER_WRITER; ++j)
            {
                size_t val_int = dist(rng);
                // Ensure value is EVEN for positive tests
                if (val_int % 2 != 0) val_int++; 

                hedge::key_t k = make_key(val_int);
                hedge::value_ptr_t v = make_val(val_int);

                if(!tree.insert(k, v)) std::abort();
                
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
                    ASSERT_TRUE(tree.get(val)) << "Latest value not found";
                }

                // Test 2: Random historical visibility (Relaxed)
                if(count > 0)
                {
                    std::uniform_int_distribution<size_t> idx_dist(0, count - 1);
                    size_t idx = idx_dist(rng);
                    hedge::key_t val = logs[w_idx].values[idx];
                    ASSERT_TRUE(tree.get(val)) << "Historical value not found";
                }

                // Test 3: Negative Lookup
                // Check a random ODD number (should not exist)
                size_t odd_val_int = val_dist(rng);
                if (odd_val_int % 2 == 0) odd_val_int++;
                
                hedge::key_t odd_key = make_key(odd_val_int);
                
                ASSERT_FALSE(tree.get(odd_key)) << "Found value that was never inserted";
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
            hedge::key_t val = log.values[k];
            ASSERT_TRUE(tree.get(val)) << "Final check: Missing value";
        }
    }
}

namespace
{
    void run_benchmark(size_t total_items, int num_threads)
    {
        btree<hedge::key_t, hedge::value_ptr_t> tree;
        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        auto worker = [&](size_t start, size_t end)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            } // Spin wait
            for(size_t i = start; i < end; ++i)
            {
                if(!tree.insert(make_key(i), make_val(i)))
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
        btree<hedge::key_t, hedge::value_ptr_t> tree;
        // 1. Populate tree
        for(size_t i = 0; i < tree_size; ++i)
        {
            if(!tree.insert(make_key(i), make_val(i)))
                std::abort();
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
                bool found = tree.get(make_key(dist(rng))).has_value();
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
        btree<hedge::key_t, hedge::value_ptr_t> tree;
        // 1. Populate tree
        for(size_t i = 0; i < tree_size; ++i)
        {
            if(!tree.insert(make_key(i), make_val(i)))
                std::abort();
        }

        // CAST TO READ ONLY
        auto* ro_tree = reinterpret_cast<btree<hedge::key_t, hedge::value_ptr_t, std::less<hedge::key_t>, true>*>(&tree);

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
                bool found = ro_tree->get(make_key(dist(rng))).has_value();
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
        auto* tree = new btree<hedge::key_t, hedge::value_ptr_t>();
        std::vector<std::thread> threads;
        std::atomic<bool> start_flag{false};

        auto worker = [&](size_t start, size_t end)
        {
            while(!start_flag.load(std::memory_order_acquire))
            {
            } // Spin wait
            for(size_t i = start; i < end; ++i)
            {
                if(!tree->insert(make_key(i), make_val(i)))
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

TEST(BTreeTest, IteratorTraversal)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    const int N = 100;
    // Insert in random order to ensure tree structure is non-trivial
    std::vector<int> data_ints;
    for(int i = 0; i < N; ++i)
        data_ints.push_back(i);
    std::mt19937 g(42);
    std::shuffle(data_ints.begin(), data_ints.end(), g);

    for(int x : data_ints)
    {
        EXPECT_TRUE(tree.insert(make_key(x), make_val(x)));
    }

    int count = 0;
    int expected_key_int = 0;
    for(auto it = tree.begin(); it != tree.end(); ++it)
    {
        auto [k, v] = *it;
        EXPECT_EQ(k, make_key(expected_key_int));
        EXPECT_EQ(v, make_val(expected_key_int));
        EXPECT_EQ(it.key(), make_key(expected_key_int));
        EXPECT_EQ(it.value(), make_val(expected_key_int));

        expected_key_int++;
        count++;
    }
    EXPECT_EQ(count, N);
}

TEST(BTreeTest, Find)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    tree.insert(make_key(10), make_val(10));
    tree.insert(make_key(5), make_val(5));
    tree.insert(make_key(20), make_val(20));
    tree.insert(make_key(15), make_val(15));

    // Find existing
    auto it = tree.find(make_key(10));
    ASSERT_NE(it, tree.end());
    EXPECT_EQ((*it).first, make_key(10));
    EXPECT_EQ(it.value(), make_val(10));

    it = tree.find(make_key(5));
    ASSERT_NE(it, tree.end());
    EXPECT_EQ((*it).first, make_key(5));

    it = tree.find(make_key(20));
    ASSERT_NE(it, tree.end());
    EXPECT_EQ((*it).first, make_key(20));

    it = tree.find(make_key(15));
    ASSERT_NE(it, tree.end());
    EXPECT_EQ((*it).first, make_key(15));

    // Find non-existing
    it = tree.find(make_key(999));
    EXPECT_EQ(it, tree.end());

    it = tree.find(make_key(0));
    EXPECT_EQ(it, tree.end());

    it = tree.find(make_key(12));
    EXPECT_EQ(it, tree.end());
}

TEST(BTreeTest, EmptyIterator)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    auto it = tree.begin();
    EXPECT_EQ(it, tree.end());

    it = tree.find(make_key(10));
    EXPECT_EQ(it, tree.end());
}

TEST(BTreeTest, UUIDKeyTest)
{
    btree<hedge::key_t, hedge::value_ptr_t> tree;
    std::mt19937 rng(42);
    uuids::uuid_random_generator gen(rng);

    const int N = 1000;
    std::vector<std::pair<hedge::key_t, hedge::value_ptr_t>> data;

    for(int i = 0; i < N; ++i)
    {
        hedge::key_t key = gen();
        hedge::value_ptr_t value(i * 100, 10, 1);

        EXPECT_TRUE(tree.insert(key, value));
        data.push_back({key, value});
    }

    // Verify
    for(const auto& p : data)
    {
        auto it = tree.find(p.first);
        ASSERT_NE(it, tree.end());
        EXPECT_EQ(it.key(), p.first);

        auto val = it.value();
        EXPECT_EQ(val.offset(), p.second.offset());
        EXPECT_EQ(val.size(), p.second.size());
        EXPECT_EQ(val.table_id(), p.second.table_id());
    }
}
