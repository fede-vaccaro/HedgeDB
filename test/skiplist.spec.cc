// THIS FILE WAS WRITTEN FROM AN LLM AND HUMAN INSPECTED
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <random>
#include <ranges>
#include <thread>
#include <vector>

#include "skiplist.h"
#include "uuid.h" // Assumes uuid.h is in the include path

namespace hedge
{
    namespace test
    {

        // Comparator for the skiplist (T is uuids::uuid)
        struct UuidComparator
        {
            bool operator()(const uuids::uuid& a, const uuids::uuid& b) const
            {
                return a < b;
            }
        };

        class SkiplistUuidTest : public ::testing::Test
        {
        protected:
            std::mt19937 _rng;
            uuids::uuid_random_generator _uuid_gen;

            SkiplistUuidTest() : _rng(std::random_device{}()), _uuid_gen(_rng) {}

            uuids::uuid random_uuid()
            {
                return _uuid_gen();
            }
        };

        TEST_F(SkiplistUuidTest, BasicUuidInsertAndFind)
        {
            hedge::skiplist<uuids::uuid>::config cfg;
            cfg.item_capacity = 1000;
            hedge::skiplist<uuids::uuid> list(cfg);
            UuidComparator cmp;

            auto id1 = random_uuid();
            auto id2 = random_uuid();
            auto id3 = random_uuid();

            list.insert(id1, cmp);
            list.insert(id2, cmp);

            ASSERT_EQ(list.size(), 2);

            auto res1 = list.find(id1, cmp);
            ASSERT_TRUE(res1.has_value());
            ASSERT_EQ(res1.value(), id1);

            auto res2 = list.find(id2, cmp);
            ASSERT_TRUE(res2.has_value());
            ASSERT_EQ(res2.value(), id2);

            auto res3 = list.find(id3, cmp); // Not inserted
            ASSERT_FALSE(res3.has_value());
        }

        TEST_F(SkiplistUuidTest, LargeDatasetUuid200k)
        {
            // Realistic load test with 200,000 UUIDs
            const size_t N = 200'000;
            hedge::skiplist<uuids::uuid>::config cfg;
            cfg.item_capacity = N;
            cfg.p = 0.5; // Probabilistic tuning for density
            hedge::skiplist<uuids::uuid> list(cfg);
            UuidComparator cmp;

            std::vector<uuids::uuid> keys;
            keys.reserve(N);

            // Generate 200k unique UUIDs
            for(size_t i = 0; i < N; ++i)
            {
                keys.push_back(random_uuid());
            }

            // Timer start
            auto start = std::chrono::high_resolution_clock::now();

            for(const auto& key : keys)
            {
                list.insert(key, cmp);
            }

            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Insert 200k UUIDs took: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                      << " ms" << std::endl;

            ASSERT_EQ(list.size(), N);

            // Verification phase (check a subset or all)
            // Checking all 200k ensures full integrity
            start = std::chrono::high_resolution_clock::now();

            for(const auto& key : keys)
            {
                auto res = list.find(key, cmp);
                ASSERT_TRUE(res.has_value()) << "Failed to find UUID: " << uuids::to_string(key);
                ASSERT_EQ(res.value(), key);
            }

            end = std::chrono::high_resolution_clock::now();
            std::cout << "Lookup 200k UUIDs took: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                      << " ms" << std::endl;
        }

        TEST_F(SkiplistUuidTest, SortedIteratorUuid)
        {
            hedge::skiplist<uuids::uuid>::config cfg;
            cfg.item_capacity = 100;
            hedge::skiplist<uuids::uuid> list(cfg);
            UuidComparator cmp;

            std::vector<uuids::uuid> inputs;
            for(int i = 0; i < 10; ++i)
                inputs.push_back(random_uuid());

            for(const auto& id : inputs)
                list.insert(id, cmp);

            // Expected order is sorted by operator<
            std::sort(inputs.begin(), inputs.end());

            auto reader = list.reader();
            std::vector<uuids::uuid> outputs;

            for(auto val : reader)
            {
                outputs.push_back(val);
            }

            ASSERT_EQ(outputs.size(), inputs.size());
            for(size_t i = 0; i < inputs.size(); ++i)
            {
                ASSERT_EQ(outputs[i], inputs[i]) << "Mismatch at index " << i;
            }
        }

        TEST_F(SkiplistUuidTest, ConcurrentAccessUuid)
        {
            const size_t N = 50'000;
            const int NUM_READERS = 4;

            hedge::skiplist<uuids::uuid>::config cfg;
            cfg.item_capacity = N;
            hedge::skiplist<uuids::uuid> list(cfg);
            UuidComparator cmp;

            // Pre-generate data to avoid lock contention on RNG during test
            std::vector<uuids::uuid> keys;
            keys.reserve(N);
            for(size_t i = 0; i < N; ++i)
                keys.push_back(random_uuid());

            std::atomic<bool> writer_done{false};

            std::vector<std::thread> readers;
            readers.reserve(NUM_READERS);

            for(int i = 0; i < NUM_READERS; ++i)
            {
                readers.emplace_back([&, i]()
                                     {
                // Each reader cycles through a subset of keys looking for them
                // until the writer is done.
                size_t idx = i;
                while(!writer_done.load(std::memory_order::acquire))
                {
                    if(idx < keys.size()) {
                        [[maybe_unused]] auto res = list.find(keys[idx], cmp);
                        idx += NUM_READERS; 
                    } else {
                        idx = i; // Reset loop
                    }
                } });
            }

            auto start = std::chrono::high_resolution_clock::now();
            for(const auto& key : keys)
            {
                list.insert(key, cmp);
            }
            writer_done.store(true, std::memory_order::release);
            auto end = std::chrono::high_resolution_clock::now();

            std::cout << "Concurrent UUID Insert (" << N << ") took: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                      << " ms" << std::endl;

            for(auto& t : readers)
                t.join();

            ASSERT_EQ(list.size(), N);
        }

        TEST_F(SkiplistUuidTest, ProbabilityBenchmarks2M)
        {
            const size_t N = 200'000;
            std::vector<double> p_values = {0.25, 0.33, 0.5, 0.75};

            for(double p : p_values)
            {
                hedge::skiplist<uuids::uuid>::config cfg;
                cfg.item_capacity = N;
                cfg.p = p;
                cfg.level_cap = std::nullopt; // No max level cap

                std::cout << "\n---------------------------------------------------" << std::endl;
                std::cout << "Testing with P=" << p << ", N=" << N << ", No Level Cap" << std::endl;

                hedge::skiplist<uuids::uuid> list(cfg);
                UuidComparator cmp;

                std::vector<uuids::uuid> keys;
                keys.reserve(N);

                std::cout << "Generating " << N << " UUIDs..." << std::endl;
                for(size_t i = 0; i < N; ++i)
                {
                    keys.push_back(random_uuid());
                }

                auto start = std::chrono::high_resolution_clock::now();
                for(const auto& key : keys)
                {
                    list.insert(key, cmp);
                }
                auto end = std::chrono::high_resolution_clock::now();
                auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

                std::cout << "Insertion took: " << insert_ms << " ms" << std::endl;
                std::cout << "Insertion Throughput: " << (N * 1000.0 / insert_ms) << " ops/sec" << std::endl;
                std::cout << "Max Levels (Calculated): " << list.levels() << std::endl;

                // Validate size
                ASSERT_EQ(list.size(), N);

                // Sanity check: Find the middle inserted element
                auto probe = keys[N / 2];
                auto res = list.find(probe, cmp);
                ASSERT_TRUE(res.has_value());
                ASSERT_EQ(res.value(), probe);
            }
        }


    } // namespace test
} // namespace hedge