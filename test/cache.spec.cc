#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "async/io_executor.h"
#include "async/task.h"
#include "async/wait_group.h"
#include "cache.h"
#include "types.h"

namespace hedge::db
{

    class PageCacheTest : public ::testing::Test
    {
    protected:
        static void SetUpTestSuite()
        {
            async::executor_pool::init_static_pool(8, 64);
        }

        static void fill_page(uint8_t* base_ptr, size_t offset, uint64_t seed)
        {
            auto* ptr = reinterpret_cast<uint64_t*>(base_ptr + offset);
            size_t count = PAGE_SIZE_IN_BYTES / sizeof(uint64_t);
            for(size_t i = 0; i < count; ++i)
            {
                ptr[i] = seed + i;
            }
        }

        static bool verify_page(const uint8_t* base_ptr, size_t offset, uint64_t seed)
        {
            const auto* ptr = reinterpret_cast<const uint64_t*>(base_ptr + offset);
            size_t count = PAGE_SIZE_IN_BYTES / sizeof(uint64_t);
            for(size_t i = 0; i < count; ++i)
            {
                if(ptr[i] != seed + i)
                {
                    return false;
                }
            }
            return true;
        }

        // --- Introspection Helpers ---

        uint64_t get_flags_for_tag(page_cache& cache, const page_tag& tag)
        {
            std::shared_lock lk(cache._m);
            auto it = cache._lut.find(tag);
            if(it == cache._lut.end())
                return 0;
            return cache._frames[it.value()].flags.load();
        }

        uint64_t get_ref_count_for_tag(page_cache& cache, const page_tag& tag)
        {
            std::shared_lock lk(cache._m);
            auto it = cache._lut.find(tag);
            if(it == cache._lut.end())
                return 0;
            return cache._frames[it.value()].flags.load() & PAGE_FLAG_REFERENCE_COUNT_MASK;
        }

        void check_all_guards_released(page_cache& cache)
        {
            for(size_t i = 0; i < cache._frames.size(); ++i)
            {
                uint64_t flags = cache._frames[i].flags.load();
                uint64_t ref_count = flags & PAGE_FLAG_REFERENCE_COUNT_MASK;
                ASSERT_EQ(ref_count, 0)
                    << "Frame " << i << " is still referenced! RefCount: " << ref_count
                    << " Flags: " << std::hex << flags << std::dec;
            }
        }

        void check_still_referenced(page_cache& cache, size_t high)
        {
            for(size_t i = 0; i < high; ++i)
            {
                uint64_t flags = cache._frames[i].flags.load();
                bool used = (flags & PAGE_FLAG_RECENTLY_USED) != 0;
                ASSERT_EQ(used, true)
                    << "Frame " << i << " is not used! " << " Flags: " << std::hex << flags << std::dec;
            }
        }
    };

    TEST_F(PageCacheTest, BasicWriteAndRead)
    {
        size_t cache_size = 1024 * 1024;
        page_cache cache(cache_size);

        auto wg = async::wait_group::make_shared();
        wg->set(1);

        std::atomic<bool> error{false};

        auto task = [&]() -> async::task<void>
        {
            auto tag = to_page_tag(1, 0);
            uint64_t seed = 0xCAFEBABE;

            {
                page_cache::write_page_guard guard = cache.get_write_slot(tag);
                fill_page(guard.data, guard.idx, seed);
            }

            {
                auto maybe_awaitable = cache.lookup(tag);
                if(!maybe_awaitable.has_value())
                {
                    error = true;
                    wg->decr();
                    co_return;
                }

                page_cache::read_page_guard guard = co_await maybe_awaitable.value();
                if(!verify_page(guard.data, guard.idx, seed))
                {
                    error = true;
                }
            }

            wg->decr();
        };

        async::executor_pool::executor_from_static_pool()->submit_io_task(task());
        wg->wait();

        ASSERT_FALSE(error);
        check_all_guards_released(cache);
        check_still_referenced(cache, 1);
    }

    TEST_F(PageCacheTest, GuardAssignmentSemantics)
    {
        size_t cache_size = 1024 * 1024;
        page_cache cache(cache_size);

        auto wg = async::wait_group::make_shared();
        wg->set(1);

        std::atomic<uint64_t> tag1_flags_after_overwrite{0};
        std::atomic<uint64_t> tag1_ref_before_overwrite{0};
        std::atomic<uint64_t> tag1_ref_after_overwrite{0};
        std::atomic<bool> lookup_success{false};

        auto task = [&]() -> async::task<void>
        {
            auto tag1 = to_page_tag(1, 0);
            auto tag2 = to_page_tag(1, PAGE_SIZE_IN_BYTES);

            // 1. Test write_page_guard operator=
            {
                page_cache::write_page_guard g1 = cache.get_write_slot(tag1);
                page_cache::write_page_guard g2 = cache.get_write_slot(tag2);

                // Overwriting g1 with g2 should trigger g1's completion (READY=1, ref-1)
                g1 = std::move(g2);

                tag1_flags_after_overwrite = get_flags_for_tag(cache, tag1);
            }

            // 2. Test read_page_guard operator=
            {
                // Prepare pages
                {
                    auto w1 = cache.get_write_slot(tag1);
                    auto w2 = cache.get_write_slot(tag2);
                }

                auto a1 = cache.lookup(tag1);
                auto a2 = cache.lookup(tag2);

                if(a1 && a2)
                {
                    lookup_success = true;
                    page_cache::read_page_guard r1 = co_await a1.value();
                    page_cache::read_page_guard r2 = co_await a2.value();

                    tag1_ref_before_overwrite = get_ref_count_for_tag(cache, tag1);

                    // Overwriting r1 with r2 should release ref on tag1
                    r1 = std::move(r2);

                    tag1_ref_after_overwrite = get_ref_count_for_tag(cache, tag1);
                }
            }

            wg->decr();
        };

        async::executor_pool::executor_from_static_pool()->submit_io_task(task());
        wg->wait();

        EXPECT_NE(tag1_flags_after_overwrite.load() & PAGE_FLAG_READY, 0) << "Writer overwrite failed to publish READY bit";

        ASSERT_TRUE(lookup_success.load());
        EXPECT_EQ(tag1_ref_before_overwrite.load(), 1);
        EXPECT_EQ(tag1_ref_after_overwrite.load(), 0) << "Reader overwrite failed to release reference";

        check_all_guards_released(cache);
    }

    TEST_F(PageCacheTest, MultiPageReader)
    {
        page_cache cache(1024 * 1024);
        auto wg = async::wait_group::make_shared();
        wg->set(1);

        std::atomic<bool> error{false};

        auto task = [&]() -> async::task<void>
        {
            std::vector<page_cache::read_page_guard> guards;
            int num_pages = 10;

            for(int i = 0; i < num_pages; ++i)
            {
                auto tag = to_page_tag(10, i * PAGE_SIZE_IN_BYTES);
                page_cache::write_page_guard g = cache.get_write_slot(tag);
                fill_page(g.data, g.idx, i * 100);
            }

            for(int i = 0; i < num_pages; ++i)
            {
                auto tag = to_page_tag(10, i * PAGE_SIZE_IN_BYTES);
                auto lookup = cache.lookup(tag);
                if(!lookup.has_value())
                {
                    error = true;
                    break;
                }
                guards.push_back(co_await lookup.value());
            }

            if(!error)
            {
                for(int i = 0; i < num_pages; ++i)
                {
                    if(!verify_page(guards[i].data, guards[i].idx, i * 100))
                        error = true;
                }
            }

            wg->decr();
        };

        async::executor_pool::executor_from_static_pool()->submit_io_task(task());
        wg->wait();

        ASSERT_FALSE(error);
        check_all_guards_released(cache);
        check_still_referenced(cache, 10);
    }

    TEST_F(PageCacheTest, LargeScaleStressTest)
    {
        constexpr size_t CACHE_SIZE_BYTES = 3 * 1024UL * 1024 * 1024;
        page_cache cache(CACHE_SIZE_BYTES);

        constexpr size_t WORKING_SET_PAGES = (CACHE_SIZE_BYTES / PAGE_SIZE_IN_BYTES) * 10.0;
        constexpr size_t TOTAL_OPS = 10'000'000;
        constexpr size_t BATCH_SIZE = 32;
        constexpr size_t NUM_TASKS = TOTAL_OPS / BATCH_SIZE;
        constexpr uint32_t FD = 42;

        auto wg = async::wait_group::make_shared();
        wg->set(NUM_TASKS);

        std::atomic<size_t> read_hits{0};
        std::atomic<size_t> read_misses{0};
        std::atomic<size_t> write_ops{0};
        std::atomic<size_t> fatal_errors{0};

        auto make_batch_task = [&](int task_id) -> async::task<void>
        {
            if(fatal_errors.load() > 0)
            {
                wg->decr();
                co_return;
            }

            std::mt19937 rng(task_id + 1337);
            std::uniform_int_distribution<uint32_t> page_dist(0, WORKING_SET_PAGES - 1);
            std::uniform_int_distribution<int> type_dist(0, 100);

            for(size_t i = 0; i < BATCH_SIZE; ++i)
            {
                if(fatal_errors.load() > 0)
                    break;

                uint32_t p_idx = page_dist(rng);
                auto tag = to_page_tag(FD, p_idx * PAGE_SIZE_IN_BYTES);
                uint64_t expected_seed = (static_cast<uint64_t>(FD) << 32) | p_idx;

                if(type_dist(rng) < 50)
                {
                    page_cache::write_page_guard guard = cache.get_write_slot(tag);
                    fill_page(guard.data, guard.idx, expected_seed);
                    write_ops++;
                }
                else
                {
                    auto maybe_awaitable = cache.lookup(tag);
                    if(!maybe_awaitable)
                    {
                        read_misses++;
                    }
                    else
                    {
                        read_hits++;
                        page_cache::read_page_guard guard = co_await maybe_awaitable.value();
                        if(!verify_page(guard.data, guard.idx, expected_seed))
                            fatal_errors++;
                    }
                }
            }
            wg->decr();
        };

        std::cout << "Starting Stress Test..." << std::endl;
        auto t0 = std::chrono::high_resolution_clock::now();

        for(size_t i = 0; i < NUM_TASKS; ++i)
        {
            const auto& executor = async::executor_pool::executor_from_static_pool();
            executor->submit_io_task(make_batch_task(i));
        }

        wg->wait();
        auto t1 = std::chrono::high_resolution_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        std::cout << "Stress test finished in " << ms << "ms." << std::endl;
        std::cout << "Ops/s : " << (TOTAL_OPS * 1000) / ms << std::endl;
        std::cout << "Avg latency per op: " << (ms * 1'000'000) / TOTAL_OPS << " ns" << std::endl;
        std::cout << "Read Hits: " << read_hits.load() << ", Read Misses: " << read_misses.load()
                  << ", Write Ops: " << write_ops.load() << std::endl;

        if(fatal_errors > 0)
            FAIL() << "Stress test failed with data corruption.";

        check_all_guards_released(cache);
    }

} // namespace hedge::db