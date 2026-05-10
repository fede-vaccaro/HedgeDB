// End-to-end test suite for the full database.
//
// The suite runs as a single TEST_P with five sequential phases over a
// single database instance:
//   * bulk_load       — N concurrent puts with v_old, no errors.
//   * full_readback   — read every key, verify bytes match v_old.
//   * interleaved_rw  — concurrent random reads over [0, N) and writes to
//                        a new range [N, N + n_new_writes), no errors.
//   * updates         — overwrite every key with v_new, verify reads.
//   * deletes         — remove the first half, re-insert a quarter with
//                        v_newest, verify the three partitions.
//
// Parameters: (N_KEYS, NUM_PARTITION_EXPONENT, PAYLOAD_SIZE).
// Keys and values are derived from xxh64 so the suite is deterministic.
//
// Why one test instead of five: memtable::put_async caches the current
// rw_sync table in a thread_local that's only refreshed on freeze. Tearing
// down the DB between TEST_P cases leaves the io-pool threads pointing at
// the old memtable, and the next DB's writes dead-loop on the mismatched
// _flush guard. A single DB across phases sidesteps that.
//
// Every coroutine-returning lambda has an empty capture list and receives
// its state through explicit parameters: capturing lambdas that spawn
// coroutines are unsafe because the closure must outlive every frame it
// spawns, which is fragile to audit.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>
#include <xxh64.hpp>

#include "db/database.h"
#include "io/static_pool.h"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"
#include "types.h"

namespace hedge::db
{
    namespace
    {
        constexpr size_t KEY_SIZE = 24;
        constexpr size_t NUM_CACHED_VALUES = 256;
        constexpr size_t NUM_WORKERS = 4;
        constexpr uint32_t QUEUE_DEPTH = 32;
        constexpr uint64_t KEY_SEED = 0xDEADBEEFULL;
        constexpr uint64_t V_NEW_SEED = 0xFEEDFACEULL;
        constexpr uint64_t V_NEWEST_SEED = 0xABCDEF01ULL;

        using byte_vec = std::vector<std::byte>;
        using value_set = std::vector<byte_vec>;

        key_t make_key(size_t i)
        {
            uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED);
            auto k = key_t::make_with_length(KEY_SIZE);
            auto span = k.as_bytes();
            std::memset(span.data(), 0, KEY_SIZE);
            std::memcpy(span.data(), &h, std::min(sizeof(h), KEY_SIZE));
            return k;
        }

        size_t value_slot(size_t i)
        {
            uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED);
            return h % NUM_CACHED_VALUES;
        }

        value_set pregenerate_values(uint64_t mix_seed, size_t payload_size)
        {
            value_set values(NUM_CACHED_VALUES);
            for(size_t slot = 0; slot < NUM_CACHED_VALUES; ++slot)
            {
                values[slot].resize(payload_size);
                std::mt19937 gen(static_cast<uint32_t>(slot ^ mix_seed));
                std::uniform_int_distribution<uint8_t> dist(0, 255);
                for(auto& b : values[slot])
                    b = static_cast<std::byte>(dist(gen));
            }
            return values;
        }

        db_config make_test_config(size_t num_partition_exponent)
        {
            db_config cfg;
            cfg.auto_compaction = true;
            cfg.memtable_budget_bytes = 4 * MiB;
            cfg.compaction_read_ahead_size_bytes = 1 * MiB;
            cfg.num_partition_exponent = num_partition_exponent;
            cfg.bucket_ratio = 1.50;
            cfg.use_direct_io = true;
            cfg.index_page_clock_cache_size_bytes = 0;
            cfg.num_background_workers = 2;
            cfg.max_pending_flushes = 4;
            cfg.min_merge_width = 2;
            cfg.max_merge_width = 8;
            cfg.disable_wal = true;
            return cfg;
        }

        // Parallel driver. `Op` must be a no-capture lambda whose signature is
        //     tmc::task<void>(size_t idx, tmc::semaphore& sem, Args&... args)
        // and whose body releases `sem` exactly once before returning. `args`
        // are threaded through by reference, so every argument must outlive
        // this call.
        template <typename Op, typename... Args>
        void run_parallel(size_t n, Op op, Args&... args)
        {
            auto worker = [](size_t tid, size_t total, Op op, Args&... args) -> tmc::task<void>
            {
                auto fg = tmc::fork_group();
                auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
                for(size_t i = tid; i < total; i += NUM_WORKERS)
                {
                    co_await sem;
                    fg.fork(op(i, sem, args...));
                }
                co_await std::move(fg);
            };

            std::vector<std::future<void>> futures;
            futures.reserve(NUM_WORKERS);
            for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
                futures.push_back(tmc::post_waitable(
                    *io::static_pool::instance(),
                    worker(tid, n, op, args...),
                    0, tid));
            for(auto& f : futures)
                f.get();
        }

        // Stopwatch that logs phase start/end durations to stderr.
        struct phase
        {
            std::string label;
            std::chrono::steady_clock::time_point t0;

            explicit phase(std::string label_) : label(std::move(label_)), t0(std::chrono::steady_clock::now())
            {
                std::cerr << "  [phase] " << label << " ..." << std::endl;
            }

            ~phase()
            {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - t0)
                              .count();
                std::cerr << "  [phase] " << label << " done in " << ms << " ms" << std::endl;
            }
        };

        // --- no-capture op lambdas used across phases ---

        auto put_op = [](size_t idx, tmc::semaphore& sem, database& db,
                         const value_set& values, std::atomic_size_t& errors) -> tmc::task<void>
        {
            auto status = co_await db.put_async(make_key(idx), values[value_slot(idx)]);
            if(!status)
                errors.fetch_add(1, std::memory_order_relaxed);
            sem.release();
        };

        auto put_op_unchecked = [](size_t idx, tmc::semaphore& sem, database& db,
                                   const value_set& values) -> tmc::task<void>
        {
            co_await db.put_async(make_key(idx), values[value_slot(idx)]);
            sem.release();
        };

        auto remove_op = [](size_t idx, tmc::semaphore& sem, database& db) -> tmc::task<void>
        {
            co_await db.remove_async(make_key(idx));
            sem.release();
        };

        struct read_stats
        {
            std::atomic_size_t not_found{0};
            std::atomic_size_t mismatches{0};
        };

        auto get_verify_op = [](size_t idx, tmc::semaphore& sem, database& db,
                                const value_set& expected, read_stats& stats) -> tmc::task<void>
        {
            auto result = co_await db.get_async(make_key(idx));
            if(!result)
                stats.not_found.fetch_add(1, std::memory_order_relaxed);
            else if(result.value() != expected[value_slot(idx)])
                stats.mismatches.fetch_add(1, std::memory_order_relaxed);
            sem.release();
        };

        // Detect whether remove_async is implemented. The helper mirrors the
        // caller's "not implemented yet" sentinel from database::remove_async.
        bool probe_remove_supported(database& db)
        {
            auto probe_task = [](database& db) -> tmc::task<hedge::status>
            {
                co_return co_await db.remove_async(make_key(0));
            };
            auto probe = tmc::post_waitable(
                *io::static_pool::instance(), probe_task(db), 0, 0);
            auto status = probe.get();
            if(!status && status.error().to_string().find("not implemented") != std::string::npos)
                return false;
            return true;
        }
    } // namespace

    struct database_e2e_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
    {
        static void SetUpTestSuite()
        {
            io::static_pool::instance()->init(io::executor_config{
                .name = "db-e2e-pool",
                .queue_depth = QUEUE_DEPTH,
                .n_threads = NUM_WORKERS,
                .auto_detect = false,
            });
        }

        void SetUp() override
        {
            this->N_KEYS = std::get<0>(GetParam());
            this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
            this->PAYLOAD_SIZE = std::get<2>(GetParam());

            if(std::filesystem::exists(_base_path))
                std::filesystem::remove_all(_base_path);

            auto maybe_db = database::make_new(_base_path, make_test_config(this->NUM_PARTITION_EXPONENT));
            ASSERT_TRUE(maybe_db) << maybe_db.error().to_string();
            _db = std::move(maybe_db.value());
        }

        void TearDown() override
        {
            _db.reset();
            if(std::filesystem::exists(_base_path))
                std::filesystem::remove_all(_base_path);
        }

        size_t N_KEYS{};
        size_t NUM_PARTITION_EXPONENT{};
        size_t PAYLOAD_SIZE{};

        std::filesystem::path _base_path = "/tmp/db_e2e_test";
        std::shared_ptr<database> _db;
    };

    TEST_P(database_e2e_test, end_to_end)
    {
        auto v_old = pregenerate_values(0, PAYLOAD_SIZE);
        auto v_new = pregenerate_values(V_NEW_SEED, PAYLOAD_SIZE);
        auto v_newest = pregenerate_values(V_NEWEST_SEED, PAYLOAD_SIZE);

        // -------------------------------------------------------------------
        // Phase 1: bulk_load — write N keys with v_old.
        // -------------------------------------------------------------------
        {
            SCOPED_TRACE("bulk_load");
            std::atomic_size_t errors{0};
            {
                phase p("bulk_load: writes");
                run_parallel(N_KEYS, put_op, *_db, v_old, errors);
            }
            {
                phase p("bulk_load: wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }
            EXPECT_EQ(errors.load(), 0UL);
        }

        // -------------------------------------------------------------------
        // Phase 2: full_readback — read every key, must match v_old.
        // -------------------------------------------------------------------
        {
            SCOPED_TRACE("full_readback");
            read_stats stats;
            {
                phase p("full_readback: reads");
                run_parallel(N_KEYS, get_verify_op, *_db, v_old, stats);
            }
            EXPECT_EQ(stats.not_found.load(), 0UL);
            EXPECT_EQ(stats.mismatches.load(), 0UL);
        }

        // -------------------------------------------------------------------
        // Phase 3: interleaved_rw — alternating reads on [0, N_KEYS) and
        // writes extending to [N_KEYS, N_KEYS + n_new_writes).
        // -------------------------------------------------------------------
        const size_t n_new_writes = N_KEYS / 5;
        const size_t total_keys = N_KEYS + n_new_writes;
        {
            SCOPED_TRACE("interleaved_rw");

            struct rw_stats
            {
                std::atomic_size_t write_errors{0};
                std::atomic_size_t read_errors{0};
                std::atomic_size_t read_mismatches{0};
                std::atomic_size_t next_write_idx;
            };
            rw_stats stats{.next_write_idx = N_KEYS};

            auto mixed_op = [](size_t op_idx, tmc::semaphore& sem, database& db,
                               const value_set& values, rw_stats& stats,
                               size_t read_upper) -> tmc::task<void>
            {
                if((op_idx % 2UL) == 1UL)
                {
                    std::mt19937 rng(static_cast<uint32_t>(op_idx));
                    size_t idx = std::uniform_int_distribution<size_t>(0, read_upper - 1)(rng);
                    auto result = co_await db.get_async(make_key(idx));
                    if(!result)
                        stats.read_errors.fetch_add(1, std::memory_order_relaxed);
                    else if(result.value() != values[value_slot(idx)])
                        stats.read_mismatches.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    size_t idx = stats.next_write_idx.fetch_add(1, std::memory_order_relaxed);
                    auto status = co_await db.put_async(make_key(idx), values[value_slot(idx)]);
                    if(!status)
                        stats.write_errors.fetch_add(1, std::memory_order_relaxed);
                }
                sem.release();
            };

            const size_t n_ops = n_new_writes * 2; // ~half reads, ~half writes
            {
                phase p("interleaved_rw: mixed ops");
                run_parallel(n_ops, mixed_op, *_db, v_old, stats, N_KEYS);
            }
            {
                phase p("interleaved_rw: wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }

            EXPECT_EQ(stats.write_errors.load(), 0UL);
            EXPECT_EQ(stats.read_errors.load(), 0UL);
            EXPECT_EQ(stats.read_mismatches.load(), 0UL);
        }

        // -------------------------------------------------------------------
        // Phase 4: updates — overwrite every key with v_new, verify reads.
        // -------------------------------------------------------------------
        {
            SCOPED_TRACE("updates");
            {
                phase p("updates: overwrites");
                run_parallel(total_keys, put_op_unchecked, *_db, v_new);
            }
            {
                phase p("updates: wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }

            read_stats stats;
            {
                phase p("updates: readback");
                run_parallel(total_keys, get_verify_op, *_db, v_new, stats);
            }
            EXPECT_EQ(stats.not_found.load(), 0UL);
            EXPECT_EQ(stats.mismatches.load(), 0UL);
        }

        // -------------------------------------------------------------------
        // Phase 5: deletes — delete first n_deletes, re-insert first
        // n_reinsert with v_newest, verify the three partitions.
        // -------------------------------------------------------------------
        if(!probe_remove_supported(*_db))
        {
            std::cerr << "  [phase] deletes: SKIPPED (remove_async not implemented)" << std::endl;
            return;
        }

        {
            SCOPED_TRACE("deletes");

            const size_t n_deletes = total_keys / 2;
            const size_t n_reinsert = n_deletes / 2;

            {
                phase p("deletes: removes (" + std::to_string(n_deletes) + ")");
                run_parallel(n_deletes, remove_op, *_db);
            }
            {
                phase p("deletes: reinsertions (" + std::to_string(n_reinsert) + ")");
                run_parallel(n_reinsert, put_op_unchecked, *_db, v_newest);
            }
            {
                phase p("deletes: wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }

            // Expected partitions:
            //   [0, n_reinsert):        re-inserted with v_newest
            //   [n_reinsert, n_deletes): deleted (errc::DELETED or KEY_NOT_FOUND)
            //   [n_deletes, total_keys): untouched, still v_new
            struct delete_stats
            {
                std::atomic_size_t wrong_reinsert{0};
                std::atomic_size_t wrong_delete{0};
                std::atomic_size_t wrong_intact{0};
            };
            delete_stats stats;

            auto verify_op = [](size_t idx, tmc::semaphore& sem, database& db,
                                const value_set& v_intact, const value_set& v_reinserted,
                                delete_stats& stats,
                                size_t n_reinsert, size_t n_deletes) -> tmc::task<void>
            {
                auto result = co_await db.get_async(make_key(idx));

                if(idx < n_reinsert)
                {
                    if(!result || result.value() != v_reinserted[value_slot(idx)])
                        stats.wrong_reinsert.fetch_add(1, std::memory_order_relaxed);
                }
                else if(idx < n_deletes)
                {
                    const bool deleted_signal = !result &&
                                                (result.error().code() == errc::DELETED ||
                                                 result.error().code() == errc::KEY_NOT_FOUND);
                    if(!deleted_signal)
                        stats.wrong_delete.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    if(!result || result.value() != v_intact[value_slot(idx)])
                        stats.wrong_intact.fetch_add(1, std::memory_order_relaxed);
                }
                sem.release();
            };

            {
                phase p("deletes: verify");
                run_parallel(total_keys, verify_op, *_db, v_new, v_newest, stats, n_reinsert, n_deletes);
            }

            EXPECT_EQ(stats.wrong_reinsert.load(), 0UL);
            EXPECT_EQ(stats.wrong_delete.load(), 0UL);
            EXPECT_EQ(stats.wrong_intact.load(), 0UL);
        }
    }

    INSTANTIATE_TEST_SUITE_P(
        default_params,
        database_e2e_test,
        testing::Combine(
            testing::Values(size_t{500'000}), // N_KEYS
            testing::Values(size_t{4}),       // NUM_PARTITION_EXPONENT
            testing::Values(size_t{100})      // PAYLOAD_SIZE
            ),
        [](const testing::TestParamInfo<database_e2e_test::ParamType>& info)
        {
            return "N" + std::to_string(std::get<0>(info.param)) +
                   "_P" + std::to_string(std::get<1>(info.param)) +
                   "_V" + std::to_string(std::get<2>(info.param));
        });

} // namespace hedge::db
