// Version history test: verifies LSM-tree returns latest version after interleaved updates
// Creates heterogeneous version history (some keys updated 0 times, others 3+ times)

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
            cfg.index_point_cache_size_bytes = 0;
            cfg.num_background_workers = 2;
            cfg.max_pending_flushes = 4; // Allow concurrent flushes (epoch ordering enforced by sst_manager)
            cfg.min_merge_width = 2;
            cfg.max_merge_width = 8;
            cfg.disable_wal = true;
            return cfg;
        }

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

        auto put_op = [](size_t idx, tmc::semaphore& sem, database& db,
                         const value_set& values, std::atomic_size_t& errors) -> tmc::task<void>
        {
            auto status = co_await db.put_async(make_key(idx), values[value_slot(idx)]);
            if(!status)
                errors.fetch_add(1, std::memory_order_relaxed);
            sem.release();
        };

        struct read_stats
        {
            std::atomic_size_t not_found{0};
            std::atomic_size_t mismatches{0};
            std::atomic_size_t old_version{0};
        };

    } // namespace

    struct version_history_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
    {
        static void SetUpTestSuite()
        {
            io::static_pool::instance()->init(io::executor_config{
                .name = "version-history-pool",
                .queue_depth = QUEUE_DEPTH,
                .n_threads = NUM_WORKERS,
                .auto_detect = false,
            });
        }

        static void TearDownTestSuite()
        {
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

        std::filesystem::path _base_path = "/tmp/db_version_test";
        std::shared_ptr<database> _db;
    };

    TEST_P(version_history_test, interleaved_updates)
    {
        const size_t num_update_rounds = 3;
        const double update_probability = 0.6;

        auto v_initial = pregenerate_values(0x1000, PAYLOAD_SIZE);
        std::vector<value_set> v_updates;
        for(size_t round = 0; round < num_update_rounds; ++round)
            v_updates.push_back(pregenerate_values(0x2000 + round, PAYLOAD_SIZE));

        std::vector<size_t> expected_slot(N_KEYS, 0);

        {
            SCOPED_TRACE("initial_write");
            std::atomic_size_t errors{0};
            {
                phase p("initial_write: writes");
                run_parallel(N_KEYS, put_op, *_db, v_initial, errors);
            }
            {
                phase p("initial_write: wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }
            EXPECT_EQ(errors.load(), 0UL);
        }

        std::mt19937 rng(42);
        std::uniform_real_distribution<double> update_dist(0.0, 1.0);

        for(size_t round = 0; round < num_update_rounds; ++round)
        {
            SCOPED_TRACE("update_round_" + std::to_string(round));

            std::vector<size_t> keys_to_update;
            for(size_t i = 0; i < N_KEYS; ++i)
            {
                if(update_dist(rng) < update_probability)
                {
                    keys_to_update.push_back(i);
                    expected_slot[i] = round + 1;
                }
            }

            std::atomic_size_t errors{0};
            {
                phase p("round_" + std::to_string(round) + ": updates (" +
                        std::to_string(keys_to_update.size()) + " keys)");

                auto update_op = [&](size_t idx, tmc::semaphore& sem, database& db,
                                     const value_set& values, std::atomic_size_t& errs,
                                     const std::vector<size_t>& key_indices) -> tmc::task<void>
                {
                    if(idx < key_indices.size())
                    {
                        auto key_idx = key_indices[idx];
                        auto status = co_await db.put_async(make_key(key_idx), values[value_slot(key_idx)]);
                        if(!status)
                            errs.fetch_add(1, std::memory_order_relaxed);
                    }
                    sem.release();
                };

                run_parallel(keys_to_update.size(), update_op, *_db, v_updates[round], errors, keys_to_update);
            }
            {
                phase p("round_" + std::to_string(round) + ": wait_for_compactions");
                _db->wait_for_compactions_to_finish();
            }
            EXPECT_EQ(errors.load(), 0UL);
        }

        {
            SCOPED_TRACE("version_verification");

            read_stats stats;

            auto verify_op = [&](size_t idx, tmc::semaphore& sem, database& db,
                                 const value_set& v_init, const std::vector<value_set>& v_upd,
                                 read_stats& s) -> tmc::task<void>
            {
                auto result = co_await db.get_async(make_key(idx));
                if(!result)
                {
                    s.not_found.fetch_add(1, std::memory_order_relaxed);
                    sem.release();
                    co_return;
                }

                const size_t expected = expected_slot[idx];
                const byte_vec& expected_value = (expected == 0) ? v_init[value_slot(idx)] : v_upd[expected - 1][value_slot(idx)];

                if(result.value() != expected_value)
                {
                    bool is_old_version = false;
                    for(size_t v = 0; v < expected; ++v)
                    {
                        const byte_vec& old_val = (v == 0) ? v_init[value_slot(idx)] : v_upd[v - 1][value_slot(idx)];
                        if(result.value() == old_val)
                        {
                            is_old_version = true;
                            break;
                        }
                    }

                    if(is_old_version)
                        s.old_version.fetch_add(1, std::memory_order_relaxed);
                    else
                        s.mismatches.fetch_add(1, std::memory_order_relaxed);
                }
                sem.release();
            };

            {
                phase p("verify: reading all versions");
                run_parallel(N_KEYS, verify_op, *_db, v_initial, v_updates, stats);
            }

            EXPECT_EQ(stats.not_found.load(), 0UL) << "Keys not found";
            EXPECT_EQ(stats.old_version.load(), 0UL) << "Keys returned old version";
            EXPECT_EQ(stats.mismatches.load(), 0UL) << "Keys returned wrong value";
        }

        size_t single_version = 0;
        size_t multi_version = 0;
        for(size_t i = 0; i < N_KEYS; ++i)
        {
            if(expected_slot[i] == 0)
                ++single_version;
            else
                ++multi_version;
        }
        std::cerr << "  [version_info] Keys with 1 version: " << single_version
                  << ", Keys with 2+ versions: " << multi_version
                  << " (" << (100.0 * multi_version / N_KEYS) << "%)" << std::endl;

        this->_db->wait_for_compactions_to_finish();
    }

    INSTANTIATE_TEST_SUITE_P(
        default_params,
        version_history_test,
        testing::Combine(
            testing::Values(size_t{200'000}), // N_KEYS (smaller than e2e test)
            testing::Values(size_t{4}),       // NUM_PARTITION_EXPONENT
            testing::Values(size_t{100})      // PAYLOAD_SIZE
            ),
        [](const testing::TestParamInfo<version_history_test::ParamType>& info)
        {
            return "N" + std::to_string(std::get<0>(info.param)) +
                   "_P" + std::to_string(std::get<1>(info.param)) +
                   "_V" + std::to_string(std::get<2>(info.param));
        });

} // namespace hedge::db
