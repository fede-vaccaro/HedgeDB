// WAL replay test: write keys via memtable, simulate crash (destroy without flush),
// recreate memtable, replay WAL, verify all keys are recovered.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <vector>

#include <gtest/gtest.h>

#include "db/memtable.h"
#include "error.hpp"
#include "io/io_executor.h"
#include "key.h"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"
#include "types.h"
#include "xxh64.hpp"

namespace hedge::db
{
    static constexpr size_t KEY_SIZE = 24;
    static constexpr uint64_t SEED = 0xCAFEBABE;

    static key_t make_test_key(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED);
        auto k = key_t::make_with_length(KEY_SIZE);
        auto span = k.as_bytes();
        std::memset(span.data(), 0, KEY_SIZE);
        std::memcpy(span.data(), &h, std::min(sizeof(h), KEY_SIZE));
        return k;
    }

    static std::vector<std::byte> make_test_value(size_t i, size_t payload_size)
    {
        std::vector<std::byte> v(payload_size);
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED + 1);
        for(size_t j = 0; j < payload_size; ++j)
            v[j] = static_cast<std::byte>((h >> (j % 8 * 8)) ^ j);
        return v;
    }

    constexpr auto N_THREADS = 1;
    constexpr auto QUEUE_DEPTH = 32;

    struct wal_replay_test : public ::testing::Test
    {
        inline static std::shared_ptr<hedge::io::io_executor> executor;

        static void SetUpTestSuite()
        {
            executor = std::make_shared<hedge::io::io_executor>(
                hedge::io::executor_config{
                    .name = "test-pool",
                    .queue_depth = QUEUE_DEPTH,
                    .n_threads = N_THREADS,
                    .auto_detect = false,
                });
        }

        void SetUp() override
        {
            if(std::filesystem::exists(_indices_path))
                std::filesystem::remove_all(_indices_path);
            std::filesystem::create_directories(_indices_path);

            sync();
        }

        std::filesystem::path _indices_path = "/tmp/db_wal_test/indices";
    };

    TEST_F(wal_replay_test, replay_recovers_unflushed_keys)
    {
        constexpr size_t N_KEYS = 10'000;
        constexpr size_t PAYLOAD_SIZE = 64;

        memtable_config cfg;
        cfg.memory_budget_cap = 256 * 1024 * 1024;
        cfg.auto_compaction = false;
        cfg.use_odirect = false;
        cfg.num_writer_threads = N_THREADS;
        cfg.use_wal = true;

        std::atomic_size_t flush_epoch{0};
        uint64_t seq_nr_before_flush;

        // Phase 1: Create memtable, write keys, drop without flushing
        {
            memtable mt(
                cfg,
                4,
                _indices_path,
                &flush_epoch,
                executor,
                [](std::vector<sst> /*new_indices*/) -> tmc::task<void>
                { co_return; },
                []() {},
                nullptr);

            auto make_put_task = [](memtable* mt) -> tmc::task<void>
            {
                auto put_task = [](memtable* mt, size_t i, tmc::semaphore& s) -> tmc::task<void>
                {
                    auto key = make_test_key(i);
                    auto value = make_test_value(i, PAYLOAD_SIZE);
                    auto status = co_await mt->put_async(key, value, hedge::value_type::IN_PLACE_VALUE);
                    EXPECT_TRUE(status) << status.error().to_string();
                    s.release();
                };

                auto semaphore = tmc::semaphore(64);
                auto fg = tmc::fork_group();

                for(size_t i = 0; i < N_KEYS; ++i)
                {
                    co_await semaphore;
                    fg.fork(put_task(mt, i, semaphore));
                }
                co_await std::move(fg);
            };

            auto f = tmc::post_waitable(*wal_replay_test::executor, make_put_task(&mt));
            f.wait();

            // Verify WAL files exist
            size_t wal_count = 0;
            for(const auto& entry : std::filesystem::directory_iterator(_indices_path))
            {
                if(entry.path().filename().string().starts_with(hedge::db::wal::WAL_FILE_PREFIX))
                    ++wal_count;
            }
            ASSERT_GT(wal_count, 0) << "No WAL files found after writes";
            std::cout << "WAL files after write: " << wal_count << std::endl;

            seq_nr_before_flush = mt.acquire_snapshot().seq_nr;
            // Drop memtable without flushing — simulates crash
        }

        // Phase 2: Create new memtable and replay WAL
        {
            memtable mt(
                cfg,
                4,
                _indices_path,
                &flush_epoch,
                executor,
                [](std::vector<sst> /*new_indices*/) -> tmc::task<void>
                { co_return; },
                []() {},
                nullptr);

            auto wal_status = mt.replay_wal();
            ASSERT_TRUE(wal_status) << wal_status.error().to_string();

            // Verify all keys are recovered
            size_t not_found = 0;
            size_t mismatches = 0;

            for(size_t i = 0; i < N_KEYS; ++i)
            {
                auto key = make_test_key(i);
                auto expected = make_test_value(i, PAYLOAD_SIZE);

                auto maybe_value = mt.get(key);
                if(!maybe_value.has_value())
                {
                    ++not_found;
                    continue;
                }

                auto& val = maybe_value.value();
                if(!std::holds_alternative<std::vector<std::byte>>(val))
                {
                    ++mismatches;
                    continue;
                }

                auto& recovered = std::get<std::vector<std::byte>>(val);
                if(recovered != expected)
                    ++mismatches;
            }

            std::cout << "Not found: " << not_found << " / " << N_KEYS << std::endl;
            std::cout << "Value mismatches: " << mismatches << std::endl;

            EXPECT_EQ(not_found, 0) << "Some keys were not recovered from WAL";
            EXPECT_EQ(mismatches, 0) << "Some values did not match after WAL replay";

            uint64_t seq_nr_after_flush = mt.acquire_snapshot().seq_nr;
            EXPECT_EQ(seq_nr_before_flush, seq_nr_after_flush);
        }
    }

} // namespace hedge::db
