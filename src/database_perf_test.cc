// Standalone benchmark: full database write + read path (no gtest).
//
// Uses xxh64 for deterministic key generation from record indices,
// enabling reproducible readback verification.
//
// Usage: ./database_perf_test [N_KEYS] [PAYLOAD_SIZE] [MEMTABLE_CAPACITY]

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>
#include <xxh64.hpp>

#include "db/database.h"
#include "db/memtable.h"
#include "io/static_pool.h"
#include "perf_counter.h"
#include "tmc/fork_group.hpp"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"

namespace hedge::db
{
    static constexpr size_t KEY_SIZE = 24;
    static constexpr uint64_t SEED_0 = 0xDEADBEEF;
    static constexpr size_t NUM_CACHED_VALUES = 1024;

    static key_t make_key(size_t i)
    {
        uint64_t h0 = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED_0);

        auto k = key_t::make_with_length(KEY_SIZE);
        auto span = k.as_bytes();
        std::memset(span.data(), 0, KEY_SIZE);

        std::memcpy(span.data(), &h0, std::min(sizeof(h0), KEY_SIZE));

        return k;
    }

    static size_t value_slot(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED_0);
        return h % NUM_CACHED_VALUES;
    }

    static std::unordered_map<size_t, std::vector<std::byte>> pregenerate_values(size_t payload_size)
    {
        std::unordered_map<size_t, std::vector<std::byte>> values;
        values.reserve(NUM_CACHED_VALUES);
        for(size_t slot = 0; slot < NUM_CACHED_VALUES; ++slot)
        {
            std::vector<std::byte> v(payload_size);
            std::mt19937 gen(static_cast<uint32_t>(slot));
            std::uniform_int_distribution<uint8_t> dist(0, 255);
            for(auto& b : v)
                b = static_cast<std::byte>(dist(gen));
            values[slot] = std::move(v);
        }
        return values;
    }

} // namespace hedge::db

int main(int argc, char* argv[])
{
    using namespace hedge;
    using namespace hedge::db;
    using clk = std::chrono::high_resolution_clock;

    // Init static threadpool
    constexpr size_t NUM_THREADS = 8;
    constexpr uint32_t QD = 64;

    auto& pool = hedge::io::static_pool::instance()->init(hedge::io::executor_config{
        .name = "io-pool",
        .queue_depth = QD,
        .n_threads = NUM_THREADS,
        .auto_detect = false,
    });

    // --- CLI args ---
    size_t N_KEYS = 300'000'000;
    size_t PAYLOAD_SIZE = 100;
    size_t MEMTABLE_CAPACITY_BYTES = 64 * MiB;
    bool readonly = false;
    bool compact_all = false;

    int positional = 0;
    for(int i = 1; i < argc; ++i)
    {
        if(std::string_view(argv[i]) == "--readonly")
        {
            readonly = true;
        }
        else if(std::string_view(argv[i]) == "--compact_all")
        {
            compact_all = true;
        }
        else if(positional == 0)
        {
            N_KEYS = std::strtoull(argv[i], nullptr, 10);
            ++positional;
        }
        else if(positional == 1)
        {
            PAYLOAD_SIZE = std::strtoull(argv[i], nullptr, 10);
            ++positional;
        }
        // else if(positional == 2)
        // {
        // MEMTABLE_CAPACITY_BYTES = std::strtoull(argv[i], nullptr, 10);
        // ++positional;
        // }
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "=== database_perf_test ===" << std::endl;
    std::cout << "N_KEYS=" << N_KEYS
              << "  PAYLOAD_SIZE=" << PAYLOAD_SIZE
              << "  MEMTABLE_CAPACITY_BYTES=" << MEMTABLE_CAPACITY_BYTES
              << "  readonly=" << (readonly ? "true" : "false")
              << "  compact_all=" << (compact_all ? "true" : "false")
              << std::endl;

    // --- Init ---
    const std::filesystem::path base_path = "/tmp/db_perf";

    db_config config;
    config.auto_compaction = true;
    config.compaction_read_ahead_size_bytes = 2 * MiB;
    config.memtable_budget_bytes = MEMTABLE_CAPACITY_BYTES;
    config.num_partition_exponent = 4;
    config.bucket_ratio = 1.50;
    config.use_direct_io = true;
    config.index_page_clock_cache_size_bytes = 0;
    config.num_background_workers = 6;
    config.max_pending_flushes = 8;
    config.min_merge_width = 4;
    config.max_merge_width = 16;
    config.disable_wal = false;

    std::shared_ptr<database> db;
    if(readonly)
    {
        auto maybe_db = database::load(base_path, config);
        if(!maybe_db)
        {
            std::cerr << "Failed to load database: " << maybe_db.error().to_string() << std::endl;
            return 1;
        }
        db = std::move(maybe_db.value());
    }
    else
    {
        if(std::filesystem::exists(base_path))
            std::filesystem::remove_all(base_path);

        auto maybe_db = database::make_new(base_path, config);
        if(!maybe_db)
        {
            std::cerr << "Failed to create database: " << maybe_db.error().to_string() << std::endl;
            return 1;
        }
        db = std::move(maybe_db.value());
    }

    auto values = pregenerate_values(PAYLOAD_SIZE);

    // =========================================================================
    // Write phase
    // =========================================================================
    if(!readonly)
    {

        auto make_put = [](size_t i, const auto& db, const auto& values, tmc::semaphore& s) -> tmc::task<void>
        {
            auto key = make_key(i);
            const auto& val = values.at(value_slot(i));
            auto status = co_await db->put_async(key, val);
            if(!status)
                std::cerr << "initial put error at i=" << i << ": " << status.error().to_string() << std::endl;
            s.release();
        };

        auto make_put_task = [](size_t tid, const auto& database, size_t initial_writes, const auto& values, auto make_put) -> tmc::task<void>
        {
            auto fg = tmc::fork_group();
            auto semaphore = tmc::semaphore(io::static_pool::instance()->queue_depth());

            for(size_t i = tid; i < initial_writes; i += NUM_THREADS)
            {
                co_await semaphore;
                fg.fork(make_put(i, database, values, semaphore));
            }

            co_await std::move(fg);
        };

        auto t0 = clk::now();

        std::vector<std::future<void>> futures;
        futures.reserve(NUM_THREADS);
        for(size_t tid = 0; tid < NUM_THREADS; ++tid)
            futures.push_back(tmc::post_waitable(pool, make_put_task(tid, db, N_KEYS, values, make_put), 0, tid));

        for(auto& f : futures)
            f.get();

        auto t0_finish_writing = clk::now();

        std::cout << "\n--- Write phase ---" << std::endl;
        auto elapsed_finish_writing_s = std::chrono::duration<double>(t0_finish_writing - t0).count();
        std::cout << "Duration: " << elapsed_finish_writing_s * 1000.0 << " ms" << std::endl;
        std::cout << "Write throughput: " << static_cast<uint64_t>(N_KEYS / elapsed_finish_writing_s) << " items/s" << std::endl;
        std::cout << "Bandwidth: " << (N_KEYS * (PAYLOAD_SIZE + KEY_SIZE) / 1e6) / elapsed_finish_writing_s << " MB/s" << std::endl;
        std::cout << "BACKPRESSURE: " << memtable::HALT_COUNTER << std::endl;
        std::cout << "Waiting for pending compactions..." << std::endl;
        db->wait_for_compactions_to_finish();

        auto t1 = clk::now();
        double elapsed_us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        std::cout << "\n--- Write phase (w/compaction) ---" << std::endl;
        double elapsed_s = elapsed_us / 1'000'000.0;
        std::cout << "Duration: " << elapsed_us / 1000.0 << " ms" << std::endl;
        std::cout << "Throughput: " << static_cast<uint64_t>(N_KEYS / elapsed_s) << " items/s" << std::endl;
        std::cout << "Bandwidth: " << (N_KEYS * (PAYLOAD_SIZE + KEY_SIZE) / 1e6) / elapsed_s << " MB/s" << std::endl;
        prof::print_internal_perf_stats(false);
    }

    if(compact_all)
    {
        db->trigger_compaction(true);
        db->wait_for_compactions_to_finish();
    }

    // =========================================================================
    // Read phase (cold + warm)
    // =========================================================================
    {
        std::cout << "\nWaiting before starting read phase..." << std::endl;
    }

    size_t read_count = std::min(N_KEYS, size_t{10'000'000});

    for(int pass = 0; pass < 2; ++pass)
    {
        std::cout << "\n--- Read phase " << (pass == 0 ? "(cold)" : "(warm)") << " ---" << std::endl;
        std::cout << "Reading " << read_count << " keys." << std::endl;

        std::atomic_size_t errors{0};

        auto make_get_task = [&](size_t tid) -> tmc::task<void>
        {
            auto fg = tmc::fork_group();
            auto semaphore = tmc::semaphore(io::static_pool::instance()->queue_depth());

            auto make_task = [](size_t idx, const auto& db, tmc::semaphore& s, bool readonly, auto& errors, const auto& values) -> tmc::task<void>
            {
                auto key = make_key(idx);

                auto maybe_value = co_await db->get_async(key);
                if(!maybe_value)
                {
                    std::cout << "get error at idx=" << idx << ": " << maybe_value.error().to_string() << std::endl;
                    errors++;
                    s.release();
                    co_return;
                }

                if(!readonly && maybe_value.value() != values.at(value_slot(idx)))
                    errors++;

                s.release();
            };

            for(size_t idx = tid; idx < read_count; idx += NUM_THREADS)
            {
                co_await semaphore;
                fg.fork(make_task(idx, db, semaphore, readonly, errors, values));
            }

            co_await std::move(fg);
        };

        auto t0 = clk::now();

        std::vector<std::future<void>> futures;
        futures.reserve(NUM_THREADS);
        for(size_t tid = 0; tid < NUM_THREADS; ++tid)
            futures.push_back(tmc::post_waitable(pool, make_get_task(tid), 0, tid));

        for(auto& f : futures)
            f.get();

        auto t1 = clk::now();

        double elapsed_us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        double elapsed_s = elapsed_us / 1'000'000.0;

        std::cout << "Duration: " << elapsed_us / 1000.0 << " ms" << std::endl;
        std::cout << "Throughput: " << static_cast<uint64_t>(read_count / elapsed_s) << " items/s" << std::endl;
        std::cout << "Bandwidth: " << (read_count * PAYLOAD_SIZE / 1e6) / elapsed_s << " MB/s" << std::endl;
        std::cout << "Errors: " << errors.load() << std::endl;
        prof::print_internal_perf_stats(true);

        if(!readonly && errors.load() > 0)
        {
            std::cerr << "Read verification failed." << std::endl;
            return 1;
        }
    }

    std::cout << "\n=== PASSED ===" << std::endl;

    db->print_tree_structure();
    return 0;
}
