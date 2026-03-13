// Standalone benchmark: full database write + read path (no gtest).
//
// Uses xxh64 for deterministic key generation from record indices,
// enabling reproducible readback verification.
//
// Usage: ./database_perf_test [N_KEYS] [PAYLOAD_SIZE] [MEMTABLE_CAPACITY]

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>

#include <xxh64.hpp>

#include "async/io_executor.h"
#include "async/task.h"
#include "async/wait_group.h"
#include "db/database.h"
#include "perf_counter.h"
#include "types.h"

namespace hedge::db
{
    static constexpr size_t KEY_SIZE = 24;
    static constexpr uint64_t SEED = 0xDEADBEEF;
    static constexpr size_t NUM_CACHED_VALUES = 1024;

    static key_t make_key(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED);
        auto k = key_t::make_with_length(KEY_SIZE);
        auto span = k.as_bytes();
        std::memset(span.data(), 0, KEY_SIZE);
        std::memcpy(span.data(), &h, std::min(sizeof(h), KEY_SIZE));
        return k;
    }

    static size_t value_slot(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), SEED);
        return h % NUM_CACHED_VALUES;
    }

    static std::unordered_map<size_t, std::vector<uint8_t>> pregenerate_values(size_t payload_size)
    {
        std::unordered_map<size_t, std::vector<uint8_t>> values;
        values.reserve(NUM_CACHED_VALUES);
        for(size_t slot = 0; slot < NUM_CACHED_VALUES; ++slot)
        {
            std::vector<uint8_t> v(payload_size);
            std::mt19937 gen(static_cast<uint32_t>(slot));
            std::uniform_int_distribution<uint8_t> dist(0, 255);
            for(auto& b : v)
                b = dist(gen);
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

    // --- CLI args ---
    size_t N_KEYS = 300'000'000;
    size_t PAYLOAD_SIZE = 100;
    size_t MEMTABLE_CAPACITY = 2'000'000;
    bool readonly = false;

    int positional = 0;
    for(int i = 1; i < argc; ++i)
    {
        if(std::string_view(argv[i]) == "--readonly")
            readonly = true;
        else if(positional == 0) { N_KEYS             = std::strtoull(argv[i], nullptr, 10); ++positional; }
        else if(positional == 1) { PAYLOAD_SIZE        = std::strtoull(argv[i], nullptr, 10); ++positional; }
        else if(positional == 2) { MEMTABLE_CAPACITY   = std::strtoull(argv[i], nullptr, 10); ++positional; }
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "=== database_perf_test ===" << std::endl;
    std::cout << "N_KEYS=" << N_KEYS
              << "  PAYLOAD_SIZE=" << PAYLOAD_SIZE
              << "  MEMTABLE_CAPACITY=" << MEMTABLE_CAPACITY
              << "  readonly=" << (readonly ? "true" : "false") << std::endl;

    // --- Init ---
    async::executor_pool::init_static_pool(20, 16);

    const std::filesystem::path base_path = "/tmp/db_perf";

    db_config config;
    config.auto_compaction = true;
    config.keys_in_mem_before_flush = MEMTABLE_CAPACITY;
    config.num_partition_exponent = 4;
    config.bucket_ratio = 1.20;
    config.use_odirect_for_indices = true;
    config.index_page_clock_cache_size_bytes = 0;
    config.index_point_cache_size_bytes = 0;
    config.flush_io_workers = 6;
    config.compaction_io_workers = 6;

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

    constexpr size_t NUM_EXECUTORS = 20;
    std::vector<std::shared_ptr<async::executor_context>> executors;
    executors.reserve(NUM_EXECUTORS);
    for(size_t i = 0; i < NUM_EXECUTORS; ++i)
        executors.push_back(async::executor_pool::executor_from_static_pool());

    // =========================================================================
    // Write phase
    // =========================================================================
    if(!readonly)
    {
        auto wg = async::wait_group::make_shared();
        wg->set(N_KEYS);

        auto make_put_task = [&](size_t i) -> async::task<void>
        {
            auto key = make_key(i);
            const auto& value = values[value_slot(i)];
            auto status = co_await db->put_async(key, value);
            if(!status)
                std::cerr << "put error at i=" << i << ": " << status.error().to_string() << std::endl;
            wg->decr();
        };

        auto t0 = clk::now();

        for(size_t i = 0; i < N_KEYS; ++i)
        {
            constexpr size_t NUM_WRITERS = 8;
            executors[i % NUM_WRITERS]->submit_io_task(make_put_task(i));
        }

        wg->wait();
        std::cout << "All insertions completed. Waiting for pending compactions..." << std::endl;
        db->wait_for_compactions_to_finish();

        auto t1 = clk::now();
        double elapsed_us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        double elapsed_s = elapsed_us / 1'000'000.0;

        std::cout << "\n--- Write phase ---" << std::endl;
        std::cout << "Duration: " << elapsed_us / 1000.0 << " ms" << std::endl;
        std::cout << "Throughput: " << static_cast<uint64_t>(N_KEYS / elapsed_s) << " items/s" << std::endl;
        std::cout << "Bandwidth: " << (N_KEYS * (PAYLOAD_SIZE + KEY_SIZE) / 1e6) / elapsed_s << " MB/s" << std::endl;
        std::cout << "Backpressure count: " << memtable::BACKPRESSURE.load() << std::endl;
        prof::print_internal_perf_stats(false);
    }

    // =========================================================================
    // Read phase (cold + warm)
    // =========================================================================
    size_t read_count = std::min(N_KEYS, size_t{10'000'000});

    std::cout << "\nSyncing FDs..." << std::endl;
    sync();

    for(int pass = 0; pass < 2; ++pass)
    {
        std::cout << "\n--- Read phase " << (pass == 0 ? "(cold)" : "(warm)") << " ---" << std::endl;
        std::cout << "Reading " << read_count << " keys." << std::endl;

        auto wg = async::wait_group::make_shared();
        wg->set(read_count);
        std::atomic_size_t errors{0};

        auto make_get_task = [&](size_t idx) -> async::task<void>
        {
            auto key = make_key(idx);

            auto maybe_value = co_await db->get_async(key);
            if(!maybe_value)
            {
                errors++;
                wg->decr();
                co_return;
            }

            if(!readonly && maybe_value.value() != values.at(value_slot(idx)))
                errors++;

            wg->decr();
        };

        auto t0 = clk::now();

        for(size_t i = 0; i < read_count; ++i)
        {
            constexpr size_t NUM_READERS = NUM_EXECUTORS;
            executors[i % NUM_READERS]->submit_io_task(make_get_task(i));
        }

        wg->wait();
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
