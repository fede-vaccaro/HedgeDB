// Standalone benchmark: 50/50 mixed read+write workload.
//
// Pre-loads INITIAL_WRITES keys, then runs mixed ops.  Reads always sample
// from the pre-loaded range [0, INITIAL_WRITES) via a fast xorshift64 PRNG.
// Writes append new keys beyond that range.
//
// Usage: ./database_perf_test_mixed <N_LOADS> <N_OPS> [--compact-all] [--load] [--reload]

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>
#include <xxh64.hpp>

#include "async/io_executor.h"
#include "async/task.h"
#include "async/wait_group.h"
#include "db/database.h"
#include "perf_counter.h"

namespace hedge::db
{
    static constexpr size_t KEY_SIZE = 24;
    static constexpr uint64_t SEED = 0xDEADBEEF;
    static constexpr size_t NUM_CACHED_VALUES = 1024;
    static constexpr uint32_t BATCH_SIZE = 1;

    using kv_pair_t = std::pair<key_t, std::vector<uint8_t>>;

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

    static std::vector<std::vector<uint8_t>> pregenerate_values(size_t payload_size)
    {
        std::vector<std::vector<uint8_t>> values(NUM_CACHED_VALUES);
        for(size_t slot = 0; slot < NUM_CACHED_VALUES; ++slot)
        {
            values[slot].resize(payload_size);
            std::mt19937 gen(static_cast<uint32_t>(slot));
            std::uniform_int_distribution<uint8_t> dist(0, 255);
            for(auto& b : values[slot])
                b = dist(gen);
        }
        return values;
    }

    // Fast thread-local PRNG (xorshift64).  Each thread gets its own state
    // seeded from std::random_device so there is no contention.
    static uint64_t fast_rand(uint64_t& state)
    {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

} // namespace hedge::db

int main(int argc, char* argv[])
{
    using namespace hedge;
    using namespace hedge::db;
    using clk = std::chrono::high_resolution_clock;

    // --- CLI args ---
    if(argc < 3)
    {
        std::cerr << "Usage: " << argv[0] << " <N_LOADS> <N_OPS> [--compact-all] [--load] [--reload]" << std::endl;
        return 1;
    }

    const size_t INITIAL_WRITES = std::strtoull(argv[1], nullptr, 10);
    const size_t N_OPS = std::strtoull(argv[2], nullptr, 10);
    constexpr size_t PAYLOAD_SIZE = 100;
    constexpr size_t MEMTABLE_CAPACITY = 2'000'000;

    bool flag_compact_all = false;
    bool flag_load_only = false;
    bool flag_reload = false;
    for(int i = 3; i < argc; ++i)
    {
        std::string arg = argv[i];
        if(arg == "--compact-all")
            flag_compact_all = true;
        else if(arg == "--load")
            flag_load_only = true;
        else if(arg == "--reload")
            flag_reload = true;
        else
        {
            std::cerr << "Unknown flag: " << arg << std::endl;
            return 1;
        }
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "=== database_perf_test_mixed (50/50 r/w) ===" << std::endl;
    std::cout << "N_LOADS=" << INITIAL_WRITES
              << "  N_OPS=" << N_OPS
              << "  PAYLOAD_SIZE=" << PAYLOAD_SIZE
              << "  MEMTABLE_CAPACITY=" << MEMTABLE_CAPACITY;
    if(flag_compact_all)
        std::cout << "  --compact-all";
    if(flag_load_only)
        std::cout << "  --load";
    if(flag_reload)
        std::cout << "  --reload";
    std::cout << std::endl;

    // --- Init ---
    async::executor_pool::init_static_pool(20, 64);

    const std::filesystem::path base_path = "/tmp/db_perf";

    db_config config;
    config.auto_compaction = true;
    config.compaction_read_ahead_size_bytes = 2 * 1024 * 1024;
    config.memtable_budget = MEMTABLE_CAPACITY;
    config.num_partition_exponent = 4;
    config.bucket_ratio = 1.50;
    config.use_odirect_for_indices = true;
    config.index_page_clock_cache_size_bytes = 0;
    config.index_point_cache_size_bytes = 0;
    config.compaction_io_workers = 8;
    config.flush_io_workers = 8;
    config.disable_wal = false;

    auto maybe_db = [&]() -> expected<std::shared_ptr<database>>
    {
        if(flag_reload)
            return database::load(base_path, config);
        if(std::filesystem::exists(base_path))
            std::filesystem::remove_all(base_path);
        return database::make_new(base_path, config);
    }();
    if(!maybe_db)
    {
        std::cerr << "Failed to open database: " << maybe_db.error().to_string() << std::endl;
        return 1;
    }
    auto db = std::move(maybe_db.value());
    auto values = pregenerate_values(PAYLOAD_SIZE);

    constexpr size_t NUM_WORKERS = 20;
    auto executors = async::executor_pool::static_pool().executors();
    executors.resize(NUM_WORKERS);

    // =========================================================================
    // Initial write phase — seed the database so reads have valid targets
    // =========================================================================
    if(INITIAL_WRITES > 0 && !flag_reload)
    {
        const size_t num_batches = (INITIAL_WRITES + BATCH_SIZE - 1) / BATCH_SIZE;
        auto wg = async::wait_group::make_shared();
        wg->set(num_batches);

        auto make_put_batch = [&](size_t batch_start) -> async::task<void>
        {
            const size_t batch_end = std::min(batch_start + BATCH_SIZE, INITIAL_WRITES);
            std::vector<kv_pair_t> entries;
            entries.reserve(batch_end - batch_start);
            for(size_t i = batch_start; i < batch_end; ++i)
                entries.emplace_back(make_key(i), values[value_slot(i)]);

            auto status = co_await db->put_batch_async(entries);
            if(!status)
                std::cerr << "initial put_batch error at batch_start=" << batch_start << ": " << status.error().to_string() << std::endl;
            wg->decr();
        };

        auto t0 = clk::now();
        for(size_t b = 0; b < INITIAL_WRITES; b += BATCH_SIZE)
            executors[(b / BATCH_SIZE) % NUM_WORKERS]->submit_io_task(make_put_batch(b));
        wg->wait();
        auto t1 = clk::now();

        double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
        std::cout << "\n--- Initial write phase (" << INITIAL_WRITES << " keys) ---" << std::endl;
        std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
        std::cout << "Throughput: " << static_cast<uint64_t>(INITIAL_WRITES / elapsed_s) << " items/s" << std::endl;
        std::cout << "Backpressure: " << memtable::HALT_COUNTER.load(std::memory_order_relaxed) << " total backpressure events during initial load" << std::endl;

        if(N_OPS == 0)
        {
            // wait for compaction
            std::cout << "\nWaiting for pending compactions..." << std::endl;
            db->wait_for_compactions_to_finish();
            auto t2 = clk::now();
            double total_s = std::chrono::duration<double>(t2 - t0).count();
            std::cout << "Total (w/compaction): " << total_s * 1000.0 << " ms" << std::endl;
            std::cout << "Total throughput (w/compaction): " << static_cast<uint64_t>(INITIAL_WRITES / total_s) << " items/s" << std::endl;
            std::cout << "\n=== DONE ===" << std::endl;
            db->print_tree_structure();
            return 0;
        }
    }

    // =========================================================================
    // Compact-all phase (optional)
    // =========================================================================
    if(flag_compact_all)
    {
        std::cout << "\n--- Compact-all phase ---" << std::endl;
        auto t0 = clk::now();
        db->trigger_compaction(true);
        db->wait_for_compactions_to_finish();
        auto t1 = clk::now();
        double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
        std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
    }

    // =========================================================================
    // Mixed phase — 50/50 read/write
    // =========================================================================
    if(!flag_load_only && N_OPS > 0)
    {
        const size_t num_chunks = (N_OPS + BATCH_SIZE - 1) / BATCH_SIZE;
        auto wg = async::wait_group::make_shared();
        wg->set(num_chunks);

        std::atomic_size_t read_count{0};
        std::atomic_size_t write_count{0};
        std::atomic_size_t read_errors{0};

        // Per-worker PRNG seeds
        std::vector<uint64_t> rng_seeds(NUM_WORKERS);
        {
            std::random_device rd;
            for(auto& s : rng_seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        std::atomic_size_t next_write_idx{INITIAL_WRITES};

        auto make_chunk = [&](size_t chunk_start, size_t worker_id) -> async::task<void>
        {
            const size_t chunk_end = std::min(chunk_start + BATCH_SIZE, N_OPS);
            std::vector<kv_pair_t> write_batch;

            for(size_t op_idx = chunk_start; op_idx < chunk_end; ++op_idx)
            {
                uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op_idx), sizeof(op_idx), 0x12345678);
                bool is_read = (decision & 1) == 0;

                if(is_read)
                {
                    uint64_t& rng = rng_seeds[worker_id];
                    size_t idx = fast_rand(rng) % INITIAL_WRITES;

                    auto key = make_key(idx);
                    auto maybe_value = co_await db->get_async(key);
                    if(!maybe_value)
                    {
                        read_errors.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "get error at idx=" << idx << ": " << maybe_value.error().to_string() << std::endl;
                    }
                    read_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    size_t idx = next_write_idx.fetch_add(1, std::memory_order_relaxed);
                    write_batch.emplace_back(make_key(idx), values[value_slot(idx)]);
                }
            }

            if(!write_batch.empty())
            {
                auto status = co_await db->put_batch_async(write_batch);
                if(!status)
                    std::cerr << "put_batch error at chunk_start=" << chunk_start << ": " << status.error().to_string() << std::endl;
                write_count.fetch_add(write_batch.size(), std::memory_order_relaxed);
            }

            wg->decr();
        };

        std::cout << "\n--- Mixed phase (50/50 r/w, " << N_OPS << " ops, batch_size=" << BATCH_SIZE << ") ---" << std::endl;
        auto t0 = clk::now();

        for(size_t c = 0; c < N_OPS; c += BATCH_SIZE)
        {
            size_t w = (c / BATCH_SIZE) % NUM_WORKERS;
            executors[w]->submit_io_task(make_chunk(c, w));
        }
        wg->wait();

        auto t1 = clk::now();
        double elapsed_s = std::chrono::duration<double>(t1 - t0).count();

        std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
        std::cout << "Total throughput: " << static_cast<uint64_t>(N_OPS / elapsed_s) << " ops/s" << std::endl;
        std::cout << "Reads:  " << read_count.load() << "  (errors: " << read_errors.load() << ")" << std::endl;
        std::cout << "Writes: " << write_count.load() << std::endl;
        std::cout << "Bandwidth: " << (N_OPS * (PAYLOAD_SIZE + KEY_SIZE) / 1e6) / elapsed_s << " MB/s" << std::endl;

        std::cout << "\nWaiting for pending compactions..." << std::endl;
        db->wait_for_compactions_to_finish();

        auto t2 = clk::now();
        double total_s = std::chrono::duration<double>(t2 - t0).count();
        std::cout << "Total (w/compaction): " << total_s * 1000.0 << " ms" << std::endl;
        std::cout << "Total throughput (w/compaction): " << static_cast<uint64_t>(N_OPS / total_s) << " ops/s" << std::endl;

        prof::print_internal_perf_stats(false);

        if(read_errors.load() > 0)
        {
            std::cerr << "Read errors detected: " << read_errors.load() << std::endl;
            return 1;
        }
    }

    std::cout << "\n=== DONE ===" << std::endl;
    db->print_tree_structure();
    return 0;
}
