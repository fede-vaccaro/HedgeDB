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
#include <future>
#include <iomanip>
#include <iostream>
#include <random>
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
        std::cerr << "Usage: " << argv[0] << " <N_LOADS> <N_OPS> [--compact-all] [--load] [--reload] [--range]" << std::endl;
        return 1;
    }

    const size_t INITIAL_WRITES = std::strtoull(argv[1], nullptr, 10);
    const size_t N_OPS = std::strtoull(argv[2], nullptr, 10);
    constexpr size_t PAYLOAD_SIZE = 100;
    constexpr size_t MEMTABLE_CAPACITY_BYTES = 64 * 1024 * 1024;

    bool flag_compact_all = false;
    bool flag_load_only = false;
    bool flag_reload = false;
    bool flag_range = false;
    for(int i = 3; i < argc; ++i)
    {
        std::string arg = argv[i];
        if(arg == "--compact-all")
            flag_compact_all = true;
        else if(arg == "--load")
            flag_load_only = true;
        else if(arg == "--reload")
            flag_reload = true;
        else if(arg == "--range")
            flag_range = true;
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
              << "  MEMTABLE_CAPACITY=" << MEMTABLE_CAPACITY_BYTES;
    if(flag_compact_all)
        std::cout << "  --compact-all";
    if(flag_load_only)
        std::cout << "  --load";
    if(flag_reload)
        std::cout << "  --reload";
    if(flag_range)
        std::cout << "  --range";
    std::cout << std::endl;

    // --- Init ---
    constexpr size_t NUM_WORKERS = 12;
    constexpr uint32_t QD = 64;

    auto& pool = hedge::io::static_pool::instance()->init(NUM_WORKERS, QD, "io-pool");

    const std::filesystem::path base_path = "/tmp/db_perf";

    db_config config;
    config.auto_compaction = true;
    config.compaction_read_ahead_size_bytes = 2 * 1024 * 1024;
    config.memtable_budget_bytes = MEMTABLE_CAPACITY_BYTES;
    config.num_partition_exponent = 4;
    config.bucket_ratio = 1.50;
    config.use_odirect_for_indices = true;
    config.index_page_clock_cache_size_bytes = 0;
    config.index_point_cache_size_bytes = 0;
    config.compaction_io_workers = 4;
    config.flush_io_workers = 4;
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

    // =========================================================================
    // Initial write phase — seed the database so reads have valid targets
    // =========================================================================
    if(INITIAL_WRITES > 0 && !flag_reload)
    {
        auto make_put = [](size_t i, const auto& db, const auto& values, tmc::semaphore& s) -> tmc::task<void>
        {
            auto key = make_key(i);
            const auto& val = values[value_slot(i)];
            auto status = co_await db->put_async(key, val);
            if(!status)
                std::cerr << "initial put error at i=" << i << ": " << status.error().to_string() << std::endl;
            s.release();
        };

        auto make_put_task = [](size_t tid, const auto& database, size_t initial_writes, const auto& values, auto make_put) -> tmc::task<void>
        {
            auto fg = tmc::fork_group();
            auto semaphore = tmc::semaphore(io::static_pool::instance()->queue_depth());

            for(size_t i = tid; i < initial_writes; i += NUM_WORKERS)
            {
                co_await semaphore;
                fg.fork(make_put(i, database, values, semaphore));
            }

            co_await std::move(fg);
        };

        auto t0 = clk::now();

        std::vector<std::future<void>> futures;
        futures.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            futures.push_back(tmc::post_waitable(pool, make_put_task(tid, db, INITIAL_WRITES, values, make_put), 0, tid));
        for(auto& f : futures)
            f.get();

        auto t1 = clk::now();

        double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
        std::cout << "\n--- Initial write phase (" << INITIAL_WRITES << " keys) ---" << std::endl;
        std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
        std::cout << "Throughput: " << static_cast<uint64_t>(INITIAL_WRITES / elapsed_s) << " items/s" << std::endl;
        std::cout << "Backpressure: " << memtable::BACKPRESSURE << " total backpressure events during initial load" << std::endl;

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
    // if(!flag_load_only && N_OPS > 0)
    // {
    //     std::atomic_size_t read_count{0};
    //     std::atomic_size_t write_count{0};
    //     std::atomic_size_t read_errors{0};

    //     // Per-worker PRNG seeds
    //     std::vector<uint64_t> rng_seeds(NUM_WORKERS);
    //     {
    //         std::random_device rd;
    //         for(auto& s : rng_seeds)
    //             s = (static_cast<uint64_t>(rd()) << 32) | rd();
    //     }

    //     std::atomic_size_t next_write_idx{INITIAL_WRITES};

    //     auto make_read = [](size_t idx, const auto& db, std::atomic_size_t& read_count,
    //                         std::atomic_size_t& read_errors, tmc::semaphore& s) -> tmc::task<void>
    //     {
    //         auto key = make_key(idx);
    //         auto maybe_value = co_await db->get_async(key);
    //         if(!maybe_value)
    //         {
    //             read_errors.fetch_add(1, std::memory_order_relaxed);
    //             std::cerr << "get error at idx=" << idx << ": " << maybe_value.error().to_string() << std::endl;
    //         }
    //         read_count.fetch_add(1, std::memory_order_relaxed);
    //         s.release();
    //     };

    //     auto make_write = [](size_t idx, const auto& db, const auto& values,
    //                          std::atomic_size_t& write_count, tmc::semaphore& s) -> tmc::task<void>
    //     {
    //         auto key = make_key(idx);
    //         const auto& val = values[value_slot(idx)];
    //         auto status = co_await db->put_async(key, val);
    //         if(!status)
    //             std::cerr << "put error at i=" << idx << ": " << status.error().to_string() << std::endl;
    //         write_count.fetch_add(1, std::memory_order_relaxed);
    //         s.release();
    //     };

    //     auto make_op_task = [&](size_t tid) -> tmc::task<void>
    //     {
    //         auto fg = tmc::fork_group();
    //         auto semaphore = tmc::semaphore(io::static_pool::instance()->queue_depth());
    //         uint64_t rng = rng_seeds[tid];

    //         for(size_t op_idx = tid; op_idx < N_OPS; op_idx += NUM_WORKERS)
    //         {
    //             uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op_idx), sizeof(op_idx), 0x12345678);
    //             bool is_read = (decision & 1) == 0;

    //             co_await semaphore;
    //             if(is_read)
    //             {
    //                 size_t idx = fast_rand(rng) % INITIAL_WRITES;
    //                 fg.fork(make_read(idx, db, read_count, read_errors, semaphore));
    //             }
    //             else
    //             {
    //                 size_t idx = next_write_idx.fetch_add(1, std::memory_order_relaxed);
    //                 fg.fork(make_write(idx, db, values, write_count, semaphore));
    //             }
    //         }

    //         co_await std::move(fg);
    //     };

    //     std::cout << "\n--- Mixed phase (50/50 r/w, " << N_OPS << " ops) ---" << std::endl;
    //     auto t0 = clk::now();

    //     std::vector<std::future<void>> futures;
    //     futures.reserve(NUM_WORKERS);
    //     for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
    //         futures.push_back(tmc::post_waitable(pool, make_op_task(tid), 0, tid));
    //     for(auto& f : futures)
    //         f.get();

    //     auto t1 = clk::now();
    //     double elapsed_s = std::chrono::duration<double>(t1 - t0).count();

    //     std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
    //     std::cout << "Total throughput: " << static_cast<uint64_t>(N_OPS / elapsed_s) << " ops/s" << std::endl;
    //     std::cout << "Reads:  " << read_count.load() << "  (errors: " << read_errors.load() << ")" << std::endl;
    //     std::cout << "Writes: " << write_count.load() << std::endl;
    //     std::cout << "Bandwidth: " << (N_OPS * (PAYLOAD_SIZE + KEY_SIZE) / 1e6) / elapsed_s << " MB/s" << std::endl;

    //     std::cout << "\nWaiting for pending compactions..." << std::endl;
    //     db->wait_for_compactions_to_finish();

    //     auto t2 = clk::now();
    //     double total_s = std::chrono::duration<double>(t2 - t0).count();
    //     std::cout << "Total (w/compaction): " << total_s * 1000.0 << " ms" << std::endl;
    //     std::cout << "Total throughput (w/compaction): " << static_cast<uint64_t>(N_OPS / total_s) << " ops/s" << std::endl;

    //     prof::print_internal_perf_stats(false);

    //     if(read_errors.load() > 0)
    //     {
    //         std::cerr << "Read errors detected: " << read_errors.load() << std::endl;
    //         return 1;
    //     }
    // }

    // =========================================================================
    // Range scan phase — measure scans/s for small, medium, and large ranges
    // =========================================================================
    if(flag_range && INITIAL_WRITES > 0)
    {
        struct scan_tier
        {
            const char* label;
            size_t min_entries;
            size_t max_entries;
        };

        constexpr std::array tiers = {
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100},
            scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024},
            scan_tier{.label = "large  (114688 - 131072)", .min_entries = 114688, .max_entries = 131072},
        };

        // Per-worker PRNG seeds
        std::vector<uint64_t> scan_rng_seeds(NUM_WORKERS);
        {
            std::random_device rd;
            for(auto& s : scan_rng_seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        const size_t N_SCANS = std::max(N_OPS, static_cast<size_t>(1000));

        std::cout << "\n=== Range scan phase ===" << std::endl;

        for(const auto& tier : tiers)
        {
            std::atomic_size_t scan_count{0};

            auto do_scan = [](const auto& db, size_t lower_idx, size_t entries,
                              std::atomic_size_t& count, tmc::semaphore& s) -> tmc::task<void>
            {
                auto lower = make_key(lower_idx);
                auto maybe_it = db->scan(lower, std::nullopt);
                if(!maybe_it)
                {
                    s.release();
                    co_return;
                }

                auto it = std::move(maybe_it.value());
                for(size_t i = 0; i < entries; ++i)
                {
                    auto maybe_kv = co_await it.next();
                    if(!maybe_kv)
                        break;
                }

                count.fetch_add(1, std::memory_order_relaxed);
                s.release();
            };

            auto make_scan_task = [&](size_t tid) -> tmc::task<void>
            {
                auto fg = tmc::fork_group();
                auto semaphore = tmc::semaphore(io::static_pool::instance()->queue_depth());
                uint64_t rng = scan_rng_seeds[tid];

                for(size_t op = tid; op < N_SCANS; op += NUM_WORKERS)
                {
                    co_await semaphore;
                    size_t lower_idx = fast_rand(rng) % INITIAL_WRITES;
                    size_t entries = tier.min_entries + (fast_rand(rng) % (tier.max_entries - tier.min_entries + 1));
                    fg.fork(do_scan(db, lower_idx, entries, scan_count, semaphore));
                }

                co_await std::move(fg);
            };

            auto t0 = clk::now();

            std::vector<tmc::task<void>> tasks;
            tasks.reserve(NUM_WORKERS);
            for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
                tasks.push_back(make_scan_task(tid));

            tmc::post_bulk_waitable(pool, tasks.begin(), tasks.end()).wait();

            auto t1 = clk::now();
            double elapsed_s = std::chrono::duration<double>(t1 - t0).count();

            std::cout << "\n--- " << tier.label << " (" << N_SCANS << " scans) ---" << std::endl;
            std::cout << "Duration: " << elapsed_s * 1000.0 << " ms" << std::endl;
            std::cout << "Throughput: " << static_cast<uint64_t>(scan_count.load() / elapsed_s) << " scans/s" << std::endl;
            std::cout << "Throughput (keys/s): " << static_cast<uint64_t>((scan_count.load() * (tier.max_entries + tier.min_entries) / 2) / elapsed_s) << " keys/s" << std::endl;
        }
    }

    std::cout << "\n=== DONE ===" << std::endl;
    db->print_tree_structure();
    db->wait_for_compactions_to_finish();
    return 0;
}
