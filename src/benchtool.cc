#include <algorithm>
#include <array>
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
#include <string>
#include <string_view>
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

    // DEFAULTs
    static constexpr size_t KEY_SIZE = 24;
    static constexpr size_t NUM_WORKERS = 12;
    static constexpr uint32_t QUEUE_DEPTH = 32;
    static constexpr uint64_t KEY_SEED = 0xDEADBEEF;
    static constexpr uint64_t OP_SEED = 0x12345678;
    static constexpr size_t NVALUES = 1024;

    using values_t = std::vector<std::vector<std::byte>>;

    struct bench_config
    {
        size_t num_ops = 1'000'000;
        size_t vsize = 100;
        std::string mode = "undefined";
        std::filesystem::path db_path = "/tmp/bench_db";
    };

    static void print_usage(const char* prog)
    {
        std::cerr << "Usage: " << prog << " [OPTIONS]\n"
                  << "  -n, --num_ops <N>   number of operations  (default: 1000000)\n"
                  << "  -v, --vsize <N>     value size in bytes   (default: 100)\n"
                  << "  -m, --mode <mode>   load|read|rw|range   (default: load)\n"
                  << "  -p, --path <path>   database path         (default: /tmp/bench_db)\n";
    }

    static bench_config parse_args(int argc, char* argv[])
    {
        bench_config cfg;
        for(int i = 1; i < argc; ++i)
        {
            std::string_view arg = argv[i];
            auto next = [&]() -> const char*
            { return (i + 1 < argc) ? argv[++i] : ""; };

            if(arg == "-n" || arg == "--num_ops")
                cfg.num_ops = std::strtoull(next(), nullptr, 10);
            else if(arg == "-v" || arg == "--vsize")
                cfg.vsize = std::strtoull(next(), nullptr, 10);
            else if(arg == "-m" || arg == "--mode")
                cfg.mode = next();
            else if(arg == "-p" || arg == "--path")
                cfg.db_path = next();
        }
        return cfg;
    }

    static key_t make_key(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED);
        auto k = key_t::make_with_length(KEY_SIZE);
        auto span = k.as_bytes();
        std::memset(span.data(), 0, KEY_SIZE);
        std::memcpy(span.data(), &h, std::min(sizeof(h), KEY_SIZE));
        return k;
    }

    static size_t value_slot(size_t i)
    {
        return xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED) % NVALUES;
    }

    static values_t pregenerate_values(size_t vsize)
    {
        values_t values(NVALUES);
        for(size_t slot = 0; slot < NVALUES; ++slot)
        {
            values[slot].resize(vsize);
            std::mt19937 gen(static_cast<uint32_t>(slot));
            std::uniform_int_distribution<uint8_t> dist(0, 255);
            for(auto& b : values[slot])
                b = static_cast<std::byte>(dist(gen));
        }
        return values;
    }

    static uint64_t xorshift64(uint64_t& state)
    {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    static db_config make_db_config()
    {
        db_config cfg;
        cfg.auto_compaction = true;
        cfg.compaction_read_ahead_size_bytes = 2 * MiB;
        cfg.memtable_budget_bytes = 32 * MiB;
        cfg.num_partition_exponent = 4;
        cfg.bucket_ratio = 1.50;
        cfg.use_odirect_for_indices = true;
        cfg.index_page_clock_cache_size_bytes = 0;
        cfg.index_point_cache_size_bytes = 0;
        cfg.compaction_io_workers = 4;
        cfg.flush_io_workers = 4;
        cfg.max_pending_flushes = 8;
        cfg.min_merge_width = 4;
        cfg.max_merge_width = 16;
        cfg.disable_wal = false;
        return cfg;
    }

    static expected<std::shared_ptr<database>> open_db(const bench_config& cfg)
    {
        auto db_cfg = make_db_config();
        if(cfg.mode == "load")
        {
            if(std::filesystem::exists(cfg.db_path))
                std::filesystem::remove_all(cfg.db_path);
            return database::make_new(cfg.db_path, db_cfg);
        }
        return database::load(cfg.db_path, db_cfg);
    }

    static void run_workers(std::vector<tmc::task<void>> tasks)
    {
        std::vector<std::future<void>> futures;
        futures.reserve(tasks.size());
        for(size_t tid = 0; tid < tasks.size(); ++tid)
            futures.push_back(tmc::post_waitable(*io::static_pool::instance(), std::move(tasks[tid]), 0, tid));
        for(auto& f : futures)
            f.get();
    }

    static void print_throughput(const char* label, size_t ops, double elapsed_s, size_t vsize)
    {
        std::cout << "\n--- " << label << " ---\n"
                  << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                  << "Throughput: " << static_cast<uint64_t>(ops / elapsed_s) << " ops/s\n"
                  << "Bandwidth:  " << (ops * (vsize + KEY_SIZE) / 1e6) / elapsed_s << " MB/s\n";
    }

    static void run_load(const std::shared_ptr<database>& db, const values_t& values,
                         size_t n, size_t vsize)
    {
        auto worker = [](size_t tid, size_t n,
                         const std::shared_ptr<database>& db, const values_t& values) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, const std::shared_ptr<database>& db,
                             const values_t& values, tmc::semaphore& sem) -> tmc::task<void>
            {
                auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                if(!status)
                    std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
            for(size_t i = tid; i < n; i += NUM_WORKERS)
            {
                co_await sem;
                fg.fork(put_op(i, db, values, sem));
            }
            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, db, values));
        run_workers(std::move(tasks));

        print_throughput("load", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        std::cout << "Backpressure: " << memtable::HALT_COUNTER << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("load (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

    static void run_read(const std::shared_ptr<database>& db, size_t n, size_t vsize)
    {
        std::atomic_size_t errors{0};

        auto worker = [](size_t tid, size_t n,
                         const std::shared_ptr<database>& db, std::atomic_size_t& errors) -> tmc::task<void>
        {
            auto get_op = [](size_t idx, const std::shared_ptr<database>& db,
                             std::atomic_size_t& errors, tmc::semaphore& sem) -> tmc::task<void>
            {
                auto result = co_await db->get_async(make_key(idx));
                if(!result)
                    errors.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
            for(size_t i = tid; i < n; i += NUM_WORKERS)
            {
                co_await sem;
                fg.fork(get_op(i, db, errors, sem));
            }
            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, db, errors));
        run_workers(std::move(tasks));

        print_throughput("read", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        std::cout << "Errors: " << errors.load() << "\n";
        prof::print_internal_perf_stats(true);
    }

    static void run_rw(const std::shared_ptr<database>& db, const values_t& values,
                       size_t n, size_t vsize)
    {
        std::atomic_size_t reads{0};
        std::atomic_size_t loads{0};
        std::atomic_size_t read_errors{0};
        std::atomic_size_t next_load_idx{n};

        std::vector<uint64_t> seeds(NUM_WORKERS);
        {
            std::random_device rd;
            for(auto& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        auto worker = [](size_t tid, size_t n, uint64_t seed,
                         const std::shared_ptr<database>& db, const values_t& values,
                         std::atomic_size_t& reads, std::atomic_size_t& loads,
                         std::atomic_size_t& read_errors, std::atomic_size_t& next_load_idx) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, const std::shared_ptr<database>& db, const values_t& values,
                             std::atomic_size_t& wcount, tmc::semaphore& sem) -> tmc::task<void>
            {
                auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                if(!status)
                    std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                wcount.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto get_op = [](size_t idx, const std::shared_ptr<database>& db,
                             std::atomic_size_t& rcount, std::atomic_size_t& errors,
                             tmc::semaphore& sem) -> tmc::task<void>
            {
                auto result = co_await db->get_async(make_key(idx));
                if(!result)
                {
                    errors.fetch_add(1, std::memory_order_relaxed);
                    std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                }
                rcount.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
            uint64_t rng = seed;

            for(size_t op = tid; op < n; op += NUM_WORKERS)
            {
                uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op), sizeof(op), OP_SEED);
                bool is_read = (decision & 1) == 0;

                co_await sem;
                if(is_read)
                    fg.fork(get_op(xorshift64(rng) % n, db, reads, read_errors, sem));
                else
                    fg.fork(put_op(next_load_idx.fetch_add(1, std::memory_order_relaxed), db, values, loads, sem));
            }

            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, seeds[tid], db, values, reads, loads, read_errors, next_load_idx));
        run_workers(std::move(tasks));

        print_throughput("rw mixed", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        std::cout << "Reads:  " << reads.load() << " (errors: " << read_errors.load() << ")\n"
                  << "loads: " << loads.load() << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("rw mixed (w/compaction)", n,
                         std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

    static void run_range(const std::shared_ptr<database>& db, size_t n)
    {
        struct scan_tier
        {
            const char* label;
            size_t min_entries;
            size_t max_entries;
        };
        static constexpr std::array tiers = {
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100},
            scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024},
            scan_tier{.label = "large  (114688 - 131072)", .min_entries = 114688, .max_entries = 131072},
        };

        std::vector<uint64_t> seeds(NUM_WORKERS);
        {
            std::random_device rd;
            for(auto& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        std::cout << "\n=== Range scan ===\n";

        for(const auto& tier : tiers)
        {
            std::atomic_size_t scan_count{0};

            auto worker = [](size_t tid, size_t n, uint64_t seed,
                             const std::shared_ptr<database>& db, scan_tier tier,
                             std::atomic_size_t& scan_count) -> tmc::task<void>
            {
                auto do_scan = [](const std::shared_ptr<database>& db, size_t lower_idx, size_t entries,
                                  std::atomic_size_t& count, tmc::semaphore& sem) -> tmc::task<void>
                {
                    auto maybe_it = db->scan(make_key(lower_idx), std::nullopt);
                    if(maybe_it)
                    {
                        auto it = std::move(maybe_it.value());
                        for(size_t i = 0; i < entries; ++i)
                        {
                            if(!(co_await it.next()))
                                break;
                        }
                        count.fetch_add(1, std::memory_order_relaxed);
                    }
                    sem.release();
                };

                auto fg = tmc::fork_group();
                auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
                uint64_t rng = seed;

                for(size_t op = tid; op < n; op += NUM_WORKERS)
                {
                    co_await sem;
                    size_t lower = xorshift64(rng) % n;
                    size_t entries = tier.min_entries + (xorshift64(rng) % (tier.max_entries - tier.min_entries + 1));
                    fg.fork(do_scan(db, lower, entries, scan_count, sem));
                }

                co_await std::move(fg);
            };

            using clk = std::chrono::high_resolution_clock;
            auto t0 = clk::now();

            std::vector<tmc::task<void>> tasks;
            tasks.reserve(NUM_WORKERS);
            for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
                tasks.push_back(worker(tid, n, seeds[tid], db, tier, scan_count));
            run_workers(std::move(tasks));

            auto elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
            size_t completed = scan_count.load();
            size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

            std::cout << "\n--- " << tier.label << " (" << n << " scans) ---\n"
                      << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                      << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                      << "Keys/s:     " << static_cast<uint64_t>(completed * avg_entries / elapsed_s) << "\n";
        }
    }

} // namespace hedge::db

int main(int argc, char* argv[])
{
    using namespace hedge;
    using namespace hedge::db;

    auto cfg = parse_args(argc, argv);

    if(cfg.mode != "load" && cfg.mode != "read" && cfg.mode != "rw" && cfg.mode != "range")
    {
        print_usage(argv[0]);
        return 1;
    }

    io::static_pool::instance()->init(NUM_WORKERS, QUEUE_DEPTH, "bench-pool");

    std::cout << std::fixed << std::setprecision(2)
              << "=== benchtool ===\n"
              << "mode=" << cfg.mode
              << "  n=" << cfg.num_ops
              << "  vsize=" << cfg.vsize
              << "  path=" << cfg.db_path
              << "\n";

    auto maybe_db = open_db(cfg);
    if(!maybe_db)
    {
        std::cerr << "Failed to open database: " << maybe_db.error().to_string() << "\n";
        return 1;
    }
    auto db = std::move(maybe_db.value());
    auto values = pregenerate_values(cfg.vsize);

    if(cfg.mode == "load")
        run_load(db, values, cfg.num_ops, cfg.vsize);
    else if(cfg.mode == "read")
        run_read(db, cfg.num_ops, cfg.vsize);
    else if(cfg.mode == "rw")
        run_rw(db, values, cfg.num_ops, cfg.vsize);
    else if(cfg.mode == "range")
        run_range(db, cfg.num_ops);

    std::cout << "\n=== DONE ===\n";
    db->print_tree_structure();
    return 0;
}
