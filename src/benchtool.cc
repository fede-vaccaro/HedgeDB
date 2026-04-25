#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
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
    static constexpr uint32_t QUEUE_DEPTH = 16;
    static constexpr uint64_t KEY_SEED = 0xDEADBEEF;
    static constexpr uint64_t OP_SEED = 0x12345678;
    static constexpr size_t NVALUES = 1024;

    using values_t = std::vector<std::vector<std::byte>>;

    static constexpr size_t LATENCY_HISTOGRAM_BUCKETS = 10000; // 10k buckets → up to 100 ms range
    static constexpr uint64_t LATENCY_BUCKET_SIZE_NS = 500;    // 500 ns per bucket → granularity for sub-microsecond latencies

    struct latency_histogram
    {
        std::vector<std::atomic<uint64_t>> buckets;
        std::atomic<uint64_t> min_ns{UINT64_MAX};
        std::atomic<uint64_t> max_ns{0};
        std::atomic<uint64_t> total_ns{0};
        std::atomic<uint64_t> count{0};

        latency_histogram() : buckets(LATENCY_HISTOGRAM_BUCKETS)
        {
            for(auto& bucket : buckets)
                bucket.store(0, std::memory_order_relaxed);
        }

        void record(uint64_t ns)
        {
            count.fetch_add(1, std::memory_order_relaxed);
            total_ns.fetch_add(ns, std::memory_order_relaxed);

            uint64_t old_min = min_ns.load(std::memory_order_relaxed);
            while(ns < old_min && !min_ns.compare_exchange_weak(old_min, ns, std::memory_order_relaxed))
                ;

            uint64_t old_max = max_ns.load(std::memory_order_relaxed);
            while(ns > old_max && !max_ns.compare_exchange_weak(old_max, ns, std::memory_order_relaxed))
                ;

            size_t bucket = std::min(ns / LATENCY_BUCKET_SIZE_NS, LATENCY_HISTOGRAM_BUCKETS - 1);
            buckets[bucket].fetch_add(1, std::memory_order_relaxed);
        }

        void print_percentiles(const char* label) const
        {
            auto cnt = count.load(std::memory_order_relaxed);
            if(cnt == 0)
                return;

            std::vector<uint64_t> cumulative(LATENCY_HISTOGRAM_BUCKETS);
            uint64_t running = 0;
            for(size_t i = 0; i < LATENCY_HISTOGRAM_BUCKETS; ++i)
            {
                running += buckets[i].load(std::memory_order_relaxed);
                cumulative[i] = running;
            }

            auto percentile = [&](double p) -> uint64_t
            {
                auto target = static_cast<uint64_t>(std::ceil(cnt * p));
                auto it = std::lower_bound(cumulative.begin(), cumulative.end(), target);
                size_t idx = std::distance(cumulative.begin(), it);
                return idx * LATENCY_BUCKET_SIZE_NS;
            };

            auto avg = total_ns.load(std::memory_order_relaxed) / cnt;
            auto min_val = min_ns.load(std::memory_order_relaxed);
            auto max_val = max_ns.load(std::memory_order_relaxed);

            std::cout << "\n--- " << label << " Latency ---\n"
                      << "Count:      " << cnt << "\n"
                      << "Min:        " << min_val / 1000.0 << " us\n"
                      << "Max:        " << max_val / 1'000'000.0 << " ms\n"
                      << "Avg:        " << avg / 1000.0 << " us\n"
                      << "p50:        " << percentile(0.50) / 1000.0 << " us\n"
                      << "p75:        " << percentile(0.75) / 1000.0 << " us\n"
                      << "p90:        " << percentile(0.90) / 1000.0 << " us\n"
                      << "p95:        " << percentile(0.95) / 1000.0 << " us\n"
                      << "p99:        " << percentile(0.99) / 1000.0 << " us\n"
                      << "p99.9:      " << percentile(0.999) / 1000.0 << " us\n";
        }
    };

    struct bench_config
    {
        size_t num_ops = 1'000'000;
        size_t vsize = 100;
        std::string mode = "undefined";
        std::filesystem::path db_path = "/tmp/bench_db";
        bool measure_latency = false;
    };

    static void print_usage(const char* prog)
    {
        std::cerr << "Usage: " << prog << " [OPTIONS]\n"
                  << "  -n, --num_ops <N>   number of operations  (default: 1000000)\n"
                  << "  -v, --vsize <N>     value size in bytes   (default: 100)\n"
                  << "  -m, --mode <mode>   load|read|rw|range   (default: load)\n"
                  << "  -p, --path <path>   database path         (default: /tmp/bench_db)\n"
                  << "  -l, --latency       enable latency measurement (default: disabled)\n";
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
            else if(arg == "-l" || arg == "--latency")
                cfg.measure_latency = true;
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
        cfg.compaction_io_workers = 8;
        cfg.flush_io_workers = 8;
        cfg.max_pending_flushes = 8;
        cfg.min_merge_width = 8;
        cfg.max_merge_width = 32;
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

    static void print_latency_note()
    {
        std::cout << "\n*** Note: Write latency measures memtable insert time (not disk flush). ***\n"
                  << "***       Actual durability includes WAL write. SST flush is async.      ***\n";
    }

    static void run_load(const std::shared_ptr<database>& db, const values_t& values,
                         size_t n, size_t vsize, bool measure_latency)
    {
        std::unique_ptr<latency_histogram> hist;
        if(measure_latency)
            hist = std::make_unique<latency_histogram>();
        latency_histogram* hist_ptr = hist.get();

        auto worker = [](size_t tid, size_t n,
                         const std::shared_ptr<database>& db, const values_t& values,
                         bool measure_latency, latency_histogram* hist) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, const std::shared_ptr<database>& db,
                             const values_t& values, tmc::semaphore& sem,
                             bool measure_latency, latency_histogram* hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                if(measure_latency && hist)
                {
                    auto start = clk::now();
                    auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    hist->record(elapsed);
                    if(!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                else
                {
                    auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    if(!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto sem = tmc::semaphore(1);
            for(size_t i = tid; i < n; i += NUM_WORKERS)
            {
                co_await sem;
                fg.fork(put_op(i, db, values, sem, measure_latency, hist));
            }
            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, db, values, measure_latency, hist_ptr));
        run_workers(std::move(tasks));

        print_throughput("load", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(hist)
        {
            hist->print_percentiles("load");
            print_latency_note();
        }
        std::cout << "Backpressure: " << memtable::HALT_COUNTER << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("load (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

    static void run_read(const std::shared_ptr<database>& db, size_t n, size_t vsize, bool measure_latency)
    {
        std::atomic_size_t errors{0};
        std::unique_ptr<latency_histogram> hist;
        if(measure_latency)
            hist = std::make_unique<latency_histogram>();
        latency_histogram* hist_ptr = hist.get();

        auto worker = [](size_t tid, size_t n,
                         const std::shared_ptr<database>& db, std::atomic_size_t& errors,
                         bool measure_latency, latency_histogram* hist) -> tmc::task<void>
        {
            auto get_op = [](size_t idx, const std::shared_ptr<database>& db,
                             std::atomic_size_t& errors, tmc::semaphore& sem,
                             bool measure_latency, latency_histogram* hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                if(measure_latency && hist)
                {
                    auto start = clk::now();
                    auto result = co_await db->get_async(make_key(idx));
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    hist->record(elapsed);
                    if(!result)
                        errors.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    auto result = co_await db->get_async(make_key(idx));
                    if(!result)
                        errors.fetch_add(1, std::memory_order_relaxed);
                }
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
            for(size_t i = tid; i < n; i += NUM_WORKERS)
            {
                co_await sem;
                fg.fork(get_op(i, db, errors, sem, measure_latency, hist));
            }
            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, db, errors, measure_latency, hist_ptr));
        run_workers(std::move(tasks));

        print_throughput("read", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(hist)
            hist->print_percentiles("read");
        std::cout << "Errors: " << errors.load() << "\n";
        prof::print_internal_perf_stats(true);
    }

    static void run_rw(const std::shared_ptr<database>& db, const values_t& values,
                       size_t n, size_t vsize, bool measure_latency)
    {
        std::atomic_size_t reads{0};
        std::atomic_size_t loads{0};
        std::atomic_size_t read_errors{0};
        std::atomic_size_t next_load_idx{n};
        std::unique_ptr<latency_histogram> read_hist, write_hist;
        if(measure_latency)
        {
            read_hist = std::make_unique<latency_histogram>();
            write_hist = std::make_unique<latency_histogram>();
        }
        latency_histogram* read_hist_ptr = read_hist.get();
        latency_histogram* write_hist_ptr = write_hist.get();

        std::vector<uint64_t> seeds(NUM_WORKERS);
        {
            std::random_device rd;
            for(auto& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        auto worker = [](size_t tid, size_t n, uint64_t seed,
                         const std::shared_ptr<database>& db, const values_t& values,
                         std::atomic_size_t& reads, std::atomic_size_t& loads,
                         std::atomic_size_t& read_errors, std::atomic_size_t& next_load_idx,
                         bool measure_latency, latency_histogram* read_hist, latency_histogram* write_hist) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, const std::shared_ptr<database>& db, const values_t& values,
                             std::atomic_size_t& wcount, tmc::semaphore& sem,
                             bool measure_latency, latency_histogram* write_hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                if(measure_latency && write_hist)
                {
                    auto start = clk::now();
                    auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    write_hist->record(elapsed);
                    if(!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                else
                {
                    auto status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    if(!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                wcount.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto get_op = [](size_t idx, const std::shared_ptr<database>& db,
                             std::atomic_size_t& rcount, std::atomic_size_t& errors,
                             tmc::semaphore& sem, bool measure_latency, latency_histogram* read_hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                if(measure_latency && read_hist)
                {
                    auto start = clk::now();
                    auto result = co_await db->get_async(make_key(idx));
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    read_hist->record(elapsed);
                    if(!result)
                    {
                        errors.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                    }
                }
                else
                {
                    auto result = co_await db->get_async(make_key(idx));
                    if(!result)
                    {
                        errors.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                    }
                }
                rcount.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto fg = tmc::fork_group();
            auto read_sem = tmc::semaphore(io::static_pool::instance()->queue_depth());
            auto write_sem = tmc::semaphore(1);
            uint64_t rng = seed;

            for(size_t op = tid; op < n; op += NUM_WORKERS)
            {
                uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op), sizeof(op), OP_SEED);
                bool is_read = (decision & 1) == 0;

                if(is_read)
                {
                    co_await read_sem;
                    fg.fork(get_op(xorshift64(rng) % n, db, reads, read_errors, read_sem, measure_latency, read_hist));
                }
                else
                {
                    co_await write_sem;
                    fg.fork(put_op(next_load_idx.fetch_add(1, std::memory_order_relaxed), db, values, loads, write_sem, measure_latency, write_hist));
                }
            }

            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(NUM_WORKERS);
        for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
            tasks.push_back(worker(tid, n, seeds[tid], db, values, reads, loads, read_errors, next_load_idx, measure_latency, read_hist_ptr, write_hist_ptr));
        run_workers(std::move(tasks));

        print_throughput("rw mixed", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(read_hist || write_hist)
        {
            if(read_hist)
                read_hist->print_percentiles("read (rw mode)");
            if(write_hist)
            {
                write_hist->print_percentiles("write (rw mode)");
                print_latency_note();
            }
        }
        std::cout << "Reads:  " << reads.load() << " (errors: " << read_errors.load() << ")\n"
                  << "loads: " << loads.load() << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("rw mixed (w/compaction)", n,
                         std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

    static void run_range(const std::shared_ptr<database>& db, size_t n, bool measure_latency)
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
            std::unique_ptr<latency_histogram> hist;
            if(measure_latency)
                hist = std::make_unique<latency_histogram>();
            latency_histogram* hist_ptr = hist.get();

            auto worker = [](size_t tid, size_t n, uint64_t seed,
                             const std::shared_ptr<database>& db, scan_tier tier,
                             std::atomic_size_t& scan_count, bool measure_latency, latency_histogram* hist) -> tmc::task<void>
            {
                auto do_scan = [](const std::shared_ptr<database>& db, size_t lower_idx, size_t entries,
                                  std::atomic_size_t& count, tmc::semaphore& sem,
                                  bool measure_latency, latency_histogram* hist) -> tmc::task<void>
                {
                    using clk = std::chrono::high_resolution_clock;
                    if(measure_latency && hist)
                    {
                        auto start = clk::now();
                        auto maybe_it = db->scan(make_key(lower_idx), std::nullopt);
                        auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                        hist->record(elapsed);
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
                    }
                    else
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
                    fg.fork(do_scan(db, lower, entries, scan_count, sem, measure_latency, hist));
                }

                co_await std::move(fg);
            };

            using clk = std::chrono::high_resolution_clock;
            auto t0 = clk::now();

            std::vector<tmc::task<void>> tasks;
            tasks.reserve(NUM_WORKERS);
            for(size_t tid = 0; tid < NUM_WORKERS; ++tid)
                tasks.push_back(worker(tid, n, seeds[tid], db, tier, scan_count, measure_latency, hist_ptr));
            run_workers(std::move(tasks));

            auto elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
            size_t completed = scan_count.load();
            size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

            std::cout << "\n--- " << tier.label << " (" << n << " scans) ---\n"
                      << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                      << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                      << "Keys/s:     " << static_cast<uint64_t>(completed * avg_entries / elapsed_s) << "\n";
            if(hist)
            {
                std::string label = std::string("seek (range ") + tier.label + ")";
                hist->print_percentiles(label.c_str());
            }
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

    io::static_pool::instance()->init(NUM_WORKERS, QUEUE_DEPTH, "bench-pool", tmc::topology::cpu_kind::PERFORMANCE);

    std::cout << std::fixed << std::setprecision(2)
              << "=== benchtool ===\n"
              << "mode=" << cfg.mode
              << "  n=" << cfg.num_ops
              << "  vsize=" << cfg.vsize
              << "  path=" << cfg.db_path
              << "  latency=" << (cfg.measure_latency ? "enabled" : "disabled")
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
        run_load(db, values, cfg.num_ops, cfg.vsize, cfg.measure_latency);
    else if(cfg.mode == "read")
        run_read(db, cfg.num_ops, cfg.vsize, cfg.measure_latency);
    else if(cfg.mode == "rw")
        run_rw(db, values, cfg.num_ops, cfg.vsize, cfg.measure_latency);
    else if(cfg.mode == "range")
        run_range(db, cfg.num_ops, cfg.measure_latency);

    std::cout << "\n=== DONE ===\n";
    db->print_tree_structure();
    return 0;
}
