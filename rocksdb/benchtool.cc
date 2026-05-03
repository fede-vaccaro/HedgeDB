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
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <xxh64.hpp>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

static constexpr size_t KEY_SIZE = 24;
static constexpr size_t NUM_WORKERS = 20;
static constexpr uint64_t KEY_SEED = 0xDEADBEEF;
static constexpr uint64_t OP_SEED = 0x12345678;
static constexpr size_t NUNIQUEVALUES = 1024;
static constexpr size_t MiB = 1024ULL * 1024;
static constexpr size_t MULTIGET_BATCH_SIZE = 8;

static constexpr size_t LATENCY_HISTOGRAM_BUCKETS = 10000;
static constexpr uint64_t LATENCY_BUCKET_SIZE_NS = 500;

struct latency_histogram
{
    std::vector<std::atomic<uint64_t>> buckets;
    std::atomic<uint64_t> min_ns{UINT64_MAX};
    std::atomic<uint64_t> max_ns{0};
    std::atomic<uint64_t> total_ns{0};
    std::atomic<uint64_t> count{0};

    latency_histogram() : buckets(LATENCY_HISTOGRAM_BUCKETS)
    {
        for(auto& b : buckets)
            b.store(0, std::memory_order_relaxed);
    }

    void record(uint64_t ns)
    {
        count.fetch_add(1, std::memory_order_relaxed);
        total_ns.fetch_add(ns, std::memory_order_relaxed);

        uint64_t old = min_ns.load(std::memory_order_relaxed);
        while(ns < old && !min_ns.compare_exchange_weak(old, ns, std::memory_order_relaxed))
            ;

        old = max_ns.load(std::memory_order_relaxed);
        while(ns > old && !max_ns.compare_exchange_weak(old, ns, std::memory_order_relaxed))
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
    std::filesystem::path db_path = "/tmp/bench_db_rocksdb";
    bool measure_latency = false;
    size_t num_threads = NUM_WORKERS;
    size_t num_bg_threads = 4;
};

static void print_usage(const char* prog)
{
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "  -n, --num_ops <N>      number of operations       (default: 1000000)\n"
              << "  -v, --vsize <N>        value size in bytes        (default: 100)\n"
              << "  -m, --mode <mode>      load|read|rw|range         (default: load)\n"
              << "  -p, --path <path>      database path              (default: /tmp/bench_db_rocksdb)\n"
              << "  -l, --latency          enable latency measurement (default: disabled)\n"
              << "  -t, --threads <N>      foreground workers         (default: 20)\n"
              << "  -b, --bg-threads <N>   max_background_jobs        (default: 4)\n";
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
        else if(arg == "-t" || arg == "--threads")
            cfg.num_threads = std::strtoull(next(), nullptr, 10);
        else if(arg == "-b" || arg == "--bg-threads")
            cfg.num_bg_threads = std::strtoull(next(), nullptr, 10);
    }
    return cfg;
}

static std::string make_key(size_t i)
{
    uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED);
    std::string k(KEY_SIZE, '\0');
    std::memcpy(k.data(), &h, sizeof(h));
    return k;
}

static size_t value_slot(size_t i)
{
    return xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED) % NUNIQUEVALUES;
}

using values_t = std::vector<std::string>;

static values_t pregenerate_values(size_t vsize)
{
    values_t values(NUNIQUEVALUES);
    for(size_t slot = 0; slot < NUNIQUEVALUES; ++slot)
    {
        values[slot].resize(vsize);
        std::mt19937 gen(static_cast<uint32_t>(slot));
        std::uniform_int_distribution<uint8_t> dist(0, 255);
        for(auto& b : values[slot])
            b = static_cast<char>(dist(gen));
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

static rocksdb::Options make_db_options(size_t num_bg_threads)
{
    rocksdb::Options opts;
    opts.create_if_missing = true;
    opts.compression = rocksdb::kNoCompression;
    opts.max_background_jobs = num_bg_threads > 0 ? static_cast<int>(num_bg_threads) : 8;
    opts.OptimizeUniversalStyleCompaction();

    auto& uc = opts.compaction_options_universal;
    uc.min_merge_width = 8;
    uc.max_merge_width = 32; // unlimited
    uc.allow_trivial_move = true;
    opts.num_levels = 40;
    opts.compaction_readahead_size = 2 * MiB;
    opts.level0_stop_writes_trigger = std::numeric_limits<int>::max();
    opts.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();

    opts.use_direct_reads = true;
    opts.use_direct_io_for_flush_and_compaction = true;

    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = rocksdb::HyperClockCacheOptions(1ULL * 1024 * 1024 * 1024).MakeSharedCache();
    table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    table_opts.block_size = 4 * 1024;
    opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));
    table_opts.pin_l0_filter_and_index_blocks_in_cache = true;

    return opts;
}

static std::unique_ptr<rocksdb::DB> open_db(const bench_config& cfg)
{
    if(cfg.mode == "load" && std::filesystem::exists(cfg.db_path))
        std::filesystem::remove_all(cfg.db_path);

    std::unique_ptr<rocksdb::DB> db;
    auto status = rocksdb::DB::Open(make_db_options(cfg.num_bg_threads), cfg.db_path.string(), &db);
    if(!status.ok())
    {
        std::cerr << "Failed to open RocksDB: " << status.ToString() << "\n";
        return nullptr;
    }
    return db;
}

static void run_workers(std::vector<std::function<void()>> tasks)
{
    std::vector<std::thread> threads;
    threads.reserve(tasks.size());
    for(auto& task : tasks)
        threads.emplace_back(task);
    for(auto& t : threads)
        t.join();
}

static void print_throughput(const char* label, size_t ops, double elapsed_s, size_t vsize)
{
    std::cout << "\n--- " << label << " ---\n"
              << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
              << "Throughput: " << static_cast<uint64_t>(ops / elapsed_s) << " ops/s\n"
              << "Bandwidth:  " << (ops * (vsize + KEY_SIZE) / 1e6) / elapsed_s << " MB/s\n";
}

static void wait_for_compactions(rocksdb::DB* db)
{
    rocksdb::WaitForCompactOptions opts;
    auto status = db->WaitForCompact(opts);
    if(!status.ok())
        std::cerr << "WaitForCompact: " << status.ToString() << "\n";
}

static void run_load(rocksdb::DB* db, const values_t& values, size_t n, size_t vsize, size_t num_threads, bool measure_latency)
{
    std::unique_ptr<latency_histogram> hist;
    if(measure_latency)
        hist = std::make_unique<latency_histogram>();
    latency_histogram* hist_ptr = hist.get();

    auto worker = [&](size_t tid)
    {
        rocksdb::WriteOptions write_opts;
        for(size_t i = tid; i < n; i += num_threads)
        {
            auto key = make_key(i);
            const auto& val = values[value_slot(i)];

            if(measure_latency && hist_ptr)
            {
                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();
                auto status = db->Put(write_opts, key, val);
                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                hist_ptr->record(elapsed);
                if(!status.ok())
                    std::cerr << "put error at " << i << ": " << status.ToString() << "\n";
            }
            else
            {
                auto status = db->Put(write_opts, key, val);
                if(!status.ok())
                    std::cerr << "put error at " << i << ": " << status.ToString() << "\n";
            }
        }
    };

    using clk = std::chrono::high_resolution_clock;
    auto t0 = clk::now();

    std::vector<std::function<void()>> tasks;
    tasks.reserve(num_threads);
    for(size_t tid = 0; tid < num_threads; ++tid)
        tasks.push_back([&worker, tid]()
                        { worker(tid); });
    run_workers(std::move(tasks));

    print_throughput("load", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
    if(hist)
    {
        hist->print_percentiles("load");
        std::cout << "\n*** Note: Write latency measures RocksDB Put() time (includes WAL write). SST flush is async. ***\n";
    }

    std::cout << "Waiting for compactions...\n";
    wait_for_compactions(db);
    print_throughput("load (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
}

static void run_read(rocksdb::DB* db, size_t n, size_t vsize, size_t num_threads, bool measure_latency)
{
    std::atomic_size_t errors{0};
    std::unique_ptr<latency_histogram> hist;
    if(measure_latency)
        hist = std::make_unique<latency_histogram>();
    latency_histogram* hist_ptr = hist.get();

    auto worker = [&](size_t tid)
    {
        rocksdb::ReadOptions read_opts;
        read_opts.async_io = true;
        read_opts.optimize_multiget_for_io = true;

        std::vector<std::string> key_strings;
        std::vector<rocksdb::Slice> key_slices;
        std::vector<rocksdb::PinnableSlice> values;
        std::vector<rocksdb::Status> statuses;

        key_strings.reserve(MULTIGET_BATCH_SIZE);
        key_slices.reserve(MULTIGET_BATCH_SIZE);
        values.reserve(MULTIGET_BATCH_SIZE);
        statuses.reserve(MULTIGET_BATCH_SIZE);

        size_t i = tid;
        while(i < n)
        {
            key_strings.clear();
            key_slices.clear();
            values.clear();
            statuses.clear();

            for(size_t b = 0; b < MULTIGET_BATCH_SIZE && i < n; ++b, i += num_threads)
                key_strings.push_back(make_key(i));

            for(const auto& k : key_strings)
            {
                key_slices.emplace_back(k);
                values.emplace_back();
                statuses.emplace_back();
            }
            values.resize(key_slices.size());

            if(measure_latency && hist_ptr)
            {
                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();

                db->MultiGet(
                    read_opts,
                    db->DefaultColumnFamily(),
                    key_slices.size(),
                    key_slices.data(),
                    values.data(),
                    nullptr,
                    statuses.data());

                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                hist_ptr->record(elapsed);
                for(const auto& s : statuses)
                    if(!s.ok())
                        errors.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                throw std::runtime_error("not implemented");
            }
        }
    };

    using clk = std::chrono::high_resolution_clock;
    auto t0 = clk::now();

    std::vector<std::function<void()>> tasks;
    tasks.reserve(num_threads);
    for(size_t tid = 0; tid < num_threads; ++tid)
        tasks.push_back([&worker, tid]()
                        { worker(tid); });
    run_workers(std::move(tasks));

    print_throughput("read", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
    if(hist)
        hist->print_percentiles("read");
    std::cout << "Errors: " << errors.load() << "\n";
}

static void run_rw(rocksdb::DB* db, const values_t& values, size_t n, size_t vsize, size_t num_threads, bool measure_latency)
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

    std::vector<uint64_t> seeds(num_threads);
    {
        std::random_device rd;
        for(auto& s : seeds)
            s = (static_cast<uint64_t>(rd()) << 32) | rd();
    }

    auto worker = [&](size_t tid)
    {
        rocksdb::WriteOptions write_opts;
        rocksdb::ReadOptions read_opts;
        read_opts.async_io = true;
        uint64_t rng = seeds[tid];

        rocksdb::PinnableSlice value_buf;

        for(size_t op = tid; op < n; op += num_threads)
        {
            uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op), sizeof(op), OP_SEED);
            bool is_read = (decision & 1) == 0;

            if(is_read)
            {
                size_t idx = xorshift64(rng) % n;
                auto key = make_key(idx);
                value_buf.Reset();

                if(measure_latency && read_hist_ptr)
                {
                    using clk = std::chrono::high_resolution_clock;
                    auto start = clk::now();
                    auto status = db->Get(read_opts, db->DefaultColumnFamily(), key, &value_buf);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    read_hist_ptr->record(elapsed);
                    if(!status.ok())
                        read_errors.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    auto status = db->Get(read_opts, db->DefaultColumnFamily(), key, &value_buf);
                    if(!status.ok())
                        read_errors.fetch_add(1, std::memory_order_relaxed);
                }
                reads.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                size_t idx = next_load_idx.fetch_add(1, std::memory_order_relaxed);
                auto key = make_key(idx);
                const auto& val = values[value_slot(idx)];

                if(measure_latency && write_hist_ptr)
                {
                    using clk = std::chrono::high_resolution_clock;
                    auto start = clk::now();
                    auto status = db->Put(write_opts, key, val);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    write_hist_ptr->record(elapsed);
                    if(!status.ok())
                        std::cerr << "put error at " << idx << ": " << status.ToString() << "\n";
                }
                else
                {
                    auto status = db->Put(write_opts, key, val);
                    if(!status.ok())
                        std::cerr << "put error at " << idx << ": " << status.ToString() << "\n";
                }
                loads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    using clk = std::chrono::high_resolution_clock;
    auto t0 = clk::now();

    std::vector<std::function<void()>> tasks;
    tasks.reserve(num_threads);
    for(size_t tid = 0; tid < num_threads; ++tid)
        tasks.push_back([&worker, tid]()
                        { worker(tid); });
    run_workers(std::move(tasks));

    print_throughput("rw mixed", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
    if(read_hist || write_hist)
    {
        if(read_hist)
            read_hist->print_percentiles("read (rw mode)");
        if(write_hist)
        {
            write_hist->print_percentiles("write (rw mode)");
            std::cout << "\n*** Note: Write latency includes WAL write. SST flush is async. ***\n";
        }
    }
    std::cout << "Reads:  " << reads.load() << " (errors: " << read_errors.load() << ")\n"
              << "Loads: " << loads.load() << "\n";

    std::cout << "Waiting for compactions...\n";
    wait_for_compactions(db);
    print_throughput("rw mixed (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
}

static void run_range(rocksdb::DB* db, size_t n, size_t num_threads, bool measure_latency)
{
    struct scan_tier
    {
        const char* label;
        size_t min_entries;
        size_t max_entries;
        size_t op_dividend; // number of ops to divide n by to determine number of scans for this tier
    };
    static constexpr std::array tiers = {
        scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .op_dividend = 1000},
        scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .op_dividend = 1000},
        scan_tier{.label = "large  (114688 - 131072)", .min_entries = 114688, .max_entries = 131072, .op_dividend = 10000},
    };

    std::vector<uint64_t> seeds(num_threads);
    {
        std::random_device rd;
        for(auto& s : seeds)
            s = (static_cast<uint64_t>(rd()) << 32) | rd();
    }

    std::cout << "\n=== Range scan ===\n";

    for(const auto& tier : tiers)
    {
        size_t n_ops = n / tier.op_dividend;

        std::atomic_size_t scan_count{0};
        std::unique_ptr<latency_histogram> hist;
        if(measure_latency)
            hist = std::make_unique<latency_histogram>();
        latency_histogram* hist_ptr = hist.get();

        auto worker = [&](size_t tid)
        {
            rocksdb::ReadOptions read_opts;
            read_opts.async_io = true;
            read_opts.adaptive_readahead = true;
            uint64_t rng = seeds[tid];

            for(size_t op = tid; op < n_ops; op += num_threads)
            {
                size_t lower = xorshift64(rng) % n_ops;
                size_t entries = tier.min_entries + (xorshift64(rng) % (tier.max_entries - tier.min_entries + 1));
                auto start_key = make_key(lower);

                auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_opts));

                if(measure_latency && hist_ptr)
                {
                    using clk = std::chrono::high_resolution_clock;
                    auto t_start = clk::now();
                    it->Seek(start_key);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - t_start).count();
                    hist_ptr->record(elapsed);
                }
                else
                {
                    it->Seek(start_key);
                }

                for(size_t i = 0; i < entries && it->Valid(); ++i)
                    it->Next();

                scan_count.fetch_add(1, std::memory_order_relaxed);
            }
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<std::function<void()>> tasks;
        tasks.reserve(num_threads);
        for(size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back([&worker, tid]()
                            { worker(tid); });
        run_workers(std::move(tasks));

        auto elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
        size_t completed = scan_count.load();
        size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

        std::cout << "\n--- " << tier.label << " (" << n_ops << " scans) ---\n"
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

int main(int argc, char* argv[])
{
    auto cfg = parse_args(argc, argv);

    if(cfg.mode != "load" && cfg.mode != "read" && cfg.mode != "rw" && cfg.mode != "range")
    {
        print_usage(argv[0]);
        return 1;
    }

    if(cfg.num_threads == 0)
    {
        std::cerr << "Error: --threads must be >= 1\n";
        print_usage(argv[0]);
        return 1;
    }

    std::cout << std::fixed << std::setprecision(2)
              << "=== rocksdb benchtool ===\n"
              << "mode=" << cfg.mode
              << "  n=" << cfg.num_ops
              << "  vsize=" << cfg.vsize
              << "  path=" << cfg.db_path
              << "  latency=" << (cfg.measure_latency ? "enabled" : "disabled")
              << "  threads=" << cfg.num_threads
              << "  bg_threads=" << cfg.num_bg_threads
              << "\n";

    auto db = open_db(cfg);
    if(!db)
        return 1;

    auto values = pregenerate_values(cfg.vsize);

    if(cfg.mode == "load")
        run_load(db.get(), values, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "read")
        run_read(db.get(), cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "rw")
        run_rw(db.get(), values, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "range")
        run_range(db.get(), cfg.num_ops, cfg.num_threads, cfg.measure_latency);

    std::cout << "\n=== DONE ===\n";
    return 0;
}
