#include "common.h"
#include "db/database.h"
#include "io/io_executor.h"
#include "io/static_pool.h"
#include "keygen.h"
#include "modes.h"
#include "utils.h"
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string_view>

namespace hedge::db
{
    static void print_usage(const char* prog)
    {
        std::cerr << "Usage: " << prog << " [OPTIONS]\n"
                  << "  -n, --num_ops <N>      number of operations       (default: 1000000)\n"
                  << "  -ts, --num_timestamps <N>  timestamps per key (timeseries mode) (default: 1)\n"
                  << "  -v, --vsize <N>        value size in bytes        (default: 100)\n"
                  << "  -m, --mode <mode>      load|timeseries|read|rw|range|range_timeseries|compaction (default: load)\n"
                  << "  -p, --path <path>      database path              (default: /tmp/bench_db)\n"
                  << "  -l, --latency          enable latency measurement (default: disabled)\n"
                  << "  -t, --threads <N>      foreground workers         (default: 12)\n"
                  << "  -b, --bg-threads <N>   background workers, 0=auto (default: 0)\n"
                  << "  -s, --stats            print compaction statistics (default: disabled)\n";
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
            else if(arg == "-ts" || arg == "--num_timestamps")
                cfg.num_ts = std::strtoull(next(), nullptr, 10);
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
            else if(arg == "-s" || arg == "--stats")
                cfg.print_stats = true;
        }
        return cfg;
    }

} // namespace hedge::db

int main(int argc, char* argv[])
{
    using namespace hedge;
    using namespace hedge::db;

    bench_config cfg = parse_args(argc, argv);

    if(cfg.mode != "load" && cfg.mode != "timeseries" && cfg.mode != "read" && cfg.mode != "rw" && cfg.mode != "range" && cfg.mode != "range_timeseries" && cfg.mode != "compaction")
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

    io::io_executor::VERBOSE = true;

    io::static_pool::instance()->init(
        io::executor_config{
            .name = "bench_pool",
            .queue_depth = 16,
            .type = io::executor_type::FOREGROUND,
            .n_threads = cfg.num_threads,
            .auto_detect = true,
        });

    std::cout << std::fixed << std::setprecision(2)
              << "=== benchtool ===\n"
              << "mode=" << cfg.mode
              << "  n=" << cfg.num_ops
              << "  num_ts=" << cfg.num_ts
              << "  vsize=" << cfg.vsize
              << "  path=" << cfg.db_path
              << "  latency=" << (cfg.measure_latency ? "enabled" : "disabled")
              << "  threads=" << cfg.num_threads
              << "  bg_threads=" << cfg.num_bg_threads.value_or(0)
              << "  stats=" << (cfg.print_stats ? "enabled" : "disabled")
              << "\n";

    expected<std::shared_ptr<database>> maybe_db = open_db(cfg);
    if(!maybe_db)
    {
        std::cerr << "Failed to open database: " << maybe_db.error().to_string() << "\n";
        return 1;
    }
    std::shared_ptr<database> db = std::move(maybe_db.value());
    values_t values = pregenerate_values(cfg.vsize);

    if(cfg.mode == "load")
        run_load(db, values, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "timeseries")
        run_load_timeseries(db, values, cfg.num_ops, cfg.num_ts, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "read")
        run_read(db, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "rw")
        run_rw(db, values, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "range")
        run_range(db, cfg.num_ops, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "range_timeseries")
        run_range_timeseries(db, cfg.num_ops, cfg.num_threads, cfg.measure_latency);
    else if(cfg.mode == "compaction")
        run_compaction(db, values, cfg.num_ops, cfg.vsize, cfg.num_threads, cfg.measure_latency);

    if(cfg.print_stats)
    {
        db->print_compaction_stats();
        std::cout << "\n";
        db->print_tree_structure();
    }

    std::cout << "\n=== DONE ===\n";

    db->wait_for_compactions_to_finish();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    io::static_pool::instance()->shutdown();
    print_max_rss();
    if(cfg.measure_latency)
        get_latency_registry().print_all();
}
