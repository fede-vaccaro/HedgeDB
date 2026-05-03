#include "utils.h"
#include "db/database.h"
#include "io/static_pool.h"
#include "tmc/sync.hpp"
#include <filesystem>
#include <future>
#include <iostream>
#include <optional>

namespace hedge::db
{
    db_config make_db_config(size_t num_bg_threads)
    {
        db_config cfg;
        cfg.auto_compaction = true;
        cfg.compaction_read_ahead_size_bytes = 2 * MiB;
        cfg.memtable_budget_bytes = 32 * MiB;
        cfg.num_partition_exponent = 4;
        cfg.bucket_ratio = 1.50;
        cfg.use_odirect_for_ssts = true;
        cfg.index_page_clock_cache_size_bytes = 0;
        cfg.index_point_cache_size_bytes = 0;
        cfg.background_workers = num_bg_threads;
        cfg.max_pending_flushes = 8;
        cfg.min_merge_width = 8;
        cfg.max_merge_width = 32;
        cfg.ssts_in_l0_block_write_threshold = std::nullopt;
        cfg.disable_wal = false;
        return cfg;
    }

    expected<std::shared_ptr<database>> open_db(const bench_config& cfg)
    {
        db_config db_cfg = make_db_config(cfg.num_bg_threads);
        if(cfg.mode == "load")
        {
            if(std::filesystem::exists(cfg.db_path))
                std::filesystem::remove_all(cfg.db_path);
            return database::make_new(cfg.db_path, db_cfg);
        }
        return database::load(cfg.db_path, db_cfg);
    }

    void run_workers(std::vector<tmc::task<void>> tasks)
    {
        std::vector<std::future<void>> futures;
        futures.reserve(tasks.size());
        for(size_t tid = 0; tid < tasks.size(); ++tid)
            futures.push_back(tmc::post_waitable(*io::static_pool::instance(), std::move(tasks[tid]), 0, tid));
        for(std::future<void>& f : futures)
            f.get();
    }

    void print_throughput(const char* label, size_t ops, double elapsed_s, size_t vsize)
    {
        std::cout << "\n--- " << label << " ---\n"
                  << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                  << "Throughput: " << static_cast<uint64_t>(ops / elapsed_s) << " ops/s\n"
                  << "Bandwidth:  " << (ops * (vsize + KEY_SIZE) / 1e6) / elapsed_s << " MB/s\n";
    }

    void print_latency_note()
    {
        std::cout << "\n*** Note: Write latency measures memtable insert time (not disk flush). ***\n"
                  << "***       Actual durability includes WAL write. SST flush is async.      ***\n";
    }

} // namespace hedge::db
