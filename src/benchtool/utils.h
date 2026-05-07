#pragma once
#include <memory>
#include <vector>

#include "common.h"
#include "db/database.h"
#include "tmc/task.hpp"

namespace hedge::db
{
    db_config make_db_config(size_t num_bg_threads);
    expected<std::shared_ptr<database>> open_db(const bench_config& cfg);
    void run_workers(std::vector<tmc::task<void>> tasks);
    void print_throughput(const char* label, size_t ops, double elapsed_s, size_t vsize);
    void print_latency_note();
    void print_max_rss();

} // namespace hedge::db
