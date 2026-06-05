#pragma once
#include <memory>
#include "common.h"
#include "db/database.h"

namespace hedge::db
{
    void run_load(const std::shared_ptr<database>& db, const values_t& values,
                  size_t n, size_t vsize, size_t num_threads, bool measure_latency);

    void run_read(const std::shared_ptr<database>& db,
                  size_t n, size_t vsize, size_t num_threads, bool measure_latency);

    void run_rw(const std::shared_ptr<database>& db, const values_t& values,
                size_t n, size_t vsize, size_t num_threads, bool measure_latency);

    void run_range(const std::shared_ptr<database>& db, size_t n, size_t num_threads, bool measure_latency);

    void run_compaction(const std::shared_ptr<database>& db, const values_t& values,
                        size_t n, size_t vsize, size_t num_threads, bool measure_latency);

} // namespace hedge::db
