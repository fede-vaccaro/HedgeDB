#include <chrono>
#include <iostream>
#include <memory>
#include <vector>
#include "db/database.h"
#include "db/memtable.h"
#include "keygen.h"
#include "perf_counter.h"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/task.hpp"
#include "utils.h"

namespace hedge::db
{
    void run_load(const std::shared_ptr<database>& db, const values_t& values,
                  size_t n, size_t vsize, size_t num_threads, bool measure_latency)
    {
        std::unique_ptr<latency_histogram> hist;
        if (measure_latency)
            hist = std::make_unique<latency_histogram>();
        latency_histogram* hist_ptr = hist.get();

        auto worker = [](size_t tid, size_t n, size_t num_threads,
                         const std::shared_ptr<database>& db, const values_t& values,
                         bool measure_latency, latency_histogram* hist) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, const std::shared_ptr<database>& db,
                             const values_t& values, tmc::semaphore& sem,
                             bool measure_latency, latency_histogram* hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                if (measure_latency && hist)
                {
                    auto start = clk::now();
                    hedge::status status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    hist->record(static_cast<uint64_t>(elapsed));
                    if (!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                else
                {
                    hedge::status status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    if (!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                sem.release();
            };

            auto fg = tmc::fork_group();
            tmc::semaphore sem(1);
            for (size_t i = tid; i < n; i += num_threads)
            {
                co_await sem;
                fg.fork(put_op(i, db, values, sem, measure_latency, hist));
            }
            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(num_threads);
        for (size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back(worker(tid, n, num_threads, db, values, measure_latency, hist_ptr));
        run_workers(std::move(tasks));

        print_throughput("load", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if (hist)
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

} // namespace hedge::db
