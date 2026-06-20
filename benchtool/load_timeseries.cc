#include <chrono>
#include <iostream>
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
    void run_load_timeseries(const std::shared_ptr<database>& db, const values_t& values,
                              size_t n, size_t num_ts, size_t vsize,
                              size_t num_threads, bool measure_latency)
    {
        latency_collector* hist = nullptr;
        if(measure_latency)
            hist = get_latency_registry().get_collector("load_timeseries", num_threads, (n * num_ts) / num_threads);

        auto worker = [](size_t tid, const std::shared_ptr<database>& db, const values_t& values, latency_collector* hist, size_t n, size_t num_ts, size_t num_threads) -> tmc::task<void>
        {
            auto put_op = [](size_t i, uint64_t ts, size_t tid, const std::shared_ptr<database>& db, const values_t& values, size_t num_ts, latency_collector* hist, tmc::semaphore& sem) -> tmc::task<void>
            {
                if(hist)
                {
                    using clk = std::chrono::high_resolution_clock;
                    auto start = clk::now();
                    hedge::status status = co_await db->put_async(make_ts_key(i, ts), values[value_slot(i * num_ts + ts)]);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    hist->record(static_cast<uint64_t>(elapsed), tid);
                    if(!status)
                        std::cerr << "put error: " << status.error().to_string() << "\n";
                }
                else
                {
                    hedge::status status = co_await db->put_async(make_ts_key(i, ts), values[value_slot(i * num_ts + ts)]);
                    if(!status)
                        std::cerr << "put error: " << status.error().to_string() << "\n";
                }
                sem.release();
                co_return;
            };

            auto fg = tmc::fork_group();
            tmc::semaphore sem(1);
            for(size_t i = tid; i < n; i += num_threads)
            {
                for(uint64_t ts = 0; ts < num_ts; ++ts)
                {
                    co_await sem;
                    fg.fork(put_op(i, ts, tid, db, values, num_ts, hist, sem));
                }
            }
            co_await std::move(fg);
        };

        size_t total = n * num_ts;

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(num_threads);
        for(size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back(worker(tid, db, values, hist, n, num_ts, num_threads));
        run_workers(std::move(tasks));

        print_throughput("load_timeseries", total, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(measure_latency)
            print_latency_note();
        std::cout << "Backpressure: " << memtable::HALT_COUNTER << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("load_timeseries (w/compaction)", total, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

} // namespace hedge::db
