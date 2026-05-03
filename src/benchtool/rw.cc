#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <vector>
#include "db/database.h"
#include "io/static_pool.h"
#include "keygen.h"
#include "perf_counter.h"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/task.hpp"
#include "utils.h"

namespace hedge::db
{
    void run_rw(const std::shared_ptr<database>& db, const values_t& values,
                size_t n, size_t vsize, size_t num_threads, bool measure_latency)
    {
        std::atomic_size_t reads{0};
        std::atomic_size_t loads{0};
        std::atomic_size_t read_errors{0};
        std::atomic_size_t next_load_idx{n};
        std::unique_ptr<latency_histogram> read_hist;
        std::unique_ptr<latency_histogram> write_hist;
        if (measure_latency)
        {
            read_hist = std::make_unique<latency_histogram>();
            write_hist = std::make_unique<latency_histogram>();
        }
        latency_histogram* read_hist_ptr = read_hist.get();
        latency_histogram* write_hist_ptr = write_hist.get();

        std::vector<uint64_t> seeds(num_threads);
        {
            std::random_device rd;
            for (uint64_t& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        auto worker = [](size_t tid, size_t n, size_t num_threads, uint64_t seed,
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
                if (measure_latency && write_hist)
                {
                    auto start = clk::now();
                    hedge::status status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    write_hist->record(static_cast<uint64_t>(elapsed));
                    if (!status)
                        std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                }
                else
                {
                    hedge::status status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                    if (!status)
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
                if (measure_latency && read_hist)
                {
                    auto start = clk::now();
                    auto result = co_await db->get_async(make_key(idx));
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    read_hist->record(static_cast<uint64_t>(elapsed));
                    if (!result)
                    {
                        errors.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                    }
                }
                else
                {
                    auto result = co_await db->get_async(make_key(idx));
                    if (!result)
                    {
                        errors.fetch_add(1, std::memory_order_relaxed);
                        std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                    }
                }
                rcount.fetch_add(1, std::memory_order_relaxed);
                sem.release();
            };

            auto fg = tmc::fork_group();
            tmc::semaphore read_sem(io::static_pool::instance()->queue_depth());
            tmc::semaphore write_sem(1);
            uint64_t rng = seed;

            for (size_t op = tid; op < n; op += num_threads)
            {
                uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op), sizeof(op), OP_SEED);
                bool is_read = (decision & 1) == 0;

                if (is_read)
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
        tasks.reserve(num_threads);
        for (size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back(worker(tid, n, num_threads, seeds[tid], db, values, reads, loads, read_errors, next_load_idx, measure_latency, read_hist_ptr, write_hist_ptr));
        run_workers(std::move(tasks));

        print_throughput("rw mixed", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if (read_hist || write_hist)
        {
            if (read_hist)
                read_hist->print_percentiles("read (rw mode)");
            if (write_hist)
            {
                write_hist->print_percentiles("write (rw mode)");
                print_latency_note();
            }
        }
        std::cout << "Reads:  " << reads.load() << " (errors: " << read_errors.load() << ")\n"
                  << "Loads: " << loads.load() << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("rw mixed (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

} // namespace hedge::db
