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
        
        std::shared_ptr<latency_collector> read_hist;
        std::shared_ptr<latency_collector> write_hist;
        if(measure_latency)
        {
            read_hist = std::make_shared<latency_collector>();
            write_hist = std::make_shared<latency_collector>();
            read_hist->init(num_threads, n / num_threads);
            write_hist->init(num_threads, n / num_threads);
            get_latency_registry().add("read (rw mode)", read_hist);
            get_latency_registry().add("write (rw mode)", write_hist);
        }

        std::vector<uint64_t> seeds(num_threads);
        {
            std::random_device rd;
            for(uint64_t& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        auto worker = [db, &values, read_hist, write_hist, n, num_threads, &reads, &loads, &read_errors, &next_load_idx, seeds](size_t tid) -> tmc::task<void>
        {
            auto put_op = [](size_t idx, size_t tid, const std::shared_ptr<database>& db, const values_t& values, std::shared_ptr<latency_collector> write_hist, std::atomic_size_t& loads, tmc::semaphore& sem) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();
                hedge::status status = co_await db->put_async(make_key(idx), values[value_slot(idx)]);
                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                if(write_hist)
                    write_hist->record(static_cast<uint64_t>(elapsed), tid);
                if(!status)
                    std::cerr << "put error at " << idx << ": " << status.error().to_string() << "\n";
                loads.fetch_add(1, std::memory_order_relaxed);
                sem.release();
                co_return;
            };

            auto get_op = [](size_t idx, size_t tid, const std::shared_ptr<database>& db, std::shared_ptr<latency_collector> read_hist, std::atomic_size_t& reads, std::atomic_size_t& read_errors, tmc::semaphore& sem) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();
                auto result = co_await db->get_async(make_key(idx));
                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                if(read_hist)
                    read_hist->record(static_cast<uint64_t>(elapsed), tid);
                if(!result)
                {
                    read_errors.fetch_add(1, std::memory_order_relaxed);
                    std::cerr << "get error at " << idx << ": " << result.error().to_string() << "\n";
                }
                reads.fetch_add(1, std::memory_order_relaxed);
                sem.release();
                co_return;
            };

            auto fg = tmc::fork_group();
            tmc::semaphore read_sem(io::static_pool::instance()->queue_depth());
            tmc::semaphore write_sem(1);
            uint64_t rng = seeds[tid];

            for(size_t op = tid; op < n; op += num_threads)
            {
                uint64_t decision = xxh64::hash(reinterpret_cast<const char*>(&op), sizeof(op), OP_SEED);
                bool is_read = (decision & 1) == 0;

                if(is_read)
                {
                    co_await read_sem;
                    fg.fork(get_op(xorshift64(rng) % n, tid, db, read_hist, reads, read_errors, read_sem));
                }
                else
                {
                    co_await write_sem;
                    fg.fork(put_op(next_load_idx.fetch_add(1, std::memory_order_relaxed), tid, db, values, write_hist, loads, write_sem));
                }
            }

            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(num_threads);
        for(size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back(worker(tid));
        run_workers(std::move(tasks));

        print_throughput("rw mixed", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(measure_latency)
        {
            get_latency_registry().print_all();
            print_latency_note();
        }
        std::cout << "Reads:  " << reads.load() << " (errors: " << read_errors.load() << ")\n"
                  << "Loads: " << loads.load() << "\n";

        std::cout << "Waiting for compactions...\n";
        db->wait_for_compactions_to_finish();
        print_throughput("rw mixed (w/compaction)", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        prof::print_internal_perf_stats(false);
    }

} // namespace hedge::db
