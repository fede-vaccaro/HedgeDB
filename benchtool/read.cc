#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
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
    void run_read(const std::shared_ptr<database>& db, size_t n, size_t vsize, size_t num_threads, bool measure_latency)
    {
        std::atomic_size_t errors{0};
        std::shared_ptr<latency_collector> hist;
        if(measure_latency)
        {
            hist = std::make_shared<latency_collector>();
            hist->init(num_threads, n / num_threads);
            get_latency_registry().add("read", hist);
        }

        auto worker = [db, hist, n, num_threads, &errors](size_t tid) -> tmc::task<void>
        {
            auto get_op = [](size_t idx, size_t tid, const std::shared_ptr<database>& db, std::shared_ptr<latency_collector> hist, std::atomic_size_t& errors, tmc::semaphore& sem) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();
                auto result = co_await db->get_async(make_key(idx));
                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                if(hist)
                    hist->record(static_cast<uint64_t>(elapsed), tid);
                if(!result)
                    errors.fetch_add(1, std::memory_order_relaxed);
                sem.release();
                co_return;
            };

            auto fg = tmc::fork_group();
            tmc::semaphore sem(io::static_pool::instance()->queue_depth());
            for(size_t i = tid; i < n; i += num_threads)
            {
                co_await sem;
                fg.fork(get_op(i, tid, db, hist, errors, sem));
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

        print_throughput("read", n, std::chrono::duration<double>(clk::now() - t0).count(), vsize);
        if(measure_latency)
            get_latency_registry().print_all();
        std::cout << "Errors: " << errors.load() << "\n";
        prof::print_internal_perf_stats(true);
    }
} // namespace hedge::db
