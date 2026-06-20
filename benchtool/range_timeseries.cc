#include "db/database.h"
#include "io/static_pool.h"
#include "keygen.h"
#include "size_literals.h"
#include "tmc/ex_any.hpp"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/task.hpp"
#include "utils.h"
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

namespace hedge::db
{
    struct pin_to_thread_ts
    {
        tmc::ex_any* executor;
        size_t thread_hint;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const noexcept
        {
            executor->post(std::move(h), 0, thread_hint);
        }
        void await_resume() const noexcept {}
    };

    // Range scan over the time-series dataset: each scan covers one device's full
    // series, [make_ts_key(i, 0), make_ts_key(i, UINT64_MAX)), i.e. all timestamps
    // under a single hash prefix. One scan per device, no size tiers.
    void run_range_timeseries(const std::shared_ptr<database>& db, size_t n, size_t num_threads, bool measure_latency)
    {
        static constexpr size_t READ_AHEAD_SIZE = 64 * KiB;

        std::atomic_size_t scan_count{0};
        std::atomic_size_t key_count{0};
        std::shared_ptr<latency_collector> hist;
        if(measure_latency)
        {
            hist = std::make_shared<latency_collector>();
            hist->init(num_threads, n / num_threads);
            get_latency_registry().add("scan (range_timeseries)", hist);
        }

        std::cout << "\n=== Range scan (timeseries) ===\n";

        auto worker = [db, hist, n, num_threads, &scan_count, &key_count](size_t tid) -> tmc::task<void>
        {
            auto do_scan = [](const std::shared_ptr<database>& db, size_t read_ahead_size, size_t device, size_t tid, std::shared_ptr<latency_collector> hist, std::atomic_size_t& scan_count, std::atomic_size_t& key_count, tmc::semaphore& sem) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                key_t lower = make_ts_key(device, 0);
                key_t upper = make_ts_key(device, UINT64_MAX);

                auto start = clk::now();
                auto maybe_it = db->scan(lower, upper, read_ahead_size);
                size_t entries = 0;
                if(maybe_it)
                {
                    auto it = std::move(maybe_it.value());
                    while(co_await it.next())
                        ++entries;
                }
                auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                key_count.fetch_add(entries, std::memory_order_relaxed);
                scan_count.fetch_add(1, std::memory_order_relaxed);
                if(hist)
                    hist->record(static_cast<uint64_t>(elapsed), tid);
                sem.release();
                co_return;
            };

            auto fg = tmc::fork_group();
            tmc::semaphore sem(io::static_pool::instance()->queue_depth());
            tmc::ex_any* executor = io::static_pool::instance()->ex().type_erased();

            for(size_t device = tid; device < n; device += num_threads)
            {
                co_await sem;
                co_await pin_to_thread_ts{executor, tid};
                fg.fork(do_scan(db, READ_AHEAD_SIZE, device, tid, hist, scan_count, key_count, sem));
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

        double elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
        size_t completed = scan_count.load();
        size_t keys = key_count.load();

        std::cout << "\n--- range_timeseries (" << completed << " scans) ---\n"
                  << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                  << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                  << "Keys/s:     " << static_cast<uint64_t>(keys / elapsed_s) << "\n"
                  << "Avg/scan:   " << (completed ? keys / completed : 0) << " entries\n";
        if(measure_latency)
            get_latency_registry().print_all();
    }

} // namespace hedge::db
