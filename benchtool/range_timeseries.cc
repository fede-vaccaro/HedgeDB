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
            executor->post(std::move(h), 0, thread_hint); // NOLINT(performance-move-const-arg)
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
        std::unique_ptr<latency_histogram> hist;
        if(measure_latency)
            hist = std::make_unique<latency_histogram>();
        latency_histogram* hist_ptr = hist.get();

        std::cout << "\n=== Range scan (timeseries) ===\n";

        auto worker = [](size_t tid, size_t n, size_t num_threads,
                         const std::shared_ptr<database>& db,
                         std::atomic_size_t& scan_count, std::atomic_size_t& key_count,
                         bool measure_latency, latency_histogram* hist) -> tmc::task<void>
        {
            auto do_scan = [](
                               const std::shared_ptr<database>& db,
                               size_t device,
                               std::atomic_size_t& scan_count,
                               std::atomic_size_t& key_count,
                               tmc::semaphore& sem,
                               bool measure_latency,
                               latency_histogram* hist) -> tmc::task<void>
            {
                using clk = std::chrono::high_resolution_clock;
                key_t lower = make_ts_key(device, 0);
                key_t upper = make_ts_key(device, UINT64_MAX);

                auto start = clk::now();
                auto maybe_it = db->scan(lower, upper, READ_AHEAD_SIZE);
                if(maybe_it)
                {
                    auto it = std::move(maybe_it.value());
                    size_t entries = 0;
                    while(co_await it.next())
                        ++entries;
                    key_count.fetch_add(entries, std::memory_order_relaxed);
                    scan_count.fetch_add(1, std::memory_order_relaxed);
                }
                if(measure_latency && hist)
                {
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    hist->record(static_cast<uint64_t>(elapsed));
                }
                sem.release();
            };

            auto fg = tmc::fork_group();
            tmc::semaphore sem(io::static_pool::instance()->queue_depth());
            tmc::ex_any* executor = io::static_pool::instance()->ex().type_erased();

            for(size_t device = tid; device < n; device += num_threads)
            {
                co_await sem;
                co_await pin_to_thread_ts{executor, tid}; // Hack around tmc for better thread-locality
                fg.fork(do_scan(db, device, scan_count, key_count, sem, measure_latency, hist));
            }

            co_await std::move(fg);
        };

        using clk = std::chrono::high_resolution_clock;
        auto t0 = clk::now();

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(num_threads);
        for(size_t tid = 0; tid < num_threads; ++tid)
            tasks.push_back(worker(tid, n, num_threads, db, scan_count, key_count, measure_latency, hist_ptr));
        run_workers(std::move(tasks));

        double elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
        size_t completed = scan_count.load();
        size_t keys = key_count.load();

        std::cout << "\n--- range_timeseries (" << completed << " scans) ---\n"
                  << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                  << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                  << "Keys/s:     " << static_cast<uint64_t>(keys / elapsed_s) << "\n"
                  << "Avg/scan:   " << (completed ? keys / completed : 0) << " entries\n";
        if(hist)
            hist->print_percentiles("scan (range_timeseries)");
    }

} // namespace hedge::db
