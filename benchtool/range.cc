#include "db/database.h"
#include "io/static_pool.h"
#include "keygen.h"
#include "size_literals.h"
#include "tmc/ex_any.hpp"
#include "tmc/fork_group.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/task.hpp"
#include "utils.h"
#include <array>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

namespace hedge::db
{
    struct pin_to_thread
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

    struct scan_tier
    {
        const char* label;
        size_t min_entries;
        size_t max_entries;
        size_t read_ahead_size;
        size_t op_dividend;
    };

    void run_range(const std::shared_ptr<database>& db, size_t n, size_t num_threads, bool measure_latency)
    {
        static constexpr std::array tiers = {
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "large  (114688 - 131072)", .min_entries = 114688, .max_entries = 131072, .read_ahead_size = 64 * KiB, .op_dividend = 10000},
        };

        std::vector<uint64_t> seeds(num_threads);
        {
            std::random_device rd;
            for(uint64_t& s : seeds)
                s = (static_cast<uint64_t>(rd()) << 32) | rd();
        }

        std::cout << "\n=== Range scan ===\n";

        for(const scan_tier& tier : tiers)
        {
            size_t n_ops = n / tier.op_dividend;

            std::atomic_size_t scan_count{0};
            std::shared_ptr<latency_collector> hist;
            if(measure_latency)
            {
                hist = std::make_shared<latency_collector>();
                hist->init(num_threads, n_ops / num_threads);
                std::string label = std::string("seek (range ") + tier.label + ")";
                get_latency_registry().add(label, hist);
            }

            auto worker = [db, tier, hist, n_ops, num_threads, &scan_count, seeds](size_t tid) -> tmc::task<void>
            {
                auto do_scan = [](const std::shared_ptr<database>& db, size_t read_ahead_size, size_t tid, std::shared_ptr<latency_collector> hist, size_t lower_idx, size_t entries, std::atomic_size_t& scan_count, tmc::semaphore& sem) -> tmc::task<void>
                {
                    using clk = std::chrono::high_resolution_clock;
                    auto start = clk::now();
                    auto maybe_it = db->scan(make_key(lower_idx), std::nullopt, read_ahead_size);
                    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                    if(maybe_it)
                    {
                        auto it = std::move(maybe_it.value());
                        for(size_t i = 0; i < entries; ++i)
                        {
                            if(!(co_await it.next()))
                                break;
                        }
                        scan_count.fetch_add(1, std::memory_order_relaxed);
                    }
                    if(hist)
                        hist->record(static_cast<uint64_t>(elapsed), tid);
                    sem.release();
                    co_return;
                };

                auto fg = tmc::fork_group();
                tmc::semaphore sem(io::static_pool::instance()->queue_depth());
                tmc::ex_any* executor = io::static_pool::instance()->ex().type_erased();
                uint64_t rng = seeds[tid];

                for(size_t op = tid; op < n_ops; op += num_threads)
                {
                    co_await sem;
                    co_await pin_to_thread{executor, tid};
                    size_t lower = xorshift64(rng) % n_ops;
                    size_t entries = tier.min_entries + (xorshift64(rng) % (tier.max_entries - tier.min_entries + 1));
                    fg.fork(do_scan(db, tier.read_ahead_size, tid, hist, lower, entries, scan_count, sem));
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
            size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

            std::cout << "\n--- " << tier.label << " (" << n_ops << " scans) ---\n"
                      << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                      << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                      << "Keys/s:     " << static_cast<uint64_t>(completed * avg_entries / elapsed_s) << "\n";
            if(measure_latency)
                get_latency_registry().print_all();
        }
    }

} // namespace hedge::db
