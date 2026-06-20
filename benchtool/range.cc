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
#include <random>
#include <vector>

namespace hedge::db
{
    struct pin_to_thread : tmc::detail::AwaitTagNoGroupAsIs
    {
        tmc::ex_any* executor;
        size_t thread_hint;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const noexcept
        {
            executor->post(std::move(h), 0, thread_hint);
        }
        void await_resume() const noexcept {}

        pin_to_thread(tmc::ex_any* ex, size_t hint) : executor(ex), thread_hint(hint) {}
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
            std::string label = std::string("seek (range ") + tier.label + ")";
            latency_collector* hist = nullptr;
            if(measure_latency)
                hist = get_latency_registry().get_collector(label, num_threads, n_ops / num_threads);

            auto worker = [](size_t tid, const std::shared_ptr<database>& db, size_t tier_min, size_t tier_max, size_t tier_read_ahead_size, latency_collector* hist, size_t n_ops, size_t num_threads, std::atomic_size_t& scan_count, std::vector<uint64_t> seeds) -> tmc::task<void>
            {
                auto do_scan = [](const std::shared_ptr<database>& db, size_t read_ahead_size, size_t tid, latency_collector* hist, size_t lower_idx, size_t entries, std::atomic_size_t& scan_count, tmc::semaphore& sem) -> tmc::task<void>
                {
                    if(hist)
                    {
                        using clk = std::chrono::high_resolution_clock;
                        auto start = clk::now();
                        auto maybe_it = db->scan(make_key(lower_idx), std::nullopt, read_ahead_size);
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
                        auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                        hist->record(static_cast<uint64_t>(elapsed), tid);
                    }
                    else
                    {
                        auto maybe_it = db->scan(make_key(lower_idx), std::nullopt, read_ahead_size);
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
                    }
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
                    size_t entries = tier_min + (xorshift64(rng) % (tier_max - tier_min + 1));
                    fg.fork(do_scan(db, tier_read_ahead_size, tid, hist, lower, entries, scan_count, sem));
                }

                co_await std::move(fg);
            };

            using clk = std::chrono::high_resolution_clock;
            auto t0 = clk::now();

            std::vector<tmc::task<void>> tasks;
            tasks.reserve(num_threads);
            for(size_t tid = 0; tid < num_threads; ++tid)
                tasks.push_back(worker(tid, db, tier.min_entries, tier.max_entries, tier.read_ahead_size, hist, n_ops, num_threads, scan_count, seeds));
            run_workers(std::move(tasks));

            double elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
            size_t completed = scan_count.load();
            size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

            std::cout << "\n--- " << tier.label << " (" << n_ops << " scans) ---\n"
                      << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                      << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                      << "Keys/s:     " << static_cast<uint64_t>(completed * avg_entries / elapsed_s) << "\n";
        }
    }

} // namespace hedge::db
