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
            executor->post(std::move(h), 0, thread_hint); // NOLINT(performance-move-const-arg)
        }
        void await_resume() const noexcept {}
    };

    struct scan_tier
    {
        const char* label;
        size_t min_entries;
        size_t max_entries;
        size_t read_ahead_size;
        size_t op_dividend; // number of ops to divide n by to determine number of scans for this tier
    };

    void run_range(const std::shared_ptr<database>& db, size_t n, size_t num_threads, bool measure_latency)
    {
        static constexpr std::array tiers = {
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            scan_tier{.label = "small  (1 - 100)", .min_entries = 1, .max_entries = 100, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "medium (512 - 1024)", .min_entries = 512, .max_entries = 1024, .read_ahead_size = 4 * KiB, .op_dividend = 1000},
            // scan_tier{.label = "large  (114688 - 131072)", .min_entries = 114688, .max_entries = 131072, .read_ahead_size = 64 * KiB, .op_dividend = 10000},
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
            std::unique_ptr<latency_histogram> hist;
            if(measure_latency)
                hist = std::make_unique<latency_histogram>();
            latency_histogram* hist_ptr = hist.get();

            auto worker = [](size_t tid, size_t n_ops, size_t num_threads, uint64_t seed,
                             const std::shared_ptr<database>& db, scan_tier tier,
                             std::atomic_size_t& scan_count, bool measure_latency, latency_histogram* hist) -> tmc::task<void>
            {
                auto do_scan = [](
                                   const std::shared_ptr<database>& db,
                                   size_t read_ahead_size,
                                   size_t lower_idx,
                                   size_t entries,
                                   std::atomic_size_t& count,
                                   tmc::semaphore& sem,
                                   bool measure_latency,
                                   latency_histogram* hist) -> tmc::task<void>
                {
                    using clk = std::chrono::high_resolution_clock;
                    if(measure_latency && hist)
                    {
                        auto start = clk::now();
                        auto maybe_it = db->scan(make_key(lower_idx), std::nullopt, read_ahead_size);
                        auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - start).count();
                        hist->record(static_cast<uint64_t>(elapsed));
                        if(maybe_it)
                        {
                            auto it = std::move(maybe_it.value());
                            for(size_t i = 0; i < entries; ++i)
                            {
                                if(!(co_await it.next()))
                                    break;
                            }
                            count.fetch_add(1, std::memory_order_relaxed);
                        }
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
                            count.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    sem.release();
                };

                auto fg = tmc::fork_group();
                tmc::semaphore sem(io::static_pool::instance()->queue_depth());
                tmc::ex_any* executor = io::static_pool::instance()->ex().type_erased();
                uint64_t rng = seed;

                for(size_t op = tid; op < n_ops; op += num_threads)
                {
                    co_await sem;
                    // co_await pin_to_thread{executor, tid}; // Hack around tmc for better thread-locality
                    size_t lower = xorshift64(rng) % n_ops;
                    size_t entries = tier.min_entries + (xorshift64(rng) % (tier.max_entries - tier.min_entries + 1));
                    fg.fork(do_scan(db, tier.read_ahead_size, lower, entries, scan_count, sem, measure_latency, hist));
                }

                co_await std::move(fg);
            };

            using clk = std::chrono::high_resolution_clock;
            auto t0 = clk::now();

            std::vector<tmc::task<void>> tasks;
            tasks.reserve(num_threads);
            for(size_t tid = 0; tid < num_threads; ++tid)
                tasks.push_back(worker(tid, n_ops, num_threads, seeds[tid], db, tier, scan_count, measure_latency, hist_ptr));
            run_workers(std::move(tasks));

            double elapsed_s = std::chrono::duration<double>(clk::now() - t0).count();
            size_t completed = scan_count.load();
            size_t avg_entries = (tier.min_entries + tier.max_entries) / 2;

            std::cout << "\n--- " << tier.label << " (" << n_ops << " scans) ---\n"
                      << "Duration:   " << elapsed_s * 1000.0 << " ms\n"
                      << "Scans/s:    " << static_cast<uint64_t>(completed / elapsed_s) << "\n"
                      << "Keys/s:     " << static_cast<uint64_t>(completed * avg_entries / elapsed_s) << "\n";
            if(hist)
            {
                std::string label = std::string("seek (range ") + tier.label + ")";
                hist->print_percentiles(label.c_str());
            }
            std::this_thread::sleep_for(std::chrono::seconds{1});
        }
    }

} // namespace hedge::db
