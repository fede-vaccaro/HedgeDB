#pragma once
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace hedge::db
{
    static constexpr size_t KEY_SIZE = 24;
    static constexpr size_t NUM_WORKERS = 12;
    static constexpr uint32_t QUEUE_DEPTH = 16;
    static constexpr uint64_t KEY_SEED = 0xDEADBEEF;
    static constexpr uint64_t OP_SEED = 0x12345678;
    static constexpr size_t NVALUES = 1024;

    using values_t = std::vector<std::vector<std::byte>>;

    static constexpr size_t LATENCY_HISTOGRAM_BUCKETS = 10000; // 10k buckets → up to 100 ms range
    static constexpr uint64_t LATENCY_BUCKET_SIZE_NS = 500;    // 500 ns per bucket

    struct latency_histogram
    {
        std::vector<std::atomic<uint64_t>> buckets;
        std::atomic<uint64_t> min_ns{UINT64_MAX};
        std::atomic<uint64_t> max_ns{0};
        std::atomic<uint64_t> total_ns{0};
        std::atomic<uint64_t> count{0};

        latency_histogram() : buckets(LATENCY_HISTOGRAM_BUCKETS)
        {
            for (auto& bucket : buckets)
                bucket.store(0, std::memory_order_relaxed);
        }

        void record(uint64_t ns)
        {
            count.fetch_add(1, std::memory_order_relaxed);
            total_ns.fetch_add(ns, std::memory_order_relaxed);

            uint64_t old_min = min_ns.load(std::memory_order_relaxed);
            while (ns < old_min && !min_ns.compare_exchange_weak(old_min, ns, std::memory_order_relaxed))
                ;

            uint64_t old_max = max_ns.load(std::memory_order_relaxed);
            while (ns > old_max && !max_ns.compare_exchange_weak(old_max, ns, std::memory_order_relaxed))
                ;

            size_t bucket_idx = std::min<size_t>(ns / LATENCY_BUCKET_SIZE_NS, LATENCY_HISTOGRAM_BUCKETS - 1);
            buckets[bucket_idx].fetch_add(1, std::memory_order_relaxed);
        }

        void print_percentiles(const char* label) const
        {
            uint64_t cnt = count.load(std::memory_order_relaxed);
            if (cnt == 0)
                return;

            std::vector<uint64_t> cumulative(LATENCY_HISTOGRAM_BUCKETS);
            uint64_t running = 0;
            for (size_t i = 0; i < LATENCY_HISTOGRAM_BUCKETS; ++i)
            {
                running += buckets[i].load(std::memory_order_relaxed);
                cumulative[i] = running;
            }

            auto percentile = [&](double p) -> uint64_t
            {
                uint64_t target = static_cast<uint64_t>(std::ceil(cnt * p));
                auto it = std::lower_bound(cumulative.begin(), cumulative.end(), target);
                size_t idx = std::distance(cumulative.begin(), it);
                return idx * LATENCY_BUCKET_SIZE_NS;
            };

            uint64_t avg = total_ns.load(std::memory_order_relaxed) / cnt;
            uint64_t min_val = min_ns.load(std::memory_order_relaxed);
            uint64_t max_val = max_ns.load(std::memory_order_relaxed);

            std::cout << "\n--- " << label << " Latency ---\n"
                      << "Count:      " << cnt << "\n"
                      << "Min:        " << min_val / 1000.0 << " us\n"
                      << "Max:        " << max_val / 1'000'000.0 << " ms\n"
                      << "Avg:        " << avg / 1000.0 << " us\n"
                      << "p50:        " << percentile(0.50) / 1000.0 << " us\n"
                      << "p75:        " << percentile(0.75) / 1000.0 << " us\n"
                      << "p90:        " << percentile(0.90) / 1000.0 << " us\n"
                      << "p95:        " << percentile(0.95) / 1000.0 << " us\n"
                      << "p99:        " << percentile(0.99) / 1000.0 << " us\n"
                      << "p99.9:      " << percentile(0.999) / 1000.0 << " us\n";
        }
    };

    struct bench_config
    {
        size_t num_ops = 1'000'000;
        size_t vsize = 100;
        std::string mode = "undefined";
        std::filesystem::path db_path = "/tmp/bench_db";
        bool measure_latency = false;
    };

} // namespace hedge::db
