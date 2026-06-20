#pragma once
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
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

    struct latency_collector
    {
        std::vector<std::vector<uint64_t>> per_thread;

        latency_collector(size_t n_threads, size_t ops_per_thread)
        {
            per_thread.resize(n_threads);
            for(auto& v : per_thread)
                v.reserve(ops_per_thread);
        }

        void record(uint64_t ns, size_t tid)
        {
            per_thread[tid].push_back(ns);
        }

        void print_percentiles(std::string_view label)
        {
            uint64_t cnt = 0;
            for(auto& v : per_thread)
                cnt += v.size();
            if(cnt == 0)
                return;

            std::cout << "Sorting latencies for " << label << "...\n";

            std::vector<uint64_t> all;
            all.reserve(cnt);
            uint64_t total_ns = 0;
            uint64_t min_val = UINT64_MAX;
            uint64_t max_val = 0;
            for(auto& v : per_thread)
            {
                for(auto ns : v)
                {
                    all.push_back(ns);
                    total_ns += ns;
                    min_val = std::min(ns, min_val);
                    max_val = std::max(ns, max_val);
                }
            }

            std::sort(all.begin(), all.end());

            auto percentile = [cnt, &all](double p) -> uint64_t
            {
                uint64_t idx = static_cast<uint64_t>(std::ceil(cnt * p)) - 1;
                if(idx >= cnt) idx = cnt - 1;
                return all[idx];
            };

            uint64_t avg = total_ns / cnt;

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

    struct latency_registry
    {
        std::unordered_map<std::string, std::unique_ptr<latency_collector>> collectors;

        latency_collector* get_collector(std::string label, size_t n_threads, size_t ops_per_thread)
        {
            auto it = collectors.find(label);
            if(it != collectors.end())
                return it->second.get();
            auto [iter, _] = collectors.emplace(std::move(label), std::make_unique<latency_collector>(n_threads, ops_per_thread));
            return iter->second.get();
        }

        void print_all()
        {
            for(auto& [label, collector] : collectors)
                if(collector)
                    collector->print_percentiles(label);
        }
    };

    latency_registry& get_latency_registry();

    struct bench_config
    {
        size_t num_ops = 1'000'000;
        size_t num_ts = 1;
        size_t vsize = 100;
        std::string mode = "undefined";
        std::filesystem::path db_path = "./benchdb";
        bool measure_latency = false;
        size_t num_threads = NUM_WORKERS;
        std::optional<size_t> num_bg_threads;
        bool print_stats = false;
    };

} // namespace hedge::db
