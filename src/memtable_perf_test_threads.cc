// Benchmark: raw-thread overhead on memtable::put().
//
// Mirrors memtable_perf_test but replaces the executor with N plain std::threads.
// Thread t inserts key i when (i % N_THREADS == t).

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <span>
#include <thread>
#include <vector>

// Macros required by memtable.h (pulled in via Folly's ConcurrentSkipList)
struct _NullStream
{
    template <typename T>
    _NullStream& operator<<(const T&) { return *this; }
};
#ifndef CHECK_EQ
#define CHECK_EQ(a, b) _NullStream()
#endif
#ifndef DCHECK
#define DCHECK(x) _NullStream()
#endif
#ifndef DCHECK_GT
#define DCHECK_GT(a, b) _NullStream()
#endif
#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) _NullStream()
#endif
#ifndef CHECK_LE
#define CHECK_LE(a, b) _NullStream()
#endif
#ifndef FOLLY_UNLIKELY
#define FOLLY_UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif
#ifndef FOLLY_LIKELY
#define FOLLY_LIKELY(x) __builtin_expect(!!(x), 1)
#endif

#include "async/io_executor.h"
#include "db/memtable.h"
#include "types.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static hedge::key_t make_key(uint64_t)
{
    std::array<uint8_t, 24> bytes{};
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint8_t> dist;
    for(auto& byte : bytes)
        byte = dist(rng);
    return hedge::key_t(bytes);
}

static const std::vector<uint8_t> DUMMY_VALUE = []()
{
    std::vector<uint8_t> dummy(100);
    std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint8_t> dist;
    for(auto& byte : dummy)
        byte = dist(rng);
    return dummy;
}();

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
    constexpr size_t N = 50'000'000;
    constexpr size_t N_THREADS = 20;

    hedge::db::memtable_config cfg;
    cfg.memory_budget_cap = 64 * 1024 * 1024;
    cfg.auto_compaction = false;
    cfg.use_odirect = true;
    cfg.use_wal = false;
    cfg.num_writer_threads = N_THREADS;

    static std::atomic_size_t flush_epoch{0};

    std::filesystem::remove_all("/tmp/indices_test_threads");
    std::filesystem::create_directories("/tmp/indices_test_threads");

    std::vector<std::shared_ptr<hedge::async::executor_context>> flush_pool(4);
    for(size_t i = 0; i < flush_pool.size(); ++i)
    {
        flush_pool[i] = hedge::async::executor_context::make_new(32, hedge::async::executor_config{
                                                                          .loops_before_sleeping = 0,
                                                                          .loops_before_yielding = 0});
        flush_pool[i]->set_thread_name("flush-" + std::to_string(i));
    }

    auto writer_executors_shared = hedge::async::executor_pool::static_pool().executors();
    std::vector<hedge::async::executor_context*> writer_executors;
    writer_executors.reserve(writer_executors_shared.size());
    for(auto& ex : writer_executors_shared)
        writer_executors.push_back(ex.get());

    hedge::db::memtable mt(
        cfg,
        /*num_partition_exponent=*/4,
        /*indices_path=*/"/tmp/indices_test_threads", &flush_epoch,
        /*push_new_indices=*/[](std::vector<hedge::db::sst>) {},
        /*trigger_compaction_callback=*/[] {},
        /*page_cache=*/nullptr,
        /*flush_executor_pool=*/std::move(flush_pool),
        /*writer_executors=*/std::move(writer_executors));

    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(N_THREADS);

    for(size_t tid = 0; tid < N_THREADS; ++tid)
    {
        threads.emplace_back([&mt, tid]()
        {
            // hedge::async::executor_context::set_affinity(tid).resume();
            for(size_t i = tid; i < N; i += N_THREADS)
            {
                mt.put(make_key(i), DUMMY_VALUE, hedge::value_type::IN_PLACE_VALUE);
            }
        });
    }

    for(auto& t : threads)
        t.join();

    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
    double throughput = N / elapsed_s;
    double bandwidth_mb = (throughput * (DUMMY_VALUE.size() + sizeof(hedge::key_t))) / (1000.0 * 1000.0);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Inserted " << N << " keys via threads in " << elapsed_s * 1000.0 << " ms\n";
    std::cout << "Throughput: " << static_cast<size_t>(throughput) << " items/s\n";
    std::cout << "Bandwidth:  " << bandwidth_mb << " MB/s\n";
    std::cout << "Backpressure count: " << hedge::db::memtable::BACKPRESSURE.load() << "\n";

    mt.wait_for_flush().wait();
}
