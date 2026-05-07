// Benchmark: executor overhead on memtable::put().
//
// Mirrors skiplist_executor_test but uses the real memtable class, adding:
//   - rw_sync::acquire_writer / release
//   - Per-thread value arena allocation
//
// Breakdown:
//   skiplist_executor_test : executor + raw skiplist insert
//   memtable_perf_test     : executor + memtable::put() (this file)
//   database_test benchmark: all of the above + put_async coroutine + value_table writes

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <span>
#include <vector>

// Macros required by memtable.h (pulled in via Folly's ConcurrentSkipList)
struct _NullStream
{
    template <typename T>
    _NullStream& operator<<(const T& /**/) { return *this; }
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

#include "db/memtable.h"
#include "io/io_ctx.h"
#include "tmc/ex_cpu.hpp"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"
#include "types.h"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static hedge::key_t make_key(uint64_t /**/)
{
    std::array<std::byte, 24> bytes{};
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    for(auto& byte : bytes)
        byte = static_cast<std::byte>(dist(rng));
    return hedge::key_t(bytes);
}

// ---------------------------------------------------------------------------
// Coroutine task — one per thread
// ---------------------------------------------------------------------------

static const std::vector<std::byte> DUMMY_VALUE = []()
{
    std::vector<std::byte> dummy(100);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    for(auto& byte : dummy)
        byte = static_cast<std::byte>(dist(rng));
    return dummy;
}();

tmc::task<void>
make_put_task(hedge::db::memtable* mt, size_t tid, size_t N, size_t N_EXECUTORS)
{
    for(size_t i = tid; i < N; i += N_EXECUTORS)
    {
        co_await mt->put_async(make_key(i),
                               DUMMY_VALUE,
                               hedge::value_type::IN_PLACE_VALUE);
    }
    co_return;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
constexpr size_t N = 50'000'000;
constexpr size_t N_EXECUTORS = 8;
constexpr uint32_t QD = 64;
constexpr size_t FLUSH_THREADS = 4;

int main()
{
    // --- Main threadpool (runs put_async coroutines) ---
    auto main_pool = std::make_shared<hedge::io::io_executor>(
        hedge::io::executor_config{
            .queue_depth = QD,
            .n_threads = N_EXECUTORS,
            .auto_detect = false,
        });
    auto flusher_pool = std::make_shared<hedge::io::io_executor>(
        hedge::io::executor_config{
            .queue_depth = QD / 2,
            .n_threads = FLUSH_THREADS,
            .auto_detect = false,
        });

    // --- Memtable setup ---
    hedge::db::memtable_config cfg;
    cfg.memory_budget_cap = 64 * 1024 * 1024;
    cfg.auto_compaction = false;
    cfg.use_odirect = true;
    cfg.use_wal = true;
    cfg.num_writer_threads = N_EXECUTORS;
    // cfg.flush_io_workers = FLUSH_THREADS;

    static std::atomic_size_t flush_epoch{0};

    std::filesystem::remove_all("/tmp/indices_test");
    std::filesystem::create_directories("/tmp/indices_test");

    hedge::db::memtable mt(
        cfg,
        /*num_partition_exponent=*/4,
        /*indices_path=*/"/tmp/indices_test",
        &flush_epoch,
        flusher_pool,
        /*push_new_indices=*/[](std::vector<hedge::db::sst> /**/) -> tmc::task<void>
        { co_return; },
        /*trigger_compaction_callback=*/[] {},
        /*page_cache=*/nullptr);

    // --- Run benchmark ---
    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<std::future<void>> futures;
    futures.reserve(N_EXECUTORS);
    for(size_t tid = 0; tid < N_EXECUTORS; ++tid)
    {
        futures.push_back(tmc::post_waitable(*main_pool,
                                             make_put_task(&mt, tid, N, N_EXECUTORS),
                                             0, tid));
    }

    for(auto& f : futures)
        f.get();

    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
    double throughput = N / elapsed_s;
    double bandwidth_mb = (throughput * (DUMMY_VALUE.size() + sizeof(hedge::key_t))) / (1000.0 * 1000.0);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Inserted " << N << " keys via executor in " << elapsed_s * 1000.0 << " ms\n";
    std::cout << "Throughput: " << static_cast<size_t>(throughput) << " items/s\n";
    std::cout << "Bandwidth:  " << bandwidth_mb << " MB/s\n";
    std::cout << "Backpressure count: " << hedge::db::memtable::HALT_COUNTER.load() << "\n";

    mt.wait_for_flush().wait();
}
