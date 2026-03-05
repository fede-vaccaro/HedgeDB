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
#include "async/task.h"
#include "async/wait_group.h"
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

// ---------------------------------------------------------------------------
// Coroutine task — one per insert
// ---------------------------------------------------------------------------

static const uint8_t DUMMY_VALUE[100]{};

hedge::async::task<void> make_put_task(hedge::db::memtable* mt, size_t i,
                                       hedge::async::wait_group* wg)
{
    mt->put(make_key(i),
            std::span<const uint8_t>(DUMMY_VALUE, sizeof(DUMMY_VALUE)),
            hedge::value_type::IN_PLACE_VALUE);
    wg->decr();
    co_return;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
    constexpr size_t N = 80'000'000;
    constexpr size_t N_EXECUTORS = 8;
    constexpr uint32_t QUEUE_DEPTH = 64;

    hedge::async::executor_pool::init_static_pool(N_EXECUTORS, QUEUE_DEPTH);

    hedge::db::memtable_config cfg;
    cfg.memory_budget_cap = 32UL * 1024 * 1024; // 512 MB — no flush at 500K keys
    cfg.auto_compaction = false;
    cfg.use_odirect = true;
    cfg.num_writer_threads = N_EXECUTORS; // avoid contention on rw_sync writers
    cfg.flush_io_workers = 6;

    static std::atomic_size_t flush_epoch{0};

    std::filesystem::remove_all("./indices");

    hedge::db::memtable mt(
        cfg,
        /*num_partition_exponent=*/4,
        /*indices_path=*/"./indices",
        &flush_epoch,
        /*push_new_indices=*/[](std::vector<hedge::db::sst>) {},
        /*trigger_compaction_callback=*/[] {},
        /*page_cache=*/nullptr);

    auto wg = hedge::async::wait_group::make_shared();
    wg->set(N);

    auto t0 = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N; ++i)
    {
        auto task = make_put_task(&mt, i, wg.get());
        hedge::async::executor_pool::executor_from_static_pool()->submit_io_task(std::move(task));
    }

    wg->wait();

    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_s = std::chrono::duration<double>(t1 - t0).count();
    double throughput = N / elapsed_s;
    double bandwidth_mb = (throughput * (sizeof(DUMMY_VALUE))) / (1000.0 * 1000.0);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Inserted " << N << " keys via executor in " << elapsed_s * 1000.0 << " ms\n";
    std::cout << "Throughput: " << static_cast<size_t>(throughput) << " items/s\n";
    std::cout << "Bandwidth:  " << bandwidth_mb << " MB/s\n";

    mt.wait_for_flush().wait();
}
