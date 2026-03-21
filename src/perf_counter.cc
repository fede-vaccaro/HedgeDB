#include "tsl/robin_map.h"
#include "types.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>

#include <errno.h>
#include <linux/perf_event.h>
#include <stdint.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "perf_counter.h"

using namespace std::literals;

/*
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'
*/

namespace hedge::prof
{

    static constexpr bool PROFILING_ENABLED = false;
    static constexpr bool TIMING_ENABLED = true;

    int open_perf_counter(uint32_t type, uint64_t config)
    {
        struct perf_event_attr pe;
        memset(&pe, 0, sizeof(struct perf_event_attr));

        pe.type = type;
        pe.size = sizeof(struct perf_event_attr);
        pe.config = config;
        pe.disabled = 1;       // start disabled
        pe.exclude_kernel = 1; // user space only
        pe.exclude_hv = 1;     // no hypervisor
        pe.exclude_idle = 0;

        // per-thread counter, any CPU
        int fd = syscall(__NR_perf_event_open, &pe, 0, -1, -1, 0);
        return fd;
    }

    void start_counter(int fd)
    {
        ioctl(fd, PERF_EVENT_IOC_RESET, 0);
        ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
    }

    long long stop_counter(int fd)
    {
        ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
        long long count = 0;
        [[maybe_unused]] int res = read(fd, &count, sizeof(long long));
        return count;
    }

    void counter::add(size_t c, size_t n)
    {
        if constexpr(!PROFILING_ENABLED)
            return;

        std::atomic_ref<size_t>(_total).fetch_add(c, std::memory_order::relaxed);
        std::atomic_ref<size_t>(_count).fetch_add(n, std::memory_order::relaxed);
    }

    void counter::start()
    {
        if constexpr(!PROFILING_ENABLED)
            return;

        // thread_local int fd_ = []()
        // {
        //     int fd = open_perf_counter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);

        //     if(fd < 0)
        //     {
        //         std::string msg = "sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'"
        //                           "sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'";

        //         throw std::runtime_error("Failed to open perf counter: " + std::string(strerror(errno)) + " . You might need to enable access to perf counters by running: \n" + msg);
        //     }
        //     return fd;
        // }();

        // fd = fd_;

        // start_counter(fd);

        auto t0 = std::chrono::high_resolution_clock::now();
        do_not_optimize(t0);
        this->_start_time = t0;
    }

    void counter::stop(bool ignore)
    {
        if constexpr(!PROFILING_ENABLED)
            return;

        // auto cycles = stop_counter(this->fd);

        if(ignore)
            return;

        this->add(0, 1);

        auto t1 = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - this->_start_time).count();
        do_not_optimize(duration);
        std::atomic_ref<std::size_t>(this->_duration).fetch_add(duration, std::memory_order::relaxed);
    }

    void counter::reset()
    {
        this->_total = 0;
        this->_count = 0;
        this->_duration = 0;
    }

    [[nodiscard]] double counter::avg() const
    {
        return (double)this->_total / (double)_count;
    }

    [[nodiscard]] double counter::total() const
    {
        return (double)this->_total;
    }

    [[nodiscard]] double counter::count() const
    {
        return (double)this->_count;
    }

    [[nodiscard]] double counter::duration_ns() const
    {
        return (double)this->_duration;
    }

    constexpr static auto keys = std::array{
        "cache_hits"sv,
        "lookup"sv,
        // "get_slot"sv,
        // "find_in_page"sv,
        // "push_coro"sv,
        // "resumed_count"sv,
        // "avg_resumed_count"sv,
        "merge_cache_bulk_lookup"sv,
        "merge_cache_hits"sv,
        "merge_cache_bulk_writes"sv,
        "merge_cache_bulk_write_us"sv,
        "merge_cache_bulk_writes_count"sv,
        "cache_find_frame_spins"sv,
        "fs_read_requests"sv,
        "file_reader_next"sv,
        "consume_and_push"sv,
        "merge_throughput_mbs"sv,
        "merge_mb_written"sv,
        "make_fs_page_from_span"sv,
        "inner_merge_loop"sv,
        "memtable_put"sv,
        "put_async"sv,
        "executor_pop_tasks"sv,
        "swap_memtable"sv,
        "test_task"sv,
        "submit_job"sv,
        "rolling_buffer::pop_front"sv,
        "qf_false_positives"sv,
        "find_value_in_sst"sv,
        "sst_visited_per_lookup"sv,
        "gc_coros"sv,
        "submit_sqe"sv,
        "wait_for_cqe"sv,
        "do_work"sv,
        "executor_launch_tasks"sv,
        // std::string_view("qf_lookups"),
    };

    static constexpr fixed_set<std::string_view, keys.size()> _metrics_set = fixed_set(keys);

    inline static std::array<std::unique_ptr<counter_i>, keys.size()> _counters = []()
    {
        std::array<std::unique_ptr<counter_i>, keys.size()> counters;
        for(auto& k : counters)
            k = std::make_unique<counter>();

        return counters;
    }();

    counter_i* _acquire_counter_ptr(std::string_view name)
    {
        if constexpr(!PROFILING_ENABLED)
            return &NOOP_COUNTER;

        size_t idx = _metrics_set.at(name);

        if(idx < _metrics_set.data.size())
            return _counters[idx].get();

        return &NOOP_COUNTER;
    }

    void print_internal_perf_stats(bool reset)
    {
        if constexpr(!PROFILING_ENABLED)
            return;

        for(size_t i = 0; i < keys.size(); ++i)
        {
            auto& counter = _counters[i];
            if(counter)
            {
                // print perf counter but prioritize average in print order
                std::cout << "Perf Counter [" << keys[i]
                          << "] - Avg: " << counter->avg()
                          << ", Count: " << counter->count()
                          << ", Total: " << counter->total()
                          << ", Avg. duration: " << (counter->duration_ns() / counter->count()) / 1000.0 << " us"
                          << ", Total duration: " << counter->duration_ns() / 1'000'000.0 << " ms\n";
                if(reset)
                    counter->reset();
            }
        }
    }

} // namespace hedge::prof
