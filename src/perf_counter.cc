#include "tsl/robin_map.h"
#include <atomic>
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

/*
sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'
*/

namespace hedge::prof
{

    static constexpr bool PROFILING_ENABLED = false;

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
    int make_counter();

    void avg_stat::add(size_t c, size_t n)
    {
        if(!PROFILING_ENABLED)
            return;

        std::atomic_ref<size_t>(stat).fetch_add(c, std::memory_order::relaxed);
        std::atomic_ref<size_t>(count).fetch_add(n, std::memory_order::relaxed);
    }

    void avg_stat::start()
    {
        if(!PROFILING_ENABLED)
            return;

        thread_local int fd_ = []()
        {
            int fd = open_perf_counter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);

            if(fd < 0)
                throw std::runtime_error("Failed to open perf counter: " + std::string(strerror(errno)));

            return fd;
        }();

        fd = fd_;

        start_counter(fd);
    }

    void avg_stat::stop(bool ignore)
    {
        if(!PROFILING_ENABLED)
            return;

        auto cycles = stop_counter(fd);
        if(ignore)
            return;
        add(cycles);
    }

    void avg_stat::reset()
    {
        stat = 0;
        count = 1;
    }

    [[nodiscard]] double avg_stat::get() const
    {
        return (double)this->stat / (double)count;
    }

    void print_internal_perf_stats(bool reset)
    {
        if(!PROFILING_ENABLED)
            return;

        for(const auto& [name, value] : avg_stat::PERF_STATS)
        {
            std::cout << "stat " << name << " : " << value.get() << std::endl;
            if(reset)
                avg_stat::PERF_STATS[name].reset();
        }
    }

    tsl::robin_map<std::string, avg_stat> avg_stat::PERF_STATS = []()
    {
        tsl::robin_map<std::string, avg_stat> initial_perfs;
        initial_perfs.emplace("cache_hits", avg_stat());
        initial_perfs.emplace("lookup", avg_stat());
        initial_perfs.emplace("get_slot", avg_stat());
        initial_perfs.emplace("find_in_page", avg_stat());
        initial_perfs.emplace("push_coro", avg_stat());
        initial_perfs.emplace("resumed_count", avg_stat());
        initial_perfs.emplace("avg_resumed_count", avg_stat());
        initial_perfs.emplace("gc_coros", avg_stat());
        return initial_perfs;
    }();

} // namespace hedge::prof
