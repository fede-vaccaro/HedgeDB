#pragma once
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

/*
To enable access to counters:

sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'
*/

namespace hedge::prof
{

    template <typename Tp>
    inline __attribute__((always_inline)) void DoNotOptimize(Tp const& value)
    {
        asm volatile("" : : "r,m"(value) : "memory");
    }

    int open_perf_counter(uint32_t type, uint64_t config);

    void start_counter(int fd);

    long long stop_counter(int fd);

    struct avg_stat
    {
        std::size_t stat{0};
        std::size_t count{1};
        inline static thread_local int fd;

        void add(size_t c, size_t n = 1);

        void start();

        void stop(bool ignore = false);

        void reset();

        [[nodiscard]] double get() const;

        static tsl::robin_map<std::string, avg_stat> PERF_STATS;
    };

    void print_internal_perf_stats(bool reset = true);

} // namespace hedge::prof
