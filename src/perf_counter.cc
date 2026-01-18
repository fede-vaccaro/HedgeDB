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

    static constexpr bool PROFILING_ENABLED = true;

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

    void counter::add(size_t c, size_t n)
    {
        if(!PROFILING_ENABLED)
            return;

        std::atomic_ref<size_t>(stat).fetch_add(c, std::memory_order::relaxed);
        std::atomic_ref<size_t>(count).fetch_add(n, std::memory_order::relaxed);
    }

    void counter::start()
    {
        if(!PROFILING_ENABLED)
            return;

        thread_local int fd_ = []()
        {
            int fd = open_perf_counter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);

            if(fd < 0)
            {
                std::string msg = "sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'"
                                  "sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'";

                throw std::runtime_error("Failed to open perf counter: " + std::string(strerror(errno)) + " . You might need to enable access to perf counters by running: \n" + msg);
            }
            return fd;
        }();

        fd = fd_;

        start_counter(fd);
    }

    void counter::stop(bool ignore)
    {
        if(!PROFILING_ENABLED)
            return;

        auto cycles = stop_counter(fd);

        if(ignore)
            return;

        this->add(cycles, 1);
    }

    void counter::reset()
    {
        stat = 0;
        count = 1;
    }

    [[nodiscard]] double counter::get() const
    {
        return (double)this->stat / (double)count;
    }

    constexpr static auto keys = std::array{
        // std::string_view{"cache_hits"},
        // std::string_view{"lookup"},
        // std::string_view{"get_slot"},
        // std::string_view{"find_in_page"},
        // std::string_view{"push_coro"},
        // std::string_view{"resumed_count"},
        // std::string_view{"avg_resumed_count"},
        // std::string_view{"gc_coros"},
        std::string_view{"merge_cache_bulk_lookup"},
        std::string_view{"merge_cache_hits"},
        std::string_view{"merge_cache_bulk_writes"},
        std::string_view{"merge_cache_bulk_write_us"},
        std::string_view{"merge_cache_bulk_writes_count"},
        std::string_view{"cache_find_frame_spins"},
        std::string_view{"coalesce_percentage"},
        std::string_view{"file_reader_next"},
        std::string_view{"consume_and_push"},
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
        size_t idx = _metrics_set.at(name);

        if(idx < _metrics_set.data.size())
            return _counters[idx].get();

        return &NOOP_COUNTER;
    }

    void print_internal_perf_stats(bool reset)
    {
        if(!PROFILING_ENABLED)
            return;

        for(size_t i = 0; i < keys.size(); ++i)
        {
            auto& counter = _counters[i];
            if(counter)
            {
                std::cout << keys[i] << ": " << counter->get() << "\n";
                if(reset)
                    counter->reset();
            }
        }
    }

} // namespace hedge::prof
