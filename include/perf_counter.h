#pragma once
#include "tsl/robin_map.h"
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string>

#include <chrono>
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
    inline __attribute__((always_inline)) void do_not_optimize(Tp const& value)
    {
        asm volatile("" : : "r,m"(value) : "memory");
    }

    struct counter_i
    {
        virtual void add(size_t c, size_t n = 1) = 0;
        virtual void start() = 0;
        virtual void stop(bool ignore = false) = 0;
        virtual void reset() = 0;
        [[nodiscard]] virtual double avg() const = 0;
        [[nodiscard]] virtual double total() const = 0;
        [[nodiscard]] virtual double count() const = 0;
        [[nodiscard]] virtual double duration_ns() const { return 0.0; }
        virtual ~counter_i() = default;
    };

    struct counter final : counter_i
    {
        std::size_t _total{0};
        std::size_t _count{0};
        std::size_t _duration{0};
        std::chrono::high_resolution_clock::time_point _start_time{};
        // inline static thread_local int fd;

        void add(size_t c, size_t n) final;

        void start() final;

        void stop(bool ignore) final;
        void reset() final;

        [[nodiscard]] double avg() const final;
        [[nodiscard]] double total() const final;
        [[nodiscard]] double count() const final;
        [[nodiscard]] double duration_ns() const final;
    };

    struct noop_counter final : counter_i
    {
        void add(size_t /*n*/, size_t /*c*/) final {}

        void start() final {}

        void stop(bool /*ignore*/) final {}
        void reset() final {}

        [[nodiscard]] double avg() const final { return 0.0; }
        [[nodiscard]] double total() const final { return 0.0; }
        [[nodiscard]] double count() const final { return 0.0; }
        [[nodiscard]] double duration_ns() const final { return 0.0; }
    };

    inline static noop_counter NOOP_COUNTER{};

    template <size_t N>
    struct fixed_string
    {
        char data[N];
        constexpr fixed_string(const char (&str)[N])
        {
            for(size_t i = 0; i < N; ++i)
                data[i] = str[i];
        }
        constexpr operator std::string_view() const { return {data, N - 1}; }
    };

    template <typename K, size_t Size>
    struct fixed_set
    {
        std::array<K, Size> data;

        constexpr auto find(const K& key) const
        {
            return std::find(data.begin(), data.end(), key);
        }

        constexpr bool contains(const K& key) const
        {
            return find(key) != data.end();
        }

        [[nodiscard]] constexpr size_t at(std::string_view k) const
        {
            return std::distance(data.begin(), find(k));
        }
    };

    template <fixed_string name>
    static counter_i& get()
    {
        static counter_i* instance = _acquire_counter_ptr(name);

        return *instance;
    }

    counter_i* _acquire_counter_ptr(std::string_view name);

    void print_internal_perf_stats(bool reset = true);

} // namespace hedge::prof
