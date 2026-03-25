#pragma once

#include <atomic>
#include <type_traits>

#include "details/notify.h"
#include "io_executor.h"
#include "task.h"

namespace hedge::async
{

    template <typename T>
    class atomic : private detail::notifier
    {
        std::atomic<T> _value;

    public:
        atomic() = default;

        atomic(std::vector<executor_context*> ctxs, T initial = T{})
            : detail::notifier(std::move(ctxs)), _value(initial) {}

        atomic(const atomic&) = delete;
        atomic& operator=(const atomic&) = delete;

        T load(std::memory_order order = std::memory_order::seq_cst) const noexcept
        {
            return _value.load(order);
        }

        void store(T desired, std::memory_order order = std::memory_order::seq_cst) noexcept
        {
            _value.store(desired, order);
        }

        T exchange(T desired, std::memory_order order = std::memory_order::seq_cst) noexcept
        {
            return _value.exchange(desired, order);
        }

        bool compare_exchange_weak(T& expected, T desired,
                                   std::memory_order success = std::memory_order::seq_cst,
                                   std::memory_order failure = std::memory_order::seq_cst) noexcept
        {
            return _value.compare_exchange_weak(expected, desired, success, failure);
        }

        bool compare_exchange_strong(T& expected, T desired,
                                     std::memory_order success = std::memory_order::seq_cst,
                                     std::memory_order failure = std::memory_order::seq_cst) noexcept
        {
            return _value.compare_exchange_strong(expected, desired, success, failure);
        }

        T fetch_add(T arg, std::memory_order order = std::memory_order::seq_cst) noexcept
            requires std::is_integral_v<T>
        {
            return _value.fetch_add(arg, order);
        }

        T fetch_sub(T arg, std::memory_order order = std::memory_order::seq_cst) noexcept
            requires std::is_integral_v<T>
        {
            return _value.fetch_sub(arg, order);
        }

        T fetch_and(T arg, std::memory_order order = std::memory_order::seq_cst) noexcept
            requires std::is_integral_v<T>
        {
            return _value.fetch_and(arg, order);
        }

        T fetch_or(T arg, std::memory_order order = std::memory_order::seq_cst) noexcept
            requires std::is_integral_v<T>
        {
            return _value.fetch_or(arg, order);
        }

        T fetch_xor(T arg, std::memory_order order = std::memory_order::seq_cst) noexcept
            requires std::is_integral_v<T>
        {
            return _value.fetch_xor(arg, order);
        }

        using detail::notifier::notify_all;

        async::task<> wait(T old, std::memory_order order = std::memory_order::seq_cst)
        {
            this->ensure_registered();

            const auto& ex = async::this_thread_executor();

            while(_value.load(order) == old)
            {
                co_await ex->enque_coro_atomic(_value, old, order, this->_mutex_id);
            }
        }
    };

} // namespace hedge::async
