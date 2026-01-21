#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <utility>

/*
    task<T> Implementation

    Based on the cppcoro (https://github.com/lewissbaker/cppcoro) task implementation with some modifications.
*/
namespace hedge::async
{
    template <typename PROMISE>
    struct awaitable_final_suspend
    {
        [[nodiscard]] bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PROMISE> h) noexcept
        {
            if(auto c = h.promise()._continuation)
                return c;
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    template <typename TASK, typename RETURN_VALUE = void>
    struct task_promise
    {
        using handle_t = std::coroutine_handle<task_promise<TASK, RETURN_VALUE>>;

        alignas(alignof(RETURN_VALUE)) std::array<uint8_t, sizeof(RETURN_VALUE)> _data;
        std::coroutine_handle<> _continuation;
        std::coroutine_handle<> _root_coro{nullptr};

        TASK get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            return TASK{std::move(h)};
        }

        auto initial_suspend()
        {
            return std::suspend_always{};
        }

        auto final_suspend() noexcept
        {
            return awaitable_final_suspend<task_promise>{};
        }

        void return_value(RETURN_VALUE&& value)
        {
            new(this->_data.data()) RETURN_VALUE{std::move(value)};
        }

        void unhandled_exception()
        {
            throw;
        }
    };

    template <typename TASK>
    struct task_promise<TASK, void>
    {
        using handle_t = std::coroutine_handle<task_promise<TASK, void>>;

        std::coroutine_handle<> _continuation;
        std::coroutine_handle<> _root_coro;

        TASK get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            return TASK{std::move(h)};
        }

        auto initial_suspend()
        {
            return std::suspend_always{};
        }

        auto final_suspend() noexcept
        {
            return awaitable_final_suspend<task_promise>{};
        }

        void return_void()
        {
        }

        void unhandled_exception() { throw; }
    };

    template <typename RETURN_VALUE = void>
    class task
    {
        using promise_t = task_promise<task, RETURN_VALUE>;
        using handle_t = std::coroutine_handle<promise_t>;
        handle_t _handle;

        explicit task(handle_t h) : _handle(h)
        {
        }

        friend promise_t;

    public:
        using promise_type = promise_t;
        task(task&& r) noexcept : _handle{std::exchange(r._handle, {})} {}
        ~task()
        {
            if(this->_handle)
                this->_handle.destroy();
        }

        task(const task&) = delete;
        task& operator=(const task&) = delete;

        task& operator=(task&& r) noexcept
        {
            if(this != &r)
            {
                if(this->_handle)
                    this->_handle.destroy();

                this->_handle = std::exchange(r._handle, {});
            }
            return *this;
        }

        bool resume()
        {
            return (*this)();
        }

        bool await_ready()
        {
            return false;
        }

        template <typename PROMISE_TYPE>
        auto await_suspend(std::coroutine_handle<PROMISE_TYPE> caller_handle)
        {
            this->_handle.promise()._continuation = caller_handle;
            this->_handle.promise()._root_coro = caller_handle.promise()._root_coro
                                                     ? caller_handle.promise()._root_coro
                                                     : caller_handle;
            return this->_handle;
        }

        template <typename U = RETURN_VALUE>
        RETURN_VALUE await_resume()
            requires(!std::is_void_v<RETURN_VALUE>)
        {
            if(!this->_handle.done())
                assert(false);

            return std::move(*reinterpret_cast<RETURN_VALUE*>(this->_handle.promise()._data.data()));
        }

        template <typename U = RETURN_VALUE>
        void await_resume()
            requires(std::is_void_v<RETURN_VALUE>)
        {
            if(!this->_handle.done())
                assert(false);
        }

        bool operator()()
        {
            if(!this->_handle.done())
                this->_handle.resume();

            return this->_handle.done();
        }

        [[nodiscard]] bool done() const
        {
            return this->_handle.done();
        }

        [[nodiscard]] std::coroutine_handle<> handle() const
        {
            return this->_handle;
        }
    };

} // namespace hedge::async

namespace std
{
    template <typename T>
    struct hash<hedge::async::task<T>>
    {
        size_t operator()(const auto& task) const noexcept
        {
            return std::hash<std::coroutine_handle<>>{}(task.handle());
        }
    };
} // namespace std
