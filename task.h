#pragma once

#include <cassert>
#include <coroutine>
#include <cstdint>
#include <utility>

#include <logger.h>

static int counter = 0;

namespace hedgehog::async
{
    template <typename PROMISE>
    struct awaitable_final_suspend
    {
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PROMISE> h) noexcept
        {
            if(auto c = h.promise()._continuation)
                return c;

            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    template <typename TASK, typename RETURN_VALUE>
    struct task_promise
    {
        using handle_t = std::coroutine_handle<task_promise<TASK, RETURN_VALUE>>;

        uint8_t _data[sizeof(RETURN_VALUE)];
        RETURN_VALUE* _value;
        std::coroutine_handle<> _continuation;

        int id = counter++;

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
            this->_value = new(this->_data) RETURN_VALUE{std::move(value)};
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
        int id = counter++;

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

    template <typename RETURN_VALUE>
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
        task(task&& r) : _handle{std::exchange(r._handle, {})} {}
        ~task()
        {
            if(this->_handle)
                this->_handle.destroy();
        }

        task(const task&) = delete;
        task& operator=(const task&) = delete;

        task& operator=(task&& r)
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

        auto await_suspend(std::coroutine_handle<> caller_handle)
        {
            this->_handle.promise()._continuation = caller_handle;
            return this->_handle;
        }

        RETURN_VALUE await_resume()
        {
            if constexpr(std::is_void_v<RETURN_VALUE>)
                return;

            if(!this->_handle.done())
                assert(false);

            return std::move(*this->_handle.promise()._value);
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
    };

} // namespace hedgehog::async