#pragma once

#include <coroutine>
#include <utility>

#include "logger.h"

static int counter = 0;

template <typename PROMISE>
struct awaitable_final_suspend
{
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<PROMISE> h) noexcept
    {
        log(h.promise().id, "-task::promise::final_suspend::await_suspend on handle: ", h.address());

        if(auto c = h.promise()._continuation)
            return c;

        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

template <typename TASK, typename RETURN_VALE>
struct promise
{
    using handle_t = std::coroutine_handle<promise<TASK, RETURN_VALE>>;

    RETURN_VALE value{};
    std::coroutine_handle<> _continuation;
    int id = counter++;

    TASK get_return_object()
    {
        auto h = handle_t::from_promise(*this);
        log(id, "-task::promise::get_return_object: ", h.address(), "");
        return TASK{std::move(h)};
    }

    auto initial_suspend()
    {
        log(id, "-task::promise::initial_suspend");
        return std::suspend_always{};
    }

    auto final_suspend() noexcept
    {
        log(id, "-task::promise::final_suspend");
        return awaitable_final_suspend<promise>{};
    }

    void return_value(RETURN_VALE&& value)
    {
        log(id, "-task::promise::return_value ", value, "");
        this->value = std::move(value);
    }

    void unhandled_exception() {}
};

template <typename TASK>
struct promise<TASK, void>
{
    using handle_t = std::coroutine_handle<promise<TASK, void>>;

    std::coroutine_handle<> _continuation;
    int id = counter++;

    TASK get_return_object()
    {
        auto h = handle_t::from_promise(*this);
        log(id, "-task::promise::get_return_object: ", h.address(), "");
        return TASK{std::move(h)};
    }

    auto initial_suspend()
    {
        log(id, "-task::promise::initial_suspend");
        return std::suspend_always{};
    }

    auto final_suspend() noexcept
    {
        log(id, "-task::promise::final_suspend");
        return awaitable_final_suspend<promise>{};
    }

    void return_void()
    {
        log(id, "-task::promise::return_void");
    }

    void unhandled_exception() {}
};

template <typename RETURN_VALUE>
class task
{
    using promise_t = promise<task, RETURN_VALUE>;
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
        log(this->_handle.promise().id, "-task::await_ready");
        return false;
    }

    auto await_suspend(std::coroutine_handle<> caller_handle)
    {
        log(this->_handle.promise().id, "-task::await_suspend on handle: ", caller_handle.address(), "");
        this->_handle.promise()._continuation = caller_handle;
        return this->_handle;
    }

    RETURN_VALUE await_resume()
    {
        if constexpr(std::is_void_v<RETURN_VALUE>)
        {
            log(this->_handle.promise().id, "-task::await_resume (void)");
            return;
        }

        log(this->_handle.promise().id, "-task::await_resume");

        if(this->_handle.done())
        {
            log(this->_handle.promise().id, "-task::done");
            return std::move(this->_handle.promise().value);
        }

        return {};
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