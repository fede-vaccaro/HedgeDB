
#include <bits/types/timer_t.h>

#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <random>
#include <ratio>
#include <thread>
#include <utility>
#include <variant>

int counter = 0;

struct request
{
    size_t x;
};

struct response
{
    size_t y;
};

class task
{
    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;
    handle_t _handle;

    struct Promise
    {
        size_t value;
        request r;
        size_t response = -1;

        int id = counter++;

        task get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            std::cout << id
                      << "-Resumable::promise::get_return_object: " << h.address()
                      << "\n";
            return task{std::move(h)};
        }

        auto initial_suspend()
        {
            std::cout << id << "-Resumable::promise::initial_suspend\n";
            return std::suspend_always{};
        }

        auto final_suspend() noexcept
        {
            std::cout << id << "-Resumable::promise::final_suspend\n";
            return std::suspend_always{};
        }

        void return_value(size_t value)
        {
            std::cout << id << "-Resumable::promise::return_value " << value << "\n";
            this->value = value;
        }

        void unhandled_exception()
        {
            try
            {
                throw; // Re-throw to catch the actual exception
            }
            catch(const std::exception& e)
            {
                std::cerr << "Coroutine " << id << " unhandled exception: " << e.what() << std::endl;
            }
            catch(...)
            {
                std::cerr << "Coroutine " << id << " unhandled unknown exception!" << std::endl;
            }
            // Optionally, mark the coroutine as errored or set a flag
        }
    };

    explicit task(handle_t h)
        : _handle(h)
    {
    }

public:
    using promise_type = Promise;
    task(task&& r)
        : _handle{std::exchange(r._handle, {})}
    {
    }
    ~task()
    {
        if(this->_handle)
            this->_handle.destroy();
    }

    bool await_ready()
    {
        std::cout << this->_handle.promise().id << "-Resumable::await_ready\n";
        return false;
    }

    void await_suspend(std::coroutine_handle<Promise> caller_handle)
    {
        std::cout << this->_handle.promise().id
                  << "-Resumable::await_suspend on handle: "
                  << caller_handle.address() << std::endl;
        return;
    }

    auto id() const { return this->_handle.promise().id; }

    size_t await_resume()
    {
        std::cout << this->_handle.promise().id << "-Resumable::await_resume\n";

        if(this->_handle.done())
        {
            std::cout << this->_handle.promise().id << "-Resumable::done\n";
            return this->_handle.promise().value;
        }

        return 0;
    }

    bool operator()()
    {
        if(!this->_handle.done())
            this->_handle.resume();

        return this->_handle.done();
    }

    bool done() const { return this->_handle && this->_handle.done(); }
};

template <typename PromiseType>
struct GetPromise
{
    PromiseType* p_;
    bool await_ready() { return false; } // says yes call await_suspend
    bool await_suspend(std::coroutine_handle<PromiseType> h)
    {
        p_ = &h.promise();
        return false; // says no don't suspend coroutine after all
    }
    PromiseType* await_resume() { return p_; }
};

struct executor
{
    struct promise_type
    {
        bool _ready = true;
        executor get_return_object()
        {
            return {.h_ = std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
    };

    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept {}
    void await_resume() noexcept {}

    bool operator()()
    {
        if(!h_.done())
            h_.resume();
        return h_.done();
    }

    bool ready() { return this->h_.promise()._ready; }

    std::coroutine_handle<promise_type> h_;
    operator std::coroutine_handle<promise_type>() const { return h_; }

    ~executor()
    {
        if(this->h_)
        {
            this->h_.destroy();
        };
    }
};

struct mailbox
{
    std::variant<std::monostate, request, response> _msg;
    std::coroutine_handle<> _continuation;

    auto submit_request(request r)
    {
        struct awaitable_mailbox
        {
            mailbox& mbox;

            bool await_ready() noexcept { return false; }

            void await_suspend(
                std::coroutine_handle<task::promise_type> handle) noexcept
            {
                std::cout << "[mailbox] await_suspend called, setting continuation: "
                          << handle.address() << "\n";
                mbox._continuation = handle;

                auto casted_continuation = std::coroutine_handle<task::promise_type>::from_address(
                    mbox._continuation.address());
                std::cout << "[mailbox] submit_request called with "
                          << mbox.get_request().x << " from "
                          << casted_continuation.promise().id << "\n";
            }

            response await_resume() noexcept
            {
                assert(std::holds_alternative<response>(mbox._msg) && "await_resume called before response was set");
                return std::move(std::get<response>(mbox._msg));
            }

            awaitable_mailbox(mailbox& m)
                : mbox(m)
            {
            }
        };

        _msg = std::move(r);
        return awaitable_mailbox{*this};
    }

    request get_request()
    {
        assert(std::holds_alternative<request>(_msg) && "get_request called before request was set");
        return std::get<request>(_msg);
    }

    auto set_response(response r)
    {
        std::cout << "[mailbox] set_response called with " << r.y << "\n";
        assert(std::holds_alternative<request>(_msg) && "set_response called before request was set");
        _msg = r;
    }

    auto resume()
    {
        std::cout << "[mailbox] resume called on _continuation: "
                  << this->_continuation.address() << " \n";

        if(this->_continuation.done())
            return true;

        assert(_continuation && !this->_continuation.done() && "resume called without a continuation set");

        this->_continuation.resume();

        return this->_continuation.done();
    }
};

using task_mailbox = std::pair<task, std::unique_ptr<mailbox>>;

class executor_context
{
public:
    executor execute(std::deque<task_mailbox>& waiting_for_io_queue,
                     std::deque<task_mailbox>& ready_queue)
    {
        std::mt19937 rd{};
        std::uniform_int_distribution<size_t> dist(0, 255);

        auto& this_promise = *(co_await GetPromise<executor::promise_type>());

        while(true)
        {
            std::cout << "[executor] start executor loop\n";

            while(waiting_for_io_queue.empty())
            {
                std::cout << "[executor] no requests, yielding\n";
                co_await std::suspend_always{};
            }

            auto res = std::move(waiting_for_io_queue.front());
            waiting_for_io_queue.pop_front();

            std::cout << "[executor] processing request: "
                      << res.second->get_request().x << " for: " << res.first.id()
                      << std::endl;

            // submit
            this_promise._ready = false;
            auto start_time = std::chrono::system_clock::now();

            {
                std::cout << "[executor] yielding\n";
                co_await std::suspend_always{}; // yield control
            }

            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - start_time);

            if(int tts = 1000 - static_cast<int>(elapsed.count()); tts >= 0)
            {
                std::cout << "[executor] remaining " << tts << "us before completion\n";
                std::this_thread::sleep_for(std::chrono::microseconds{tts});
            }
            // simulate work
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            this_promise._ready = true;

            // send answers
            std::cout << "[executor] pushing " << res.first.id()
                      << " to ready_queue\n";
            res.second->set_response(response{dist(rd)});
            ready_queue.push_back(std::move(res));

            std::cout << "[executor] run finished, yielding\n";
            co_await std::suspend_always{};
        }
    }
};

task get_obj(request r, mailbox& mbox)
{
    std::cout << "[io_task] processing request: " << r.x << std::endl;

    auto response = co_await mbox.submit_request(r);

    std::cout << "[io_task] request " << r.x << " STEP-1 completed: " << r.x
              << " response: " << response.y << std::endl;

    std::this_thread::sleep_for(std::chrono::microseconds{500});

    auto new_request = request{response.y + 1};

    auto new_response = co_await mbox.submit_request(new_request);

    std::this_thread::sleep_for(std::chrono::microseconds{500});

    std::cout << "[io_task] request " << r.x
              << " STEP-2 completed: " << new_response.y << std::endl;

    co_return 1;
};

int main()
{
    std::deque<task_mailbox> requests_queue;       // tasks requested, but not yet submitted
    std::deque<task_mailbox> waiting_for_io_queue; // tasks waiting for IO
    std::deque<task_mailbox> ready_queue;          // tasks which received IO responses, but not finished yet

    executor_context context;
    auto executor = context.execute(waiting_for_io_queue, ready_queue);

    for(size_t i = 0; i < 3; i++)
    {
        auto mbox = std::make_unique<mailbox>();
        auto task = get_obj(request{i}, *mbox);
        requests_queue.emplace_back(std::move(task), std::move(mbox));
    }

    for(size_t i = 0; i < 20; i++)
    {
        if(!requests_queue.empty()) // start processing requests
        {
            std::cout << "[main] processing request_queue tasks" << std::endl;
            auto [task, mbox] = std::move(requests_queue.front());
            requests_queue.pop_front();

            if(!task())
                waiting_for_io_queue.push_back({std::move(task), std::move(mbox)});
        }
        else
        {
            std::cout << "[main] no requests queued" << std::endl;
        }

        std::cout << "[main] submitting sqe. ready: "
                  << (executor.ready() ? "true" : "false") << std::endl;

        // assert(executor.ready()); // executor should be ready to submit sqe
        executor(); // submit sqe if any

        if(!ready_queue.empty()) // do work on tasks which received IO responses
        {
            std::cout << "[main] processing ready tasks" << std::endl;
            auto [task, mbox] = std::move(ready_queue.front());
            ready_queue.pop_front();
            std::cout << "[main] resuming task/mailbox: " << task.id() << std::endl;
            mbox->resume();
            if(!task.done())
                waiting_for_io_queue.emplace_front(
                    std::move(task),
                    std::move(mbox)); // tasks which started to be
                                      // executed have high priority!
        }
        else
        {
            std::cout << "[main] no ready tasks" << std::endl;
        }

        std::cout << "[main] waiting for cqes" << std::endl;
        executor(); // resume executor - wait for cqes and move to ready queue
    }
}