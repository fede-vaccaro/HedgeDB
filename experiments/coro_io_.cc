
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
#include <chrono>

int counter = 0;

struct req
{
    size_t x;
};

struct response
{
    size_t y;
};

class io_task
{

    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;
    handle_t _handle;

    struct Promise
    {
        size_t value;
        req r;
        size_t response = -1;

        int id = counter++;

        io_task get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            std::cout << id << "-Resumable::promise::get_return_object: " << h.address() << "\n";
            return io_task{std::move(h)};
        }

        auto initial_suspend() { 
            std::cout << id << "-Resumable::promise::initial_suspend\n";
            return std::suspend_always{}; 
        }

        auto final_suspend() noexcept 
        {
            std::cout << id << "-Resumable::promise::final_suspend\n";
            return std::suspend_always{};
        }

        void return_value(size_t value) {
            std::cout << id << "-Resumable::promise::return_value " << value << "\n";
            this->value = value;
        }

        void set_request(req r){
            this->r = r;
        }

        req get_request(){
            return this->r;
        }

        void set_response(size_t r){
            this->response = r;
        }

        auto get_response(){
            return this->response;
        }

        void unhandled_exception() {}
    };
        
    explicit io_task(handle_t h) : _handle(h) {
    }
public:
    using promise_type = Promise;
    io_task(io_task&& r) : _handle{std::exchange(r._handle, {})} {}
    ~io_task() 
    { 
        if(this->_handle)
            this->_handle.destroy(); 
        }

    bool resume()
    {
        if(!this->_handle.done())
            this->_handle.resume();
        else
            std::cout << this->_handle.promise().value << std::endl;

        return !this->_handle.done();
    }

    bool await_ready() 
    {
        std::cout << this->_handle.promise().id << "-Resumable::await_ready\n";
        return false;
    }

    void await_suspend(std::coroutine_handle<Promise> caller_handle)
    {
        std::cout << this->_handle.promise().id <<  "-Resumable::await_suspend on handle: " << caller_handle.address() << std::endl;
        return;
    }

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

    void set_io_return(size_t ret)
    {
        this->_handle.promise().set_response(ret);
    }

    req get_request()
    {
        return this->_handle.promise().r;
    }

};

template<typename PromiseType>
struct GetPromise {
  PromiseType *p_;
  bool await_ready() { return false; } // says yes call await_suspend
  bool await_suspend(std::coroutine_handle<PromiseType> h) {
    p_ = &h.promise();
    return false;     // says no don't suspend coroutine after all
  }
  PromiseType *await_resume() { return p_; }
};

struct executor {
  struct promise_type {
    bool _ready = true;
    executor get_return_object() {
      return {
        .h_ = std::coroutine_handle<promise_type>::from_promise(*this)
      };
    }
    std::suspend_never initial_suspend() { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void unhandled_exception() {}
  };

  bool await_ready() noexcept { return false; }
  void await_suspend(std::coroutine_handle<>) noexcept {}
  void await_resume() noexcept {}

  bool operator()() { if(!h_.done()) h_.resume(); return h_.done(); }

  bool ready()
  {
    return this->h_.promise()._ready;
  }

  std::coroutine_handle<promise_type> h_;
  operator std::coroutine_handle<promise_type>() const { return h_; }

  ~executor() { if(this->h_) { this->h_.destroy(); }; }

};

auto submit_request(req r)
{
    struct awaitable_req
    {
        req r;
        std::coroutine_handle<io_task::promise_type> handle;

        bool await_ready() noexcept { return false; }
        void await_suspend(std::coroutine_handle<io_task::promise_type> handle) noexcept {
            this->handle = handle;
            handle.promise().set_request(r);
        }

        size_t await_resume() noexcept {
            return handle.promise().get_response();
        }
    };

    return awaitable_req{r};
}


executor execute(std::deque<io_task>& waiting_for_io_queue, std::deque<io_task>& ready_queue)
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

        std::cout << "[executor] processing request: " << res.get_request().x << std::endl;
        
        // submit
        this_promise._ready = false;
        auto start_time = std::chrono::system_clock::now();
        
        {   
            std::cout << "[executor] yielding\n";         
            co_await std::suspend_always{}; // yield control
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - start_time);

        if (int tts = 1000 - static_cast<int>(elapsed.count()); tts >= 0)
        {   
            std::cout << "[executor] remaining " << tts << "us before completion\n";         
            std::this_thread::sleep_for(std::chrono::microseconds{tts});
        }
        // simulate work
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        this_promise._ready = true;

        // send answers
        std::cout << "[executor] pushing to ready_queue\n";
        res.set_io_return(dist(rd));
        ready_queue.push_back(std::move(res));

        std::cout << "[executor] run finished, yielding\n";         
        co_await std::suspend_always{}; 
    }
}


io_task get_obj(req r)
{
    std::cout << "[io_task] processing request: " << r.x << std::endl;

    auto response = co_await submit_request(r);

    std::cout << "[io_task] request " << r.x << " STEP-1 completed: " << r.x << " response: " << response << std::endl; 

    std::this_thread::sleep_for(std::chrono::microseconds{500});

    auto new_request = req{response + 1};

    auto new_response = co_await submit_request(new_request);

    std::this_thread::sleep_for(std::chrono::microseconds{500});

    std::cout << "[io_task] request " << r.x << " STEP-2 completed: " << new_response << std::endl;

    co_return 1;
};


int main()
{
    std::deque<io_task> requests_queue; // tasks requested, but not yet submitted
    std::deque<io_task> waiting_for_io_queue; // tasks waiting for IO
    std::deque<io_task> ready_queue; // tasks which received IO responses, but not finished yet

    auto executor = execute(waiting_for_io_queue, ready_queue);

    for(size_t i = 0; i < 3; i++)
        requests_queue.push_back(get_obj({i}));

    for(size_t i = 0; i < 20; i++)
    {

        if(!requests_queue.empty()) // start processing requests 
        {
            std::cout << "[main] processing request_queue tasks" << std::endl;
            auto task = std::move(requests_queue.front());
            requests_queue.pop_front();
            auto done = task();
            if(!done)
                waiting_for_io_queue.push_back(std::move(task));
        }
        else
        {
            std::cout << "[main] no requests queued" << std::endl;
        }

        std::cout << "[main] submitting sqe. ready: " << (executor.ready() ? "true" : "false") << std::endl;

        // assert(executor.ready()); // executor should be ready to submit sqe
        executor(); // submit sqe if any

        if(!ready_queue.empty()) // do work on tasks which received IO responses
        {
            std::cout << "[main] processing ready tasks" << std::endl;
            auto task = std::move(ready_queue.front());
            ready_queue.pop_front();
            bool done = task();
            if(!done)
                waiting_for_io_queue.push_front(std::move(task)); // tasks which started to be executed have high priority!
        }
        else
        {
            std::cout << "[main] no ready tasks" << std::endl;
        }

        std::cout << "[main] waiting for cqes" << std::endl;
        executor(); // resume executor - wait for cqes and move to ready queue

    }


}