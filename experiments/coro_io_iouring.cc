#include <bits/types/timer_t.h>

#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <liburing.h>
#include <random>
#include <ratio>
#include <sstream> // Required for std::ostringstream
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>

#include "logger.h"

using namespace std::string_literals;

int counter = 0;

struct request
{
    int fd;
    size_t offset;
    size_t size;
};

struct response
{
    std::unique_ptr<uint8_t> data; // pointer to the data read
    size_t data_size;
};

class task
{
    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;
    handle_t _handle;

    struct Promise
    {

        int id = counter++;

        task get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            log(id, "-Resumable::promise::get_return_object: ", h.address());
            return task{std::move(h)};
        }

        auto initial_suspend()
        {
            log(id, "-Resumable::promise::initial_suspend");
            return std::suspend_always{};
        }

        auto final_suspend() noexcept
        {
            log(id, "-Resumable::promise::final_suspend");
            return std::suspend_always{};
        }

        void return_void()
        {
            log(id, "-Resumable::promise::return_void");
        }

        void unhandled_exception()
        {
            throw;
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
        log(this->_handle.promise().id, "-Resumable::await_ready");
        return false;
    }

    void await_suspend(std::coroutine_handle<Promise> caller_handle)
    {
        log(this->_handle.promise().id, "-Resumable::await_suspend on handle: ", caller_handle.address());
        return;
    }

    auto id() const { return this->_handle.promise().id; }

    void await_resume()
    {
        log(this->_handle.promise().id, "-Resumable::await_resume");

        if(this->_handle.done())
        {
            log(this->_handle.promise().id, "-Resumable::done");
        }
    }

    bool operator()()
    {
        if(!this->_handle.done())
            this->_handle.resume();

        return this->_handle.done();
    }

    bool done() const { return this->_handle && this->_handle.done(); }
};

std::pair<int, size_t> open_fd(const std::string& path, bool direct = true)
{
    auto flag = O_RDONLY;
    if(direct)
        flag |= O_DIRECT;

    int fd = open(path.c_str(), flag);
    if(fd < 0)
    {
        throw std::runtime_error("couldnt open"s + path);
    }

    auto size = std::filesystem::file_size(path);
    return {fd, size};
}

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

const std::string INDEX_PATH = "/tmp/iota.bin";

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
                log("[mailbox] await_suspend called, setting continuation: ", handle.address());
                mbox._continuation = handle;

                auto casted_continuation = std::coroutine_handle<task::promise_type>::from_address(
                    mbox._continuation.address());
                log("[mailbox] submit_request called with ", mbox.get_request().offset, " from ", casted_continuation.promise().id);
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
        log("[mailbox] set_response called with ", r.data_size);
        assert(std::holds_alternative<request>(_msg) && "set_response called before request was set");
        _msg = std::move(r);
    }

    auto resume()
    {
        log("[mailbox] resume called on _continuation: ", this->_continuation.address());

        if(this->_continuation.done())
            return true;

        assert(_continuation && !this->_continuation.done() && "resume called without a continuation set");

        this->_continuation.resume();

        return this->_continuation.done();
    }
};

using task_mailbox = std::pair<task, std::unique_ptr<mailbox>>;
size_t PAGE_SIZE = 4096;

size_t time_alloc = 0;
std::unique_ptr<uint8_t> allocate_mem_aligned(size_t size)
{
    auto t0 = std::chrono::steady_clock::now();
    uint8_t* ptr = nullptr;
    if(posix_memalign((void**)&ptr, PAGE_SIZE, size) != 0)
    {
        perror("posix_memalign failed");
        throw std::runtime_error("Failed to allocate aligned memory for buffers");
    }
    auto t1 = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    time_alloc += duration.count();
    return std::unique_ptr<uint8_t>(ptr);
}

std::vector<task_mailbox> pop_n(std::deque<task_mailbox>& queue, size_t n)
{
    auto it_start = queue.begin();
    auto it_end = it_start + std::min(n, queue.size());

    if(it_start == it_end)
    {
        log("[pop_n] no tasks to pop");
        return {};
    }

    std::vector<task_mailbox> slice;
    auto slice_size = std::distance(it_start, it_end);
    slice.reserve(slice_size);
    std::move(it_start, it_end, std::back_inserter(slice));

    for(int i = 0; i < slice_size; ++i)
        queue.pop_front();

    return slice;
}

class executor_context
{
    size_t QUEUE_DEPTH = 64;

    size_t _current_request_id{0};
    struct io_uring _ring;

    size_t _request_counter = 0;
    using in_flight_request_t = std::pair<task_mailbox, response>;
    std::unordered_map<size_t, in_flight_request_t> _in_flight_requests;

    std::optional<uint32_t> _sqe_space_left{};

public:
    void submit_sqe(std::deque<task_mailbox>& waiting_for_io_queue, std::deque<task_mailbox>& ready_queue)
    {
        if(!this->_in_flight_requests.empty()) // or: this->_in_flight_requests.size() > 16 // 32 
            return;

        this->ready();

        auto request_nr = std::exchange(this->_sqe_space_left, std::nullopt).value();

        auto requests = pop_n(waiting_for_io_queue, request_nr);

        if(requests.empty())
            return;

        for(auto& tmbox : requests)
        {
            auto& [task, mailbox] = tmbox;

            io_uring_sqe* sqe = io_uring_get_sqe(&this->_ring);

            if(!sqe)
                throw std::runtime_error("io_uring_get_sqe failed: "s + strerror(errno));

            size_t request_idx = this->_request_counter++;

            const auto& req = mailbox->get_request();

            response resp = {
                .data = allocate_mem_aligned(req.size),
                .data_size = req.size};

            io_uring_prep_read(sqe, req.fd, resp.data.get(), req.size, req.offset);

            sqe->user_data = request_idx;

            this->_in_flight_requests.emplace(request_idx, in_flight_request_t{task_mailbox{std::move(task), std::move(mailbox)}, std::move(resp)});
        }

        auto ready = io_uring_sq_ready(&this->_ring);
        log("Submitting ", ready, " requests.");

        if(ready > 0)
        {
            auto submit = io_uring_submit(&this->_ring);

            if(submit < 0)
                throw std::runtime_error("io_uring_submit: "s + strerror(-submit));

            if(ready != submit) // todo might remove
            {
                std::cerr << "Warning: io_uring_submit ready != submit. in flight: "
                          << _in_flight_requests.size() << ", ready: " << ready << "submit: " << submit << std::endl;
            }

            if(ready != requests.size())
            {
                std::cerr << "Warning: io_uring_submit ready != requests.size(). requests.size(): "
                          << requests.size() << ", ready: " << ready << "submit: " << submit << std::endl;
            }
        }
    }

    void wait_for_cqe(std::deque<task_mailbox>& ready_queue)
    {
        auto wait_nr = io_uring_cq_ready(&this->_ring);

        if(wait_nr == 0)
            return;

        io_uring_cqe* cqe;
        auto ret = io_uring_wait_cqe_nr(&this->_ring, &cqe, wait_nr);

        if(ret < 0)
            throw std::runtime_error("io_uring_wait_cqe: "s + strerror(-ret));

        uint32_t head{};
        uint32_t cqe_count{};

        io_uring_for_each_cqe(&this->_ring, head, cqe)
        {
            if(cqe->res < 0)
            {
                log("current request: ", "in_flight_requests[cqe->user_data].second_value_here"); // Replaced with a placeholder as `in_flight_requests[cqe->user_data].second` was commented out
                throw std::runtime_error("Read error: "s + strerror(-cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
            }
            else if(cqe->res != PAGE_SIZE)
            {
                throw std::runtime_error("Wrong read: expected " + std::to_string(PAGE_SIZE) + ", got " + std::to_string(cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
            }

            // if(cqe->user_data >= _in_flight_requests.size())
            // throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", exceeds in_flight_requests size: " + std::to_string(_in_flight_requests.size()));

            auto it = this->_in_flight_requests.find(cqe->user_data);
            if(it == this->_in_flight_requests.end())
                throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", not found in in_flight_requests");

            auto [tmbox, response] = std::move(it->second);
            this->_in_flight_requests.erase(it);

            tmbox.second->set_response(std::move(response));
            ready_queue.push_back(std::move(tmbox));

            cqe_count++;
        }

        if(cqe_count < wait_nr)
            throw std::runtime_error("io_uring_wait_cqe_nr returned "s + std::to_string(wait_nr) + " but processed " + std::to_string(cqe_count) + " cqe(s).");

        io_uring_cq_advance(&this->_ring, cqe_count);
    }

    size_t in_flight_requests_size() const
    {
        return this->_in_flight_requests.size();
    }

    executor_context()
    {
        int ret = io_uring_queue_init(QUEUE_DEPTH, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);

        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s + strerror(-ret));

        log("[executor_context] io_uring initialized with QUEUE_DEPTH: ", QUEUE_DEPTH);
    }

    ~executor_context()
    {
        io_uring_queue_exit(&this->_ring);
        log("[executor_context] io_uring exited");
    }

    bool ready()
    {
        this->_sqe_space_left = io_uring_sq_space_left(&this->_ring);

        log("[executor_context] ready called, sqe_space_left: ", *this->_sqe_space_left);

        return this->_sqe_space_left > 0;
    }
};

std::atomic_uint64_t completed = 0;

task get_obj(request r, mailbox& mbox)
{
    auto response = co_await mbox.submit_request(r);

    std::vector<uint8_t> data{};
    data.assign(static_cast<uint8_t*>(response.data.get()),
                static_cast<uint8_t*>(response.data.get()) + response.data_size);

    // log("[get_obj] received data of size: ", data.size());

    // auto start_value = r.offset / sizeof(uint64_t);

    // auto* ptr = reinterpret_cast<uint64_t*>(data.data());
    // for(size_t i = 0; i < data.size() / sizeof(uint64_t); ++i)
    // {
    //     if(start_value + i != ptr[i])
    //     {
    //         std::cerr << "Error: expected value " << start_value << ", got " << ptr[i] << " at index " << i << " and offset " << r.offset << "\n";
    //         throw std::runtime_error("Data mismatch");
    //     }
    // }

    // log_always("[get_obj] task complete with size ", data.size());

    completed++;
}

int main()
{
    std::deque<task_mailbox> requests_queue;       // tasks requested, but not yet submitted
    std::deque<task_mailbox> waiting_for_io_queue; // tasks waiting for IO
    std::deque<task_mailbox> ready_queue;          // tasks which received IO responses, but not finished yet

    executor_context context{};

    auto [fd, file_size] = open_fd(INDEX_PATH);

    auto rng = std::mt19937(std::random_device{}());
    auto dist = std::uniform_int_distribution<size_t>(0, file_size / PAGE_SIZE - 10);

    auto N_REQUESTS = 1000000;
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        auto mbox = std::make_unique<mailbox>();
        auto task = get_obj(request{.fd = fd, .offset = dist(rng) * PAGE_SIZE, .size = PAGE_SIZE}, *mbox);
        requests_queue.emplace_back(std::move(task), std::move(mbox));
    }

    size_t time_prepare = 0;
    size_t time_execute = 0;
    size_t time_section_ready = 0;

    auto t0 = std::chrono::steady_clock::now();
    while(true)
    {
        auto t00 = std::chrono::steady_clock::now();
        while(!requests_queue.empty()) // start processing requests
        {
            log("[main] processing request_queue tasks");
            auto [task, mbox] = std::move(requests_queue.front());
            requests_queue.pop_front();

            if(!task())
                waiting_for_io_queue.push_back({std::move(task), std::move(mbox)});
        }
        auto t01 = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t01 - t00);
        time_prepare += duration.count();
        // {
        // log("[main] no requests queued");
        // }

        // assert(executor.ready()); // executor should be ready to submit sqe
        auto t11 = std::chrono::steady_clock::now();
        if(context.ready())
            context.submit_sqe(waiting_for_io_queue, ready_queue);
        auto t12 = std::chrono::steady_clock::now();
        auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(t12 - t11);

        auto t31 = std::chrono::steady_clock::now();
        while(!ready_queue.empty()) // do work on tasks which received IO responses
        {
            log("[main] processing ready tasks: ", ready_queue.size());
            auto [task, mbox] = std::move(ready_queue.front());
            ready_queue.pop_front();
            log("[main] resuming task/mailbox: ", task.id());
            mbox->resume();
            if(!task.done())
                waiting_for_io_queue.emplace_front(std::move(task), std::move(mbox)); // it is not correct: it should emplace front BUT respecting the ready_queue order

            if(context.ready() && !waiting_for_io_queue.empty())
                break;
        }
        auto t32 = std::chrono::steady_clock::now();
        auto duration4 = std::chrono::duration_cast<std::chrono::microseconds>(t32 - t31);
        time_section_ready += duration4.count();

        // else
        // {
        // log("[main] no ready tasks");
        // }

        log("[main] waiting for cqes");
        auto t22 = std::chrono::steady_clock::now();
        context.wait_for_cqe(ready_queue); // wait for cqe and process them
        auto t23 = std::chrono::steady_clock::now();
        auto duration3 = std::chrono::duration_cast<std::chrono::microseconds>(t23 - t22);

        time_execute += duration2.count() + duration3.count();

        if(ready_queue.empty() && requests_queue.empty() && waiting_for_io_queue.empty())
        {
            if(context.in_flight_requests_size() > 0)
            {
                // log_always("[main] waiting for in-flight requests to complete: ", context.in_flight_requests_size());
            }
            else
            {
                log_always("[main] all tasks completed, exiting");
                break; // exit if no tasks to process
            }
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Processed " << N_REQUESTS << " requests in " << duration.count() / 1000.0 << " ms." << std::endl;
    std::cout << "Average time per request: " << static_cast<double>(duration.count()) / N_REQUESTS << " us." << std::endl;
    std::cout << "Average throughput: " << static_cast<size_t>(static_cast<double>(N_REQUESTS) / (duration.count() / 1000000.0)) << " requests/sec." << std::endl;
    std::cout << "Total completed: " << completed << std::endl;
    std::cout << "Time section 0: " << time_prepare / 1000.0 << " ms." << std::endl;
    std::cout << "Time section 1: " << time_execute / 1000.0 << " ms." << std::endl;
    std::cout << "Time section 2: " << time_section_ready / 1000.0 << " ms." << std::endl;
    std::cout << "Time spent on allocation: " << time_alloc / 1000.0 << " ms." << std::endl;
}