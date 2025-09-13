#include <atomic>
#include <bits/types/timer_t.h>

#include <cassert>
#include <chrono>
#include <condition_variable>
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
#include "task.h"

using namespace std::string_literals;

struct read_request
{
    int fd;
    size_t offset;
    size_t size;
};

struct read_response
{
    std::unique_ptr<uint8_t> data;
    size_t data_size;
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
    std::variant<std::monostate, read_request, read_response> msg;
    std::coroutine_handle<> continuation;

    read_request get_request()
    {
        assert(!std::holds_alternative<std::monostate>(msg) && "get_request called before request was set");
        return std::get<read_request>(msg);
    }

    auto set_response(read_response r)
    {
        log("[mailbox] set_response called with ", r.data_size);
        assert(std::holds_alternative<read_request>(msg) && "set_response called before request was set");
        this->msg = std::move(r);
    }

    auto resume()
    {
        log("[mailbox] resume called on _continuation: ", this->continuation.address());

        if(this->continuation.done())
            return true;

        assert(continuation && !this->continuation.done() && "resume called without a continuation set");

        this->continuation.resume();

        return this->continuation.done();
    }
};

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

template <typename T>
std::vector<T> pop_n(std::deque<T>& queue, size_t n)
{
    auto it_start = queue.begin();
    auto it_end = it_start + std::min(n, queue.size());

    if(it_start == it_end)
    {
        log("[pop_n] no tasks to pop");
        return {};
    }

    std::vector<T> slice;
    auto slice_size = std::distance(it_start, it_end);
    slice.reserve(slice_size);
    std::move(it_start, it_end, std::back_inserter(slice));

    for(int i = 0; i < slice_size; ++i)
        queue.pop_front();

    return slice;
}

constexpr auto QUEUE_DEPTH = 256;

class executor_context
{
    size_t _queue_depth = QUEUE_DEPTH;
    size_t _max_buffered_requests = QUEUE_DEPTH * 4;

    io_uring _ring;

    std::atomic_bool _running{true};
    std::thread _worker;

    using in_flight_request_t = std::pair<std::unique_ptr<mailbox>, read_response>;
    std::unordered_map<std::coroutine_handle<>, in_flight_request_t> _in_flight_requests;

    std::deque<std::unique_ptr<mailbox>> _waiting_for_io_queue;
    std::deque<std::unique_ptr<mailbox>> _ready_queue;

    // task ingress buffer
    std::condition_variable _cv;
    std::mutex _pending_requests_mutex;
    std::deque<task<void>> _pending_requests;

public:
    void submit_sqe()
    {
        auto sq_ready = static_cast<int32_t>(io_uring_sq_ready(&this->_ring));
        auto in_flight = static_cast<int32_t>(this->_in_flight_requests.size());

        auto potential_cqe_ready = sq_ready + in_flight;
        auto cqe_space_margin = static_cast<int32_t>(this->_queue_depth) * 2 - potential_cqe_ready;

        if(cqe_space_margin <= 0) // avoid cqe overflow risk
            return;

        auto sq_space_left = io_uring_sq_space_left(&this->_ring);
        auto sq_to_submit = std::min(static_cast<int32_t>(sq_space_left), cqe_space_margin);

        auto requests = pop_n(this->_waiting_for_io_queue, sq_to_submit);

        if(requests.empty())
            return;

        for(auto& mailbox : requests)
        {
            io_uring_sqe* sqe = io_uring_get_sqe(&this->_ring);

            if(!sqe)
                throw std::runtime_error("io_uring_get_sqe failed: "s + strerror(errno));

            const auto& req = mailbox->get_request();

            read_response resp = {
                .data = allocate_mem_aligned(req.size),
                .data_size = req.size};

            io_uring_prep_read(sqe, req.fd, resp.data.get(), req.size, req.offset);

            auto continuation = mailbox->continuation;

            sqe->user_data = reinterpret_cast<uint64_t>(continuation.address());

            this->_in_flight_requests.emplace(continuation, in_flight_request_t{std::move(mailbox), std::move(resp)});
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
                // std::cerr << "Warning: io_uring_submit ready != submit. in flight: "
                        //   << _in_flight_requests.size() << ", ready: " << ready << " submit: " << submit << " requests: " << requests.size() << std::endl;
            }

            if(ready != requests.size())
            {
                // std::cerr << "Warning: io_uring_submit ready != requests.size(). requests.size(): "
                //   << requests.size() << ", ready: " << ready << " submit: " << submit << " in flight: " << _in_flight_requests.size() << std::endl;
            }
        }
    }

    void wait_for_cqe()
    {
        auto wait_nr = io_uring_cq_ready(&this->_ring);

        if(wait_nr == 0)
            return;

        struct io_uring_cqe* cqe;
        auto ret = io_uring_wait_cqe_nr(&this->_ring, &cqe, wait_nr);

        if(ret < 0)
            throw std::runtime_error("io_uring_wait_cqe: "s + strerror(-ret));

        unsigned head;
        unsigned cqe_count = 0;

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

            auto coro_handle = std::coroutine_handle<>::from_address(reinterpret_cast<void*>(cqe->user_data));
            auto it = this->_in_flight_requests.find(coro_handle);

            if(it == this->_in_flight_requests.end())
                throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", not found in in_flight_requests");

            auto [mailbox, response] = std::move(it->second);

            this->_in_flight_requests.erase(it);

            mailbox->set_response(std::move(response));

            this->_ready_queue.emplace_back(std::move(mailbox));

            cqe_count++;
        }

        if(cqe_count < wait_nr)
            throw std::runtime_error("io_uring_wait_cqe_nr returned "s + std::to_string(wait_nr) + " but processed " + std::to_string(cqe_count) + " cqe(s).");

        io_uring_cq_advance(&this->_ring, cqe_count);
    }

    void do_work()
    {
        while(!this->_ready_queue.empty())
        {
            auto mailbox = std::move(this->_ready_queue.front());
            this->_ready_queue.pop_front();

            mailbox->continuation.resume();

            if(io_uring_cq_ready(&this->_ring) > 0)
                break;
        }
    }

    auto submit_request(read_request r)
    {
        struct awaitable_mailbox
        {
            mailbox& mbox;

            bool await_ready() noexcept { return false; }

            void await_suspend(
                std::coroutine_handle<> handle) noexcept
            {
                mbox.continuation = handle;
            }

            read_response await_resume() noexcept
            {
                log("[awaitable_mailbox] await_resume called on mailbox with continuation: ", mbox.continuation.address());
                assert(std::holds_alternative<read_response>(mbox.msg) && "await_resume called before response was set");
                return std::move(std::get<read_response>(mbox.msg));
            }

            awaitable_mailbox(mailbox& m)
                : mbox(m)
            {
            }
        };

        auto new_mailbox = std::make_unique<mailbox>();
        new_mailbox->msg = std::move(r);

        auto awaitable = awaitable_mailbox{*new_mailbox};

        this->_waiting_for_io_queue.emplace_back(std::move(new_mailbox));

        return awaitable;
    }

    size_t in_flight_requests_size() const
    {
        return this->_in_flight_requests.size();
    }

    executor_context()
    {
        int ret = io_uring_queue_init(this->_queue_depth, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);

        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s + strerror(-ret));

        log("[executor_context] io_uring initialized with QUEUE_DEPTH: ", this->_queue_depth);

        this->_worker = std::thread([this]()
                                    { this->_run(); });
    }

    ~executor_context()
    {
        this->shutdown();

        io_uring_queue_exit(&this->_ring);
        log("[executor_context] io_uring exited");
    }

    void submit_io_task(task<void> task)
    {
        if(!this->_running.load(std::memory_order_relaxed))
        {
            std::cerr << "cannot submit task: context closed" << std::endl;
            return;
        }

        {
            // back pressure here
            std::unique_lock lk(this->_pending_requests_mutex);

            this->_cv.wait(lk, [this]()
                           { return this->_pending_requests.size() < this->_max_buffered_requests; });

            this->_pending_requests.emplace_back(std::move(task));
        }
    };

    void shutdown()
    {
        if(!this->_running.load(std::memory_order_relaxed))
            return;

        std::cout << "Closing uring_reader." << std::endl;

        this->_running.store(false, std::memory_order_relaxed);

        this->_cv.notify_all();

        if(_worker.joinable())
            _worker.join();
    }

private:
    void _run()
    {
        log_always("Launching io executor. Queue depth: ", this->_queue_depth, " Max buffered tasks: ", this->_max_buffered_requests);

        std::vector<task<void>> in_progress_tasks;
        in_progress_tasks.reserve(this->_max_buffered_requests);

        std::deque<task<void>> new_tasks;

        while(true)
        {
            if(in_progress_tasks.size() < this->_max_buffered_requests)
            {
                std::unique_lock lk(this->_pending_requests_mutex); // should have priority over submit

                while(!this->_pending_requests.empty() && in_progress_tasks.size() < this->_max_buffered_requests)
                {
                    auto task = std::move(this->_pending_requests.front());
                    this->_pending_requests.pop_front();

                    new_tasks.emplace_back(std::move(task));
                }

                if(!this->_running.load(std::memory_order_relaxed) && this->_pending_requests.empty())
                    break;
            }
            this->_cv.notify_all();

            this->submit_sqe();

            while(!new_tasks.empty())
            {
                auto task = std::move(new_tasks.front());
                new_tasks.pop_front();

                bool done = task();
                if(!done)
                    in_progress_tasks.emplace_back(std::move(task));
            }

            in_progress_tasks.erase(
                std::remove_if(in_progress_tasks.begin(), in_progress_tasks.end(),
                               [](const auto& task)
                               { return task.done(); }),
                in_progress_tasks.end());

            this->do_work();

            // log("[main] waiting for cqes");

            // print_status();

            this->wait_for_cqe();
        }
    }
};

class working_group
{
    std::atomic_uint64_t _counter{0};
    std::atomic_bool _done{false};

    std::condition_variable _cv;
    std::mutex _mutex;

public:
    void incr()
    {
        this->_counter++;
    }

    void set(size_t count)
    {
        this->_counter = count;
    }

    void decr()
    {
        this->_counter--;

        this->_done = this->_counter == 0;

        if(this->_done)
            this->_cv.notify_all();
    }

    void wait()
    {
        std::unique_lock lk(this->_mutex);
        this->_cv.wait(lk, [this]()
                       { return this->_done.load(); });
    }
};

std::atomic_uint64_t atomic_counter = 0;

task<void> get_obj(read_request r, executor_context& executor, working_group& wg)
{
    auto response = co_await executor.submit_request(r);

    std::vector<uint8_t> data{};
    data.assign(static_cast<uint8_t*>(response.data.get()),
                static_cast<uint8_t*>(response.data.get()) + response.data_size);

    // log("[get_obj] received data of size: ", data.size());

    auto start_value = r.offset / sizeof(uint64_t);

    auto* ptr = reinterpret_cast<uint64_t*>(data.data());
    for(size_t i = 0; i < data.size() / sizeof(uint64_t); ++i)
    {
        if(start_value + i != ptr[i])
        {
            std::cerr << "Error: expected value " << start_value << ", got " << ptr[i] << " at index " << i << " and offset " << r.offset << "\n";
            throw std::runtime_error("Data mismatch");
        }
    }

    // log_always("[get_obj] task complete with size ", data.size(), "counting up to: ", atomic_counter++);

    wg.decr();
}

int main()
{
    working_group wg;
    executor_context context{};

    auto [fd, file_size] = open_fd(INDEX_PATH);

    auto rng = std::mt19937(std::random_device{}());
    auto dist = std::uniform_int_distribution<size_t>(0, file_size / PAGE_SIZE - 10);

    auto N_REQUESTS = 1000000;
    wg.set(N_REQUESTS);

    auto t0 = std::chrono::steady_clock::now();
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        auto task = get_obj(read_request{.fd = fd, .offset = dist(rng) * PAGE_SIZE, .size = PAGE_SIZE}, context, wg);
        context.submit_io_task(std::move(task));
    }
    // log_always("Submitted ", N_REQUESTS, " jobs");
    wg.wait();
    auto t1 = std::chrono::steady_clock::now();

    context.shutdown();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Processed " << N_REQUESTS << " requests in " << duration.count() / 1000.0 << " ms." << std::endl;
    std::cout << "Average time per request: " << static_cast<double>(duration.count()) / N_REQUESTS << " us." << std::endl;
    std::cout << "Average throughput: " << static_cast<size_t>(static_cast<double>(N_REQUESTS) / (duration.count() / 1000000.0)) << " requests/sec." << std::endl;
}