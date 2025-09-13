#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <iterator>
#include <liburing/io_uring.h>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bits/types/timer_t.h>
#include <fcntl.h>
#include <liburing.h>

#include "io_executor.h"
#include "logger.h"
#include "task.h"

using namespace std::string_literals;

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

    queue.erase(it_start, it_end);

    return slice;
}

executor_context::executor_context(uint32_t queue_depth) : _queue_depth(queue_depth), _max_buffered_requests(queue_depth * 4)
{
    int ret = io_uring_queue_init(this->_queue_depth, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);

    if(ret < 0)
        throw std::runtime_error("error with io_uring_queue_init: "s + strerror(-ret));

    log("[executor_context] io_uring initialized with QUEUE_DEPTH: ", this->_queue_depth);

    this->_worker = std::thread([this]()
                                { this->_run(); });
}

executor_context::~executor_context()
{
    this->shutdown();

    io_uring_queue_exit(&this->_ring);
    log("[executor_context] io_uring exited");
}


void executor_context::_submit_sqe()
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
        if(!mailbox->prepare_sqes(&this->_ring))
        {
            this->_waiting_for_io_queue.emplace_front(std::move(mailbox));
            continue;
        }

        this->_in_flight_requests.emplace(mailbox->get_continuation(), std::move(mailbox));
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

void executor_context::_wait_for_cqe()
{
    if(this->_in_flight_requests.empty())
        return;

    io_uring_cqe* cqe;
    auto ret = io_uring_wait_cqe(&this->_ring, &cqe);

    if(ret < 0)
        throw std::runtime_error("io_uring_wait_cqe: "s + strerror(-ret));

    uint32_t head{};
    uint32_t cqe_count{};

    io_uring_for_each_cqe(&this->_ring, head, cqe)
    {
        auto coro_handle = std::coroutine_handle<>::from_address(reinterpret_cast<void*>(cqe->user_data));
        auto it = this->_in_flight_requests.find(coro_handle);

        if(it == this->_in_flight_requests.end())
            throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", not found in in_flight_requests");

        auto& mailbox = it->second;

        if(mailbox->handle_cqe(cqe))
        {
            this->_ready_queue.emplace_back(std::move(mailbox));
            this->_in_flight_requests.erase(it);
        }

        cqe_count++;
    }

    io_uring_cq_advance(&this->_ring, cqe_count);
}

void executor_context::_do_work()
{
    while(!this->_ready_queue.empty())
    {
        auto mailbox = std::move(this->_ready_queue.front());
        this->_ready_queue.pop_front();

        mailbox->resume();

        if(io_uring_cq_ready(&this->_ring) > 0)
            break;
    }
}

void executor_context::submit_io_task(task<void> task)
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

void executor_context::shutdown()
{
    if(!this->_running.load(std::memory_order_relaxed))
        return;

    std::cout << "Closing uring_reader." << std::endl;

    this->_running.store(false, std::memory_order_relaxed);

    this->_cv.notify_all();

    if(_worker.joinable())
        _worker.join();
}

void executor_context::_run()
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

        this->_submit_sqe();

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

        this->_do_work();

        // log("[main] waiting for cqes");

        // print_status();

        this->_wait_for_cqe();
    }
}