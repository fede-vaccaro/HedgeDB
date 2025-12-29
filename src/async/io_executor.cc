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

namespace hedge::async
{

    template <typename T>
    std::vector<T> pop_n(std::deque<T>& queue, size_t n)
    {
        auto it_start = queue.begin();
        auto it_end = it_start + std::min(n, queue.size());

        if(it_start == it_end)
            return {};

        std::vector<T> slice;

        auto slice_size = std::distance(it_start, it_end);
        slice.reserve(slice_size);
        std::move(it_start, it_end, std::back_inserter(slice));

        queue.erase(it_start, it_end);

        return slice;
    }

    executor_context::executor_context(uint32_t queue_depth) : _queue_depth(queue_depth), _max_buffered_requests(queue_depth * 4)
    {
        int ret = io_uring_queue_init(this->_queue_depth, &this->_ring, 0);

        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s + strerror(-ret));

        this->_worker = std::thread(
            [this]()
            {
                this->_event_loop();
            });

        this->_in_flight_requests.reserve(this->_queue_depth * 32);

        pthread_setname_np(this->_worker.native_handle(), "io-executor");
    }

    executor_context::~executor_context()
    {
        this->shutdown();

        io_uring_queue_exit(&this->_ring);
    }

    void executor_context::_submit_sqe()
    {
        auto sq_ready = static_cast<int32_t>(io_uring_sq_ready(&this->_ring));
        auto in_flight = static_cast<int32_t>(this->_in_flight_requests.size());

        auto potential_cqe_ready = sq_ready + in_flight;
        auto cqe_space_margin = (static_cast<int32_t>(this->_queue_depth) * 2) - potential_cqe_ready;

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

            uint64_t this_req_id = this->_current_request_id++;

            io_uring_sqe_set_data64(sqe, this_req_id);

            mailbox->prepare_sqe(sqe);

            this->_in_flight_requests.emplace(this_req_id, std::move(mailbox));
        }

        auto ready = io_uring_sq_ready(&this->_ring);

        if(ready > 0)
        {
            auto submit = io_uring_submit(&this->_ring);

            if(submit < 0)
            {
                std::cout << "io_uring_submit failed with error: " << strerror(-submit) << std::endl;
                throw std::runtime_error("io_uring_submit: "s + strerror(-submit));
            }

            // if(ready != submit) // todo might remove
            // {
            // }

            // if(ready != requests.size())
            // {
            // }
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
            uint64_t request_id = io_uring_cqe_get_data64(cqe);

            auto it = this->_in_flight_requests.find(request_id);

            if(it == this->_in_flight_requests.end())
                throw std::runtime_error("Invalid user_data: " + std::to_string(request_id) + ", not found in in_flight_requests");

            auto& mailbox = it->second;

            mailbox->handle_cqe(cqe);

            this->_io_ready_queue.emplace_back(std::move(mailbox));
            this->_in_flight_requests.erase(it);

            cqe_count++;
        }

        io_uring_cq_advance(&this->_ring, cqe_count);
    }

    void executor_context::_do_work()
    {
        while(!this->_io_ready_queue.empty())
        {
            auto mailbox = std::move(this->_io_ready_queue.front());
            this->_io_ready_queue.pop_front();

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

        if(this->_worker.joinable())
            this->_worker.join();
    }

    void executor_context::_event_loop()
    {
        log_always("Launching io executor. Queue depth: ", this->_queue_depth, " Max buffered tasks: ", this->_max_buffered_requests);

        std::vector<task<void>> in_progress_tasks;
        in_progress_tasks.reserve(this->_max_buffered_requests);

        std::deque<task<void>> new_tasks;
        while(true)
        {
            if(in_progress_tasks.size() < this->_queue_depth)
            {
                std::unique_lock lk(this->_pending_requests_mutex); // should have priority over submit

                while(!this->_pending_requests.empty() && in_progress_tasks.size() < this->_queue_depth)
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

            this->_wait_for_cqe();
        }
    }

    void executor_context::register_fd(int32_t fd)
    {
        auto offs = this->_registered_fds.size();
        this->_registered_fds.push_back(fd);
        auto result = io_uring_register_files_update(&this->_ring, offs, this->_registered_fds.data(), 1);
        if(result < 0)
        {
            log_always("Failed to register file descriptor ", fd, ": ", strerror(-result));
            throw std::runtime_error("Failed to register file descriptor: " + std::string(strerror(-result)));
        }
    }

    //
    // Static I/O Thread Pool
    //
    std::unique_ptr<executor_pool> executor_pool::_static_pool = nullptr;

    void executor_pool::init_static_pool(size_t pool_size, size_t queue_depth)
    {
        if(executor_pool::_static_pool)
            return;

        executor_pool::_static_pool = std::unique_ptr<executor_pool>(new executor_pool(pool_size, queue_depth));
    }

    const std::shared_ptr<executor_context>& executor_pool::executor_from_static_pool()
    {
        assert(executor_pool::_static_pool);
        return executor_pool::_static_pool->get_executor();
    }

    thread_local std::shared_ptr<executor_context> executor_context::_this_thread_executor = nullptr;

    const std::shared_ptr<executor_context>& executor_context::this_thread_executor()
    {
        return executor_context::_this_thread_executor;
    }

    executor_pool::executor_pool(size_t pool_size, uint32_t queue_depth)
    {
        // Check if pool size is power of 2
        if(std::popcount(pool_size) != 1)
            throw std::runtime_error("executor_pool size must be a power of 2");

        for(size_t i = 0; i < pool_size; ++i)
        {
            auto new_executor = std::make_shared<executor_context>(queue_depth);
            new_executor->sync_submit(
                [&new_executor]() -> async::task<int>
                {
                    executor_context::_this_thread_executor = new_executor;
                    co_return 0; // At the moment, sync_submit requires a return value
                }());
            this->_executors.emplace_back(std::move(new_executor));
        }
    }

    executor_pool& executor_pool::static_pool()
    {
        if(executor_pool::_static_pool == nullptr)
            throw std::runtime_error("static executors not initialized: call executor_pool::init first");

        return *executor_pool::_static_pool;
    }

    const std::shared_ptr<executor_context>& executor_pool::get_executor()
    {
        auto idx = this->_next_executor.fetch_add(1, std::memory_order_relaxed) & (this->_executors.size() - 1);
        return this->_executors[idx];
    }

} // namespace hedge::async