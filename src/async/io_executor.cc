#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <iterator>
#include <liburing/io_uring.h>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <bits/types/timer_t.h>
#include <fcntl.h>
#include <liburing.h>

#include "io_executor.h"
#include "logger.h"
#include "perf_counter.h"
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
        this->_in_progress_tasks.reserve(this->_queue_depth * 2);

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

            auto& mailbox = it.value();

            mailbox->handle_cqe(cqe);

            this->_io_ready_queue.emplace_back(std::move(mailbox));
            this->_in_flight_requests.erase(it);

            cqe_count++;
        }

        io_uring_cq_advance(&this->_ring, cqe_count);
    }

    void executor_context::_do_work()
    {
        thread_local std::deque<std::unique_ptr<mailbox>> requeued;

        while(!this->_io_ready_queue.empty())
        {
            auto mailbox = std::move(this->_io_ready_queue.front());
            this->_io_ready_queue.pop_front();

            if(mailbox->get_continuation() == nullptr)
                requeued.emplace_back(std::move(mailbox));
            else
                mailbox->resume();

            if(io_uring_cq_ready(&this->_ring) > 0)
                break;
        }

        while(!requeued.empty())
        {
            this->_waiting_for_io_queue.emplace_back(std::move(requeued.front()));
            requeued.pop_front();
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

            this->_pending_requests_cv.wait(lk, [this]()
                                            { return this->_pending_requests.size() < this->_max_buffered_requests; });

            this->_pending_requests.emplace_back(std::move(task));
        }

        this->_sleep_cv.notify_one();
    };

    void executor_context::submit_io_task_high_pri(task<void> task)
    {
        if(!this->_running.load(std::memory_order_relaxed))
        {
            std::cerr << "cannot submit task: context closed" << std::endl;
            return;
        }

        {
            std::unique_lock lk(this->_pending_requests_mutex);

            this->_pending_requests.emplace_front(std::move(task));
        }

        this->_sleep_cv.notify_one();
    };

    void executor_context::shutdown()
    {
        if(!this->_running.load(std::memory_order_relaxed))
            return;

        std::cout << "Closing uring_reader." << std::endl;

        this->_running.store(false, std::memory_order_relaxed);

        this->_sleep_cv.notify_one();
        this->_pending_requests_cv.notify_all();

        if(this->_worker.joinable())
            this->_worker.join();
    }

    bool executor_context::_should_sleep()
    {
        if(!this->_in_flight_requests.empty() ||
           !this->_in_progress_tasks.empty() ||
           !this->_waiting_for_io_queue.empty() ||
           !this->_io_ready_queue.empty())
            return false;

        size_t pending;

        {
            std::unique_lock lk(this->_pending_requests_mutex);
            pending = this->_pending_requests.size();
        }

        return pending == 0 && this->_running.load(std::memory_order_relaxed);
    }

    void executor_context::_event_loop()
    {
        log_always("Launching io executor. Queue depth: ", this->_queue_depth, " Max buffered tasks: ", this->_max_buffered_requests);

        std::deque<task<void>> new_tasks;

        while(true)
        {
            if(this->_in_progress_tasks.size() < this->_queue_depth)
            {
                std::unique_lock lk(this->_pending_requests_mutex); // should have priority over submit

                if(this->_count_before_sleep == CYCLES_BEFORE_SLEEP)
                {
                    this->_sleep_cv.wait(lk, [this]()
                                         { return !this->_pending_requests.empty() || !this->_running.load(std::memory_order::relaxed); });
                    this->_count_before_sleep = 0;
                }

                while(!this->_pending_requests.empty() && this->_in_progress_tasks.size() < this->_queue_depth)
                {
                    auto task = std::move(this->_pending_requests.front());
                    this->_pending_requests.pop_front();

                    new_tasks.emplace_back(std::move(task));
                }

                if(!this->_running.load(std::memory_order_relaxed) && this->_pending_requests.empty()) // && this->_in_progress_tasks.empty() && this->_in_flight_requests.empty() && new_tasks.empty()
                    break;
            }
            this->_pending_requests_cv.notify_all();

            this->_submit_sqe();

            while(!new_tasks.empty())
            {
                auto task = std::move(new_tasks.front());
                new_tasks.pop_front();

                auto handle = task.handle();
                auto [_, ok] = this->_in_progress_tasks.insert({handle, std::move(task)});
                assert(ok);

                handle.resume();
            }

            this->_gc_tasks();

            this->_do_work();

            this->_wait_for_cqe();

            if(this->_should_sleep())
                this->_count_before_sleep++;
        }
    }

    void executor_context::_gc_tasks()
    {
        prof::avg_stat::PERF_STATS["gc_coros"].start();

        thread_local std::vector<decltype(this->_in_progress_tasks)::iterator> to_remove;
        to_remove.clear();

        for(auto it = this->_in_progress_tasks.begin(); it != this->_in_progress_tasks.end(); ++it)
        {
            if(it->second.done())
                to_remove.emplace_back(it);
        }

        for(const auto& it : to_remove)
            this->_in_progress_tasks.erase(it);

        prof::avg_stat::PERF_STATS["gc_coros"].stop();
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

    std::shared_ptr<executor_context> executor_context::make_new(uint32_t queue_depth)
    {
        auto new_executor = std::shared_ptr<executor_context>(new executor_context(queue_depth));

        auto task_generator = [&new_executor]() -> async::task<int>
        {
            executor_context::_this_thread_executor = new_executor;
            co_return 0; // At the moment, sync_submit requires a return value
        };

        [[maybe_unused]] auto r = new_executor->sync_submit(task_generator());

        return new_executor;
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
        for(size_t i = 0; i < pool_size; ++i)
        {
            auto new_executor = executor_context::make_new(queue_depth);

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
        auto idx = this->_next_executor.fetch_add(1, std::memory_order_relaxed) % this->_executors.size();
        return this->_executors[idx];
    }

} // namespace hedge::async