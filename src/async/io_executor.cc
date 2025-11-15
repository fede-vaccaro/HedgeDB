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
        int ret = io_uring_queue_init(this->_queue_depth, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);

        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s + strerror(-ret));

        this->_worker = std::thread([this]()
                                    { this->_event_loop(); });

        this->_in_flight_requests.reserve(this->_queue_depth * 32);

        pthread_setname_np(this->_worker.native_handle(), "io-executor");
    }

    executor_context::~executor_context()
    {
        this->shutdown();

        io_uring_queue_exit(&this->_ring);
    }

    uint64_t executor_context::forge_request_key(uint64_t request_id, uint8_t sub_request_idx)
    {
        request_id %= (1UL << 56);

        uint64_t key = request_id;

#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        key |= (static_cast<uint64_t>(sub_request_idx) << 56);
#else
        key |= (static_cast<uint64_t>(sub_request_idx) << 8);
#endif
#else
#error "Byte order not defined. Please define __BYTE_ORDER__."
#endif

        return key;
    }

    std::pair<uint64_t, uint8_t> executor_context::parse_request_key(uint64_t key)
    {
#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        uint64_t request_id = key & 0x00FFFFFFFFFFFFFF;
        uint8_t sub_request_idx = static_cast<uint8_t>(key >> 56);
#else
        uint64_t request_id = key >> 8;
        uint8_t sub_request_idx = static_cast<uint8_t>(key & 0xFF);
#endif
#else
#error "Byte order not defined. Please define __BYTE_ORDER__."
#endif

        assert(request_id < (1UL << 56));

        return {request_id, sub_request_idx};
    }

    std::vector<io_uring_sqe*>& executor_context::_fill_sqes(size_t sqes_requested)
    {
        thread_local std::vector<io_uring_sqe*> tmp_sqes{};

        tmp_sqes.clear();

        for(size_t i = 0; i < sqes_requested; ++i)
        {
            io_uring_sqe* sqe = io_uring_get_sqe(&this->_ring);

            if(sqe == nullptr)
                throw std::runtime_error("io_uring_get_sqe failed");

            uint64_t sqe_id = forge_request_key(this->current_request_id, static_cast<uint8_t>(i));
            io_uring_sqe_set_data64(sqe, sqe_id);

            tmp_sqes.emplace_back(sqe);
        }

        return tmp_sqes;
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
            auto space_needed = mailbox->needed_sqes();

            if(space_needed > sq_space_left)
            {
                this->_waiting_for_io_queue.emplace_front(std::move(mailbox));
                continue;
            }

            sq_space_left -= space_needed;

            mailbox->prepare_sqes(this->_fill_sqes(space_needed));

            this->_in_flight_requests.emplace(this->current_request_id++, std::move(mailbox));
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
            auto [request_id, sub_request_id] = parse_request_key(io_uring_cqe_get_data64(cqe));

            auto it = this->_in_flight_requests.find(request_id);

            if(it == this->_in_flight_requests.end())
                throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", not found in in_flight_requests");

            auto& mailbox = it->second;

            if(mailbox->handle_cqe(cqe, sub_request_id))
            {
                this->_io_ready_queue.emplace_back(std::move(mailbox));
                this->_in_flight_requests.erase(it);
            }

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

    const std::shared_ptr<executor_context>& executor_from_static_pool()
    {
        constexpr size_t POOL_SIZE = 8;

        using executor_pool = std::array<std::shared_ptr<executor_context>, POOL_SIZE>;

        static std::atomic_uint64_t current_head{0};
        static executor_pool pool = []()
        {
            executor_pool _pool;

            for(auto& pool_ptr : _pool)
                pool_ptr = std::make_shared<executor_context>(128);

            return _pool;
        }();

        return pool[current_head.fetch_add(1, std::memory_order_relaxed) % pool.size()];
    }

} // namespace hedge::async