#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <future>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include <logger.h>

#include "mailbox.h"
#include "task.h"

namespace hedge::async
{
    class executor_context
    {
        size_t _queue_depth;
        size_t _max_buffered_requests;

        io_uring _ring;

        std::atomic_bool _running{true};
        std::thread _worker;

        uint64_t current_request_id{0};
        std::unordered_map<uint64_t, std::unique_ptr<mailbox>> _in_flight_requests;

        std::deque<std::unique_ptr<mailbox>> _waiting_for_io_queue;
        std::deque<std::unique_ptr<mailbox>> _io_ready_queue;

        std::condition_variable _cv;
        std::mutex _pending_requests_mutex;
        std::deque<task<void>> _pending_requests;

        std::vector<int32_t> _registered_fds;

    public:
        executor_context() = default;
        explicit executor_context(uint32_t queue_depth);
        ~executor_context();

        executor_context(const executor_context&) = delete;
        executor_context& operator=(const executor_context&) = delete;

        template <typename T>
        T sync_submit(task<T> task)
        {
            std::promise<T> promise;

            auto future = promise.get_future();

            auto task_lambda = [promise = std::move(promise), &task]() mutable -> hedge::async::task<void>
            {
                auto value = co_await task;

                promise.set_value(std::move(value));
            };

            this->submit_io_task(task_lambda());

            return future.get();
        }

        void submit_io_task(task<void> task); // do NOT call this from a task, otherwise it will deadlock! TODO: address this

        void shutdown();

        auto submit_request(auto request)
        {
            using request_t = std::decay_t<decltype(request)>;

            std::unique_ptr<mailbox> new_mailbox = from_request(std::move(request));

            auto awaitable = awaitable_mailbox<typename request_t::response_t>{*new_mailbox};

            this->_waiting_for_io_queue.emplace_back(std::move(new_mailbox));

            return awaitable;
        }

        void register_fd(int32_t fd);

    private:
        void _submit_sqe();
        void _do_work();
        void _wait_for_cqe();
        void _event_loop();
        std::vector<io_uring_sqe*>& _fill_sqes(size_t sqes_requested);

        static uint64_t forge_request_key(uint64_t request_id, uint8_t sub_request_idx);
        static std::pair<uint64_t, uint8_t> parse_request_key(uint64_t key);
    };

} // namespace hedge::async