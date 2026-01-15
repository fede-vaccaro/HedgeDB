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
#include <tsl/robin_map.h>
#include <tsl/sparse_map.h>
#include <type_traits>

#include <logger.h>

#include "mailbox.h"
#include "task.h"

namespace hedge::async
{

    /*

        The Async I/O Layer (io_uring integration)

        ┌────────────────────────────────────┐
        │      executor_context              │
        │                                    │
        │  ┌──────────────────┐              │
        │  │  _pending_queue  │  (tasks)     │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │  ┌──────────────────┐              │
        │  │ _waiting_for_io  │  (mailboxes) │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │  ┌──────────────────┐              │
        │  │   io_uring SQE   │  (submit)    │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │  ┌──────────────────┐              │
        │  │ in_flight_reqs   │  (pending)   │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │  ┌──────────────────┐              │
        │  │   io_uring CQE   │  (complete)  │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │  ┌──────────────────┐              │
        │  │  _io_ready_queue │  (resume)    │
        │  └────────┬─────────┘              │
        │           ▼                        │
        │      [coroutine.resume()]          │
        └────────────────────────────────────┘

        co_await executor.submit_request(read_request{fd, offset, size})
        ↓
        1. Wrap request in mailbox (type-erased)
        2. Store coroutine handle in mailbox
        3. Convert to io_uring SQE(s)
        4. Submit to kernel
        ↓ (later, when I/O completes)
        5. CQE arrives → find mailbox by request_id
        6. mailbox.handle_cqe() → populate response
        7. mailbox.resume() → coroutine continues
    */
    class executor_pool;

    class executor_context : public std::enable_shared_from_this<executor_context>
    {
        friend executor_pool;

        size_t _queue_depth;
        size_t _max_buffered_requests;

        static constexpr int32_t CYCLES_BEFORE_SLEEP = 16;
        int32_t _count_before_sleep{0};

        io_uring _ring;

        std::atomic_bool _running{true};
        std::thread _worker;

        uint64_t _current_request_id{0};

        tsl::robin_map<uint64_t, std::unique_ptr<mailbox>> _in_flight_requests;
        std::unordered_map<std::coroutine_handle<>, task<void>> _in_progress_tasks;

        std::deque<std::unique_ptr<mailbox>> _waiting_for_io_queue;
        std::deque<std::unique_ptr<mailbox>> _io_ready_queue;

        std::condition_variable _sleep_cv;
        std::condition_variable _pending_requests_cv;
        std::mutex _pending_requests_mutex;
        std::deque<task<void>> _pending_requests;

        std::vector<int32_t> _registered_fds;

    public:
        executor_context() = default;
        ~executor_context();

        executor_context(const executor_context&) = delete;
        executor_context& operator=(const executor_context&) = delete;

        template <typename T>
        T sync_submit(task<T> t)
        {
            std::promise<T> promise;

            auto future = promise.get_future();

            auto task_lambda = [](std::promise<T> promise, task<T> t) mutable -> hedge::async::task<void>
            {
                auto value = co_await t;

                promise.set_value(std::move(value));
            };

            this->submit_io_task(task_lambda(std::move(promise), std::move(t)));

            return future.get();
        }

        void submit_io_task(task<void> task);          // do NOT call this from a task, otherwise it will deadlock! TODO: address this
        bool try_submit_io_task(task<void> task); // do NOT call this from a task, otherwise it will deadlock! TODO: address this

        void shutdown();

        // TODO: enforce that this is callable only from this_thread_executor()
        auto submit_request(auto request)
        {
            using request_t = std::decay_t<decltype(request)>;

            std::unique_ptr<mailbox> new_mailbox = from_request(std::move(request));

            auto awaitable = awaitable_mailbox<typename request_t::response_t>{new_mailbox.get()};

            this->_waiting_for_io_queue.emplace_back(std::move(new_mailbox));

            return awaitable;
        }

        // TODO: enforce that this is callable only from this_thread_executor()
        task<void> extract_task(std::coroutine_handle<> root_coro)
        {
            auto it = this->_in_progress_tasks.find(root_coro);

            assert(it != this->_in_progress_tasks.end());

            auto extracted = task<void>(std::move(it->second));

            this->_in_progress_tasks.erase(it);

            return extracted;
        }

        // TODO: enforce that this is callable only from this_thread_executor()
        void transfer_task(task<void> task, std::coroutine_handle<> continuation)
        {
            auto [it, ok] = this->_in_progress_tasks.emplace(task.handle(), std::move(task));
            assert(ok);

            // make continuation mailbox
            auto continuation_mailbox = std::make_unique<mailbox>(async::continuation_mailbox{});
            continuation_mailbox->set_continuation(continuation);
            this->_io_ready_queue.push_front(std::move(continuation_mailbox));
        }

        // TODO: enforce that this is callable only from this_thread_executor()
        auto yield()
        {
            return this->submit_request(yield_request{});
        }

        void register_fd(int32_t fd);

        static std::shared_ptr<executor_context> make_new(uint32_t queue_depth);

        static const std::shared_ptr<executor_context>& this_thread_executor();

    private:
        explicit executor_context(uint32_t queue_depth); // use executor_context::make_new instead

        static thread_local std::shared_ptr<executor_context> _this_thread_executor;

        void _submit_sqe();
        void _do_work();
        void _wait_for_cqe();
        void _event_loop();
        void _gc_tasks();

        bool _should_sleep();
        std::vector<io_uring_sqe*>& _fill_sqes(size_t sqes_requested);
    };

    inline const std::shared_ptr<executor_context>& this_thread_executor()
    {
        return executor_context::this_thread_executor();
    }

    class executor_pool
    {
        std::vector<std::shared_ptr<executor_context>> _executors;
        std::atomic_size_t _next_executor{0};

        executor_pool(size_t pool_size, uint32_t queue_depth);
        const std::shared_ptr<executor_context>& get_executor();

        static std::unique_ptr<executor_pool> _static_pool;

    public:
        [[nodiscard]] size_t size() const
        {
            return this->_executors.size();
        }

        static executor_pool& static_pool();
        static void init_static_pool(size_t pool_size, size_t queue_depth);
        static const std::shared_ptr<executor_context>& executor_from_static_pool();
    };

} // namespace hedge::async