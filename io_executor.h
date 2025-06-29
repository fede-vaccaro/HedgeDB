#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <variant>

#include "logger.h"
#include "mailbox.h"
#include "task.h"

class executor_context
{
    size_t _queue_depth;
    size_t _max_buffered_requests;

    io_uring _ring;

    std::atomic_bool _running{true};
    std::thread _worker;

    std::unordered_map<std::coroutine_handle<>, std::unique_ptr<mailbox>> _in_flight_requests;

    std::deque<std::unique_ptr<mailbox>> _waiting_for_io_queue;
    std::deque<std::unique_ptr<mailbox>> _ready_queue;

    std::condition_variable _cv;
    std::mutex _pending_requests_mutex;
    std::deque<task<void>> _pending_requests;

public:
    executor_context(uint32_t queue_depth);
    ~executor_context();

    void submit_io_task(task<void> task);
    void shutdown();

    auto submit_request(auto request)
    {
        using request_t = std::decay_t<decltype(request)>;

        std::unique_ptr<mailbox> new_mailbox = from_request(std::move(request));

        auto awaitable = awaitable_mailbox<typename request_t::response_t>{*new_mailbox};

        this->_waiting_for_io_queue.emplace_back(std::move(new_mailbox));

        return awaitable;
    }

private:
    void _submit_sqe();
    void _do_work();
    void _wait_for_cqe();
    void _run();
};
