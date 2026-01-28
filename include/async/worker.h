#pragma once

#include "io_executor.h"
#include <condition_variable>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>

namespace hedge::async
{

    class worker
    {
        using job_t = std::function<void()>;

        alignas(64) std::atomic_size_t _job_count{0};
        std::thread _worker;

        bool _running{true};
        std::mutex _queue_m;

        std::deque<job_t> _job_queue;
        async::mpsc_queue<job_t, 32> _fast_job_queue;

    public:
        worker();
        ~worker();

        void submit(std::function<void()> job);
        void shutdown();

    private:
        void _run();
    };
} // namespace hedge::async
