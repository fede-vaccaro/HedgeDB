#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>
#include <mutex>

namespace hedgehog::async
{

    class worker
    {
        static constexpr size_t MAX_JOBS = 16;

        using job_t = std::function<void()>;

        std::thread _worker;

        bool _running{true};
        std::condition_variable _cv;
        std::mutex _queue_m;
        std::deque<job_t> _job_queue;

    public:
        worker();
        ~worker();
        
        void submit(std::function<void()> job);
        void shutdown();

    private:
        void _run();
    };
} // namespace hedgehog::db
