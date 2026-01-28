#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <pthread.h>
#include <thread>

#include "perf_counter.h"
#include "worker.h"

namespace hedge::async
{

    template <typename T>
    T pop_back(std::deque<T>& queue)
    {
        auto val = std::move(queue.back());
        queue.pop_back();
        return std::move(val);
    }

    template <typename T>
    T pop_front(std::deque<T>& queue)
    {
        auto val = std::move(queue.front());
        queue.pop_front();
        return std::move(val);
    }

    worker::worker()
    {
        this->_worker = std::thread([this]()
                                    { this->_run(); });

        pthread_setname_np(this->_worker.native_handle(), "db-worker");
    }

    worker::~worker()
    {
        this->shutdown();
    }

    void worker::submit(std::function<void()> job)
    {
        // prof::counter_guard guard(prof::get<"submit_job">());

        // size_t current_job_count = this->_job_count.fetch_add(1, std::memory_order::relaxed) + 1;
        this->_job_count.fetch_add(1, std::memory_order::relaxed);

        // try to push to fast queue first
        bool ok = this->_fast_job_queue.push_back(job);

        // if fast queue is full, push to normal queue
        if(!ok)
        {
            std::lock_guard lk(this->_queue_m);

            this->_job_queue.emplace_back(std::move(job));
        }
    }

    void worker::shutdown()
    {
        {
            std::unique_lock lk(this->_queue_m);

            if(!this->_running)
                return;

            this->_running = false;
        }

        this->_job_count.store(0, std::memory_order::relaxed);

        if(this->_worker.joinable())
            this->_worker.join();
    }

    void worker::_run()
    {
        while(true)
        {
            std::deque<job_t> fetched_tasks;

            // Fetch from fast queue
            std::optional<job_t> job_from_fast_queue;
            while(true)
            {
                job_from_fast_queue = this->_fast_job_queue.pop_front();

                if(!job_from_fast_queue.has_value())
                    break;

                fetched_tasks.emplace_back(std::move(job_from_fast_queue.value()));
            }

            // Fetch from deque
            {
                std::unique_lock lk(this->_queue_m);

                while(!this->_job_queue.empty())
                    fetched_tasks.emplace_back(pop_front(this->_job_queue));
            }

            while(!fetched_tasks.empty())
            {
                pop_front(fetched_tasks)();

                this->_job_count.fetch_sub(1, std::memory_order::relaxed);
            }

            while(this->_job_count.load(std::memory_order::relaxed) == 0 && this->_running)
                std::this_thread::yield();

            {
                std::unique_lock lk(this->_queue_m);
                if(this->_job_queue.empty() && this->_fast_job_queue.empty() && !this->_running)
                    return;
            }
        }
    }

} // namespace hedge::async