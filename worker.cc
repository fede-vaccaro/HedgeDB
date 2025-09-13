#include <functional>

#include "worker.h"

namespace hedge::async
{

    template <typename T>
    T pop(std::deque<T>& queue)
    {
        auto val = std::move(queue.back());
        queue.pop_back();
        return std::move(val);
    }

    worker::worker()
    {
        this->_worker = std::thread([this]()
                                    { this->_run(); });
    }

    worker::~worker()
    {
        this->shutdown();
    }

    void worker::submit(std::function<void()> job)
    {
        {
            std::unique_lock lk(this->_queue_m);

            if(!this->_running)
                return;

            this->_cv.wait(lk, [this]()
                           { return this->_job_queue.size() < MAX_JOBS; });

            this->_job_queue.emplace_back(std::move(job));
        }

        this->_cv.notify_all();
    }

    void worker::shutdown()
    {
        {
            std::unique_lock lk(this->_queue_m);

            if(!this->_running)
                return;

            this->_running = false;
        }

        this->_cv.notify_all();

        if(this->_worker.joinable())
            this->_worker.join();
    }

    void worker::_run()
    {
        while(true)
        {
            std::deque<job_t> fetched_tasks;

            {
                std::unique_lock lk(this->_queue_m);
                this->_cv.wait(lk, [this]()
                               { return this->_job_queue.size() > 0 || !this->_running; });

                while(!this->_job_queue.empty())
                    fetched_tasks.emplace_back(pop(this->_job_queue));
            }

            this->_cv.notify_all();

            while(!fetched_tasks.empty())
                pop(fetched_tasks)();

            {
                std::unique_lock lk(this->_queue_m);
                if(this->_job_queue.empty() && !this->_running)
                    return;
            }
        }
    }

} // namespace hedge::db