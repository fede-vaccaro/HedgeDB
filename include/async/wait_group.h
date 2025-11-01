#pragma once

#include <atomic>
#include <condition_variable>

namespace hedge::async
{
    class wait_group
    {
        std::atomic_uint64_t _counter{0};
        std::atomic_bool _done{false};

        std::condition_variable _cv;
        std::mutex _mutex;

    public:
        void set(size_t count)
        {
            this->_counter = count;
        }

        void incr()
        {
            this->_counter++;
        }

        void decr()
        {
            this->_counter--;

            this->_done = this->_counter == 0;

            if(this->_done)
                this->_cv.notify_all();
        }

        void wait()
        {
            std::unique_lock lk(this->_mutex);
            this->_cv.wait(lk, [this]()
                           { return this->_done.load(); });
        }

        bool wait_for(std::chrono::milliseconds timeout)
        {
            std::unique_lock lk(this->_mutex);
            return this->_cv.wait_for(lk, timeout, [this]()
                                      { return this->_done.load(); });
        }
    };

} // namespace hedge::async
