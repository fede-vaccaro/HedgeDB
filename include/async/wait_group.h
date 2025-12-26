#pragma once

#include <atomic>
#include <condition_variable>

namespace hedge::async
{
    class wait_group
    {
        std::atomic_uint64_t _counter{0};
        std::atomic_bool _done{false};

        // needed for the wait for
        std::condition_variable _cv;
        std::mutex _mutex;

        wait_group() = default;

    public:
        static std::shared_ptr<wait_group> make_shared()
        {
            return std::shared_ptr<wait_group>(new wait_group());
        }

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
            auto cntr = this->_counter.fetch_sub(1) - 1;

            bool done = (cntr == 0);

            if(done)
            {
                this->_done = true;
                this->_done.notify_all();
                this->_cv.notify_all();
            }
        }

        void wait()
        {
            if(this->_done.load())
                return;
            this->_done.wait(true);
        }

        bool wait_for(std::chrono::milliseconds timeout)
        {
            std::unique_lock lk(this->_mutex);
            return this->_cv.wait_for(lk, timeout, [this]()
                                      { return this->_done.load(); });
        }
    };

} // namespace hedge::async
