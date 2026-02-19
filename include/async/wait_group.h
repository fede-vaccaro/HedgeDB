#pragma once

#include <atomic>
#include <condition_variable>

namespace hedge::async
{
    // TODO: this is slow, ugly (should use RAII) and buggy
    class wait_group
    {
        std::atomic_uint64_t _counter{0};
        std::atomic_bool _done{false};

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
            this->_counter.store(count);
            this->_done.store(false);
        }

        void decr()
        {
            this->_counter.fetch_sub(1);

            this->_done.store(this->_counter.load() == 0);

            if(this->_done.load())
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
