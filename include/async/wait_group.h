#pragma once

#include <atomic>
#include <condition_variable>

namespace hedge::async
{
    // TODO: this is slow, ugly (should use RAII) and buggy
    class wait_group
    {
        std::atomic_uint64_t _counter{0};
        std::atomic_bool _done{true};

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
            this->_done.store(count == 0, std::memory_order::seq_cst);
        }

        // TODO: make some wait_group_guard that automatically calls decr on destruction
        void decr()
        {
            // acq_rel: the release ensures all work done before decr() is visible to the
            // thread that observes c==0; the acquire chains with prior releases so that
            // each decrement sees a consistent counter value.
            // On x86 a LOCK RMW already implies a full barrier, so this is free.
            size_t c = this->_counter.fetch_sub(1, std::memory_order::acq_rel) - 1;
            if(c == 0)
            {
                this->_done.store(true, std::memory_order::release);
                this->_done.notify_all();
                this->_cv.notify_all();
            }
        }

        void wait()
        {
            // std::unique_lock lk(this->_mutex);
            // this->_cv.wait(lk, [this]()
            //    { return this->_done.load(std::memory_order::relaxed); });
            this->_done.wait(false, std::memory_order::acquire);
        }

        bool wait_for(std::chrono::milliseconds timeout)
        {
            std::unique_lock lk(this->_mutex);
            return this->_cv.wait_for(lk, timeout, [this]()
                                      { return this->_done.load(std::memory_order::seq_cst); });
        }
    };

} // namespace hedge::async
