#pragma once

#include "details/notify.h"
#include "io_executor.h"
#include "task.h"

namespace hedge::async
{
    class cond_var : private detail::notifier
    {
    public:
        using detail::notifier::notifier;
        using detail::notifier::notify_all;

        template <typename LOCK, typename PRED>
        async::task<> wait(LOCK& lk, PRED p)
        {
            this->ensure_registered();

            while(!p())
            {
                co_await async::this_thread_executor()->enque_coro(lk, this->_mutex_id);

                lk.lock();
            }
        }
    };
} // namespace hedge::async
