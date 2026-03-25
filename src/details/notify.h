#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "io_executor.h"

using namespace std::string_literals;

namespace hedge::async::detail
{

    class notifier
    {
    protected:
        std::vector<executor_context*> _contexts;
        int32_t _mutex_id = -1;

        void ensure_registered()
        {
            if(std::atomic_ref<int32_t>(_mutex_id).load(std::memory_order::relaxed) == -1)
                executor_context::register_mutex(&this->_mutex_id);
        }

        void wake_up(io_uring* ring)
        {
            uint64_t msg = static_cast<uint64_t>(this->_mutex_id) | executor_context::WAKE_MSG_BIT_FLAG;

            size_t sent = 0;
            while(sent < this->_contexts.size())
            {
                for(size_t i = sent; i < this->_contexts.size(); i++)
                {
                    io_uring_sqe* sqe = io_uring_get_sqe(ring);
                    if(sqe == nullptr)
                        break;

                    io_uring_prep_msg_ring(sqe, this->_contexts[i]->uring_fd(), 0, msg, 0);
                    io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
                    sent++;
                }

                int32_t res = io_uring_submit(ring);
                if(res < 0)
                    throw std::runtime_error("io_uring_submit: "s + strerror(-res));
            }

            for(auto* ex : this->_contexts)
                ex->wake();
        }

    public:
        notifier() = default;
        notifier(std::vector<executor_context*> ctxs) : _contexts(std::move(ctxs)) {}
        notifier(const notifier&) = delete;

        void notify_all(io_uring* sender)
        {
            this->wake_up(sender);
        }

        async::task<> notify_all()
        {
            this->wake_up(async::this_thread_executor()->ring());
            co_return;
        }
    };

} // namespace hedge::async::detail
