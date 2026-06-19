#pragma once

#include "io/io_ctx.h"

#include <functional>
#include <stdexcept>
#include <type_traits>

#include <tmc/detail/thread_locals.hpp>
#include <tmc/ex_manual_st.hpp>
#include <tmc/task.hpp>

namespace hedge
{

    class db_ctx
    {
        inline static thread_local db_ctx* this_thread_ctx = nullptr;

        std::unique_ptr<io::io_ctx> _ctx;
        tmc::ex_manual_st _ex;

    public:
        void init(size_t queue_depth = 16)
        {
            if(db_ctx::this_thread_ctx != nullptr)
                throw std::runtime_error("db_ctx already set for this thread");

            this->_ctx = std::make_unique<io::io_ctx>(queue_depth);
            this->_ex.init();

            db_ctx::this_thread_ctx = this;
            io::io_ctx::set_thread_local(this->_ctx.get());
            tmc::detail::this_thread::executor() = this->_ex.type_erased();
        }

        template <typename T>
            requires(!std::is_void_v<T>)
        void post(tmc::task<T> task, std::function<void(T)> callback)
        {
            auto wrap = [](tmc::task<T> t, std::function<void(T)> cb) -> tmc::task<void>
            {
                cb(co_await std::move(t));
            };
            this->_ex.post(wrap(std::move(task), std::move(callback)));
        }

        void post(tmc::task<void> task, std::function<void()> callback)
        {
            auto wrap = [](tmc::task<void> t, std::function<void()> cb) -> tmc::task<void>
            {
                co_await std::move(t);
                cb();
            };
            this->_ex.post(wrap(std::move(task), std::move(callback)));
        }

        size_t tick()
        {
            this->_ex.run_all();

            if(this->_ctx->in_flight_count() == 0 && this->_ctx->pending_count() == 0)
                return 0;

            return this->_ctx->submit_and_wait();
        }

        void shutdown()
        {
            if(this_thread_ctx == nullptr)
                return;

            this->_ex.teardown();
            this->_ctx.reset();

            io::io_ctx::set_thread_local(nullptr);
            tmc::detail::this_thread::executor() = nullptr;
            this_thread_ctx = nullptr;
        }

        ~db_ctx()
        {
            this->shutdown();
        }
    };

} // namespace hedge
