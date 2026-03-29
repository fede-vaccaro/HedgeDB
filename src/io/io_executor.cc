#include "io/io_executor.h"

namespace hedge::io
{
    io_executor::io_executor(uint32_t n_threads, uint32_t queue_depth) : _queue_depth(queue_depth), _n_threads(n_threads), _ctxs(n_threads)
    {
        this->_ex.set_thread_count(n_threads)
            .set_thread_init_hook(
                [this](size_t id)
                {
                    auto& ctx = (this->_ctxs[id] = std::make_unique<io_ctx>(this->_queue_depth));
                    io_ctx::set_thread_local(ctx.get());
                })
            .set_thread_teardown_hook(
                [](size_t)
                {
                    io_ctx::set_thread_local(nullptr);
                })
            .set_thread_post_run_hook(
                [this](size_t tid) -> bool
                {
                    return this->_ctxs[tid]->submit_and_wait();
                })
            .init();
    }

    io_executor::~io_executor()
    {
        this->_ex.teardown();
        this->_ctxs.clear();
    }

} // namespace hedge::io