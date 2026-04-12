#include <cstddef>

#include "io/io_executor.h"
#include "tmc/ex_cpu.hpp"

namespace hedge::io
{
    io_executor::io_executor(uint32_t n_threads, uint32_t queue_depth, std::optional<std::string> name)
    {
        this->init(n_threads, queue_depth, std::move(name));
    }

    io_executor& io_executor::init(uint32_t n_threads, uint32_t queue_depth, std::optional<std::string> name)
    {
        bool expected = false;
        if(!this->_initialized.compare_exchange_strong(expected, true))
            return *this;

        this->_n_threads = n_threads;
        this->_queue_depth = queue_depth;
        this->name_prefix = std::move(name).value_or("");

        this->_ctxs.resize(n_threads);

        this->_ex.set_thread_count(this->num_threads())
            .set_thread_init_hook(
                [this](size_t id)
                {
                    auto& ctx = (this->_ctxs[id] = std::make_unique<io_ctx>(this->queue_depth()));
                    io_ctx::set_thread_local(ctx.get());
                    if(!this->name_prefix.empty())
                    {
                        thread_local std::string thread_name = this->name_prefix + "-" + std::to_string(id);
                        pthread_setname_np(pthread_self(), thread_name.c_str());
                    }
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
            .set_spins(32)
            .init();

        return *this;
    }

    io_executor::~io_executor()
    {
        this->_ex.teardown();
        this->_ctxs.clear();
    }

    void set_thread_affinity(std::pair<int32_t, int32_t> cpu_range)
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for(int32_t cpu = cpu_range.first; cpu <= cpu_range.second; ++cpu)
            CPU_SET(cpu, &cpuset);

        pthread_t thread_id = pthread_self();
        int result = pthread_setaffinity_np(thread_id, sizeof(cpu_set_t), &cpuset);
        if(result != 0)
        {
            throw std::runtime_error("Failed to set thread affinity: " + std::string(strerror(result)));
        }
    };

} // namespace hedge::io