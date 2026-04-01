#pragma once

#include "io_ctx.h"
#include "tmc/detail/concepts_awaitable.hpp"
#include "tmc/ex_cpu.hpp"
#include "tmc/work_item.hpp"

namespace hedge::io
{
    class io_executor
    {
        friend struct tmc::detail::executor_traits<io_executor>;

        uint32_t _queue_depth;
        uint32_t _n_threads;
        std::vector<std::unique_ptr<io_ctx>> _ctxs;
        tmc::ex_cpu _ex;
        std::atomic_bool _initialized{false};
        std::string name_prefix;

    public:
        explicit io_executor(uint32_t n_threads, uint32_t queue_depth, std::optional<std::string> name = std::nullopt);

        io_executor() = default;
        io_executor& init(uint32_t n_threads, uint32_t queue_depth, std::optional<std::string> name = std::nullopt);

        [[nodiscard]] uint32_t num_threads() const
        {
            assert(this->_initialized);
            return this->_n_threads;
        }

        [[nodiscard]] uint32_t queue_depth() const
        {
            assert(this->_initialized);
            return this->_queue_depth;
        }

        [[nodiscard]] tmc::ex_cpu& ex()
        {
            return this->_ex;
        }

        ~io_executor();
    };

    void set_thread_affinity(std::pair<int32_t, int32_t> cpu_range);

    inline void set_thread_affinity(int32_t tid)
    {
        set_thread_affinity({tid, tid});
    }

} // namespace hedge::io

template <>
struct tmc::detail::executor_traits<hedge::io::io_executor>
{
    static void post(hedge::io::io_executor& io_ex, tmc::work_item&& Item, size_t Priority, size_t ThreadHint)
    {
        io_ex._ex.post(static_cast<tmc::work_item&&>(Item), Priority, ThreadHint);
    }

    template <typename It>
    static void post_bulk(
        hedge::io::io_executor& io_ex, It&& Items, size_t Count, size_t Priority,
        size_t ThreadHint)
    {
        io_ex._ex.post_bulk(static_cast<It&&>(Items), Count, Priority, ThreadHint);
    }

    static tmc::ex_any* type_erased(hedge::io::io_executor& io_ex)
    {
        return io_ex._ex.type_erased();
    }

    static TMC_DECL std::coroutine_handle<>
    dispatch(hedge::io::io_executor& ex, std::coroutine_handle<> Outer, size_t Priority)
    {
        if(tmc::detail::this_thread::exec_prio_is(ex._ex.type_erased(), Priority))
            return Outer;

        tmc::post(ex._ex, static_cast<std::coroutine_handle<>&&>(Outer), Priority);
        return std::noop_coroutine();
    }
};