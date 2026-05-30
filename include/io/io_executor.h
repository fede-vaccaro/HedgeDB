#pragma once

#include "io/io_ctx.h"
#include "tmc/detail/concepts_awaitable.hpp"
#include "tmc/ex_cpu.hpp"
#include "tmc/topology.hpp"
#include "tmc/work_item.hpp"

namespace hedge::io
{

    enum class executor_type : uint8_t
    {
        FOREGROUND,
        BACKGROUND,
        GENERAL_PURPOSE
    };

    std::string to_string(executor_type type);

    struct executor_config
    {
#ifdef TMC_USE_HWLOC
        static constexpr bool AUTO_DETECT_DEFAULT{true};
#else
        static constexpr bool AUTO_DETECT_DEFAULT{false};
#endif

        std::string name{"hdb-pool"};
        size_t queue_depth{8};
        executor_type type{executor_type::GENERAL_PURPOSE};
        std::optional<size_t> n_threads{std::nullopt};
        bool auto_detect{AUTO_DETECT_DEFAULT};
    };
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
        static std::atomic_bool VERBOSE;

        explicit io_executor(const executor_config& cfg);

        io_executor() = default;
        io_executor& init(const executor_config& cfg);

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

        void shutdown();

        ~io_executor();

    private:
#ifdef TMC_USE_HWLOC
        [[nodiscard]] tmc::topology::topology_filter _hwloc_partition_filter_hybrid_cpu(const executor_config& cfg);
        [[nodiscard]] tmc::topology::topology_filter _hwloc_partition_filter_normal_cpu(const executor_config& cfg);
        [[nodiscard]] tmc::topology::topology_filter _hwloc_partition_filter(const executor_config& cfg);
#endif
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