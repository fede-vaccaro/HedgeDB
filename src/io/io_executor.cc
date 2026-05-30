#include <cstddef>
#include <numeric>
#include <ranges>

#include "io/io_executor.h"
#include "tmc/ex_cpu.hpp"
#include "tmc/topology.hpp"

namespace hedge::io
{
    std::atomic_bool io_executor::VERBOSE = false;

    io_executor::io_executor(const executor_config& cfg)
    {
        this->init(cfg);
    }

    io_executor& io_executor::init(const executor_config& cfg)
    {
        bool expected = false;
        if(!this->_initialized.compare_exchange_strong(expected, true))
            return *this;

#ifdef TMC_USE_HWLOC
        if(io_executor::VERBOSE)
        {
            std::cout << "Executor auto_detect is " << (cfg.auto_detect ? "enabled" : "disabled") << "\n";
        }

        tmc::topology::topology_filter filter{};
        if(cfg.auto_detect)
            filter = this->_hwloc_partition_filter(cfg);
#else
        if(cfg.auto_detect)
            throw std::runtime_error("Auto-detecting topology requires hwloc support.");
#endif

        if(!cfg.auto_detect)
        {
            switch(cfg.type)
            {
                case executor_type::FOREGROUND:
                    this->_n_threads = cfg.n_threads.value_or(std::thread::hardware_concurrency() / 2);
                    break;
                case executor_type::BACKGROUND:
                    this->_n_threads = cfg.n_threads.value_or(std::thread::hardware_concurrency() / 2);
                    break;
                case executor_type::GENERAL_PURPOSE:
                    this->_n_threads = cfg.n_threads.value_or(std::thread::hardware_concurrency());
                    break;
            }
        }

        this->_queue_depth = cfg.queue_depth;
        this->name_prefix = cfg.name;

        assert(this->_n_threads > 0);
        this->_ctxs.resize(this->_n_threads);

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
            .set_work_stealing_strategy(tmc::work_stealing_strategy::HIERARCHY_MATRIX)
            .set_spins(32);

#ifdef TMC_USE_HWLOC
        if(cfg.auto_detect)
        {
            this->_ex.add_partition(filter)
                .set_thread_packing_strategy(tmc::topology::thread_packing_strategy::FAN)
                .set_thread_pinning_level(tmc::topology::thread_pinning_level::CORE);
        }
#endif

        this->_ex.init();
        return *this;
    }

    void io_executor::shutdown()
    {
        this->_ex.teardown();
        this->_ctxs.clear();
        this->_initialized.store(false);
    }

    io_executor::~io_executor()
    {
        if(this->_initialized.load())
            this->shutdown();
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

#ifdef TMC_USE_HWLOC
    // This function creates a topology filter for hybrid CPUs (i.e. Intel Core with P and E cores) based on the executor type.
    [[nodiscard]] tmc::topology::topology_filter io_executor::_hwloc_partition_filter_hybrid_cpu(const executor_config& cfg)
    {
        auto filter = tmc::topology::topology_filter{};
        auto topology = tmc::topology::query();

        if(io_executor::VERBOSE)
        {
            std::cout << "Detected hybrid CPU topology: "
                      << topology.core_count() << " cores, "
                      << topology.group_count() << " groups, "
                      << topology.numa_count() << " NUMA nodes\n";
        }

        auto resolve_kind_to_group = [&topology](tmc::topology::cpu_kind::value kind)
        {
            std::vector<size_t> groups;
            size_t count{0};
            for(const auto& group : topology.groups | std::views::filter([kind](const tmc::topology::core_group& g)
                                                                         { return (g.cpu_kind & kind) != 0; }))
            {
                groups.emplace_back(group.index);
                count += group.core_indexes.size() * group.smt_level;
            }

            return std::make_pair(groups, count);
        };

        tmc::topology::cpu_kind::value kind = tmc::topology::cpu_kind::ALL;
        switch(cfg.type)
        {
            case executor_type::FOREGROUND:
                kind = tmc::topology::cpu_kind::PERFORMANCE;
                break;
            case executor_type::BACKGROUND:
                kind = tmc::topology::cpu_kind::EFFICIENCY1;
                break;
            case executor_type::GENERAL_PURPOSE:
                kind = tmc::topology::cpu_kind::ALL;
                break;
        }

        auto [groups, count] = resolve_kind_to_group(kind);
        filter.set_group_indexes(groups);

        if(cfg.n_threads.has_value() && cfg.n_threads.value() < count)
            count = cfg.n_threads.value();

        this->_n_threads = count;
        assert(this->_n_threads > 0);

        // debug
        if(io_executor::VERBOSE)
        {
            std::cout << "Allowed groups for executor type " << to_string(cfg.type) << ": ";
            for(size_t group_index : groups)
                std::cout << group_index << " ";
            std::cout << "\nTotal threads allowed for this executor: " << count << "\n";
        }
        return filter;
    }

    // This function creates a topology filter for normal CPUs (i.e. a single type of core) by splitting the cores between foreground and background executors.
    [[nodiscard]] tmc::topology::topology_filter io_executor::_hwloc_partition_filter_normal_cpu(const executor_config& cfg)
    {
        if(io_executor::VERBOSE)
        {
            std::cout << "Detected normal CPU topology. Total cores: " << tmc::topology::query().core_count() << "\n";
        }

        auto filter = tmc::topology::topology_filter{};
        auto topology = tmc::topology::query();

        // idx == -1 means not found
        // idx >   0 is the group id
        auto find_core_group = [&topology](size_t core_index) -> int32_t
        {
            for(const auto& group : topology.groups)
            {
                if(std::ranges::find(group.core_indexes, core_index) == group.core_indexes.end())
                    continue;

                return static_cast<int32_t>(group.index);
            }

            return -1;
        };

        // 0 means error
        // 1 returns no SMT
        // 2 returns SMT available
        auto resolve_core_smt_level = [&topology, find_core_group](size_t core_index) -> int32_t
        {
            const int32_t group_idx = find_core_group(core_index);
            if(group_idx < 0)
                return 0;

            return static_cast<int32_t>(topology.groups[group_idx].smt_level);
        };

        switch(cfg.type)
        {

            case executor_type::FOREGROUND:
            {
                // set lower half of cores for foreground, higher half for background
                auto core_indexes = std::vector<size_t>(topology.core_count() / 2);
                std::ranges::iota(core_indexes, 0);

                if(cfg.n_threads.has_value() && cfg.n_threads.value() < core_indexes.size())
                    core_indexes.resize(cfg.n_threads.value());

                filter.set_core_indexes(core_indexes);
                this->_n_threads = std::accumulate(
                    core_indexes.begin(),
                    core_indexes.end(),
                    0UL,
                    [resolve_core_smt_level](size_t acc, size_t core_index) -> size_t
                    {
                        auto smt_level = resolve_core_smt_level(core_index);
                        assert(smt_level != 0);
                        return acc + smt_level;
                    });

                if(io_executor::VERBOSE)
                {
                    std::cout << "Allowed cores for foreground executor: ";
                    for(size_t core_index : core_indexes)
                        std::cout << core_index << " (group : " << find_core_group(core_index) << ") ";
                    std::cout << "\nTotal threads allowed for foreground executor: " << this->_n_threads << "\n";
                }
                break;
            }
            case executor_type::BACKGROUND:
            {
                // set higher half of cores for background, lower half for foreground
                auto core_indexes = std::vector<size_t>(topology.core_count() / 2);
                std::ranges::iota(core_indexes, topology.core_count() / 2);
                if(cfg.n_threads.has_value() && cfg.n_threads.value() < core_indexes.size())
                    core_indexes.resize(cfg.n_threads.value());

                filter.set_core_indexes(core_indexes);
                this->_n_threads = std::accumulate(
                    core_indexes.begin(),
                    core_indexes.end(),
                    0UL,
                    [resolve_core_smt_level](size_t acc, size_t core_index) -> size_t
                    {
                        auto smt_level = resolve_core_smt_level(acc);
                        assert(smt_level != 0);
                        return acc + resolve_core_smt_level(core_index);
                    });

                // debug
                if(io_executor::VERBOSE)
                {
                    std::cout << "Allowed cores for background executor: ";
                    for(size_t core_index : core_indexes)
                        std::cout << core_index << " (group : " << find_core_group(core_index) << ") ";
                    std::cout << "\nTotal threads allowed for background executor: " << this->_n_threads << "\n";
                }
                break;
            }
            case executor_type::GENERAL_PURPOSE:
            {
                this->_n_threads = topology.core_count();
                break;
            }
        }

        return filter;
    }

    tmc::topology::topology_filter io_executor::_hwloc_partition_filter(const executor_config& cfg)
    {
        auto topology = tmc::topology::query();

        if(!cfg.auto_detect)
            return {};

        return topology.is_hybrid()
                   ? this->_hwloc_partition_filter_hybrid_cpu(cfg)
                   : this->_hwloc_partition_filter_normal_cpu(cfg);
    }
#endif

    std::string to_string(executor_type type)
    {
        switch(type)
        {
            case executor_type::FOREGROUND:
                return "FOREGROUND";
            case executor_type::BACKGROUND:
                return "BACKGROUND";
            case executor_type::GENERAL_PURPOSE:
                return "GENERAL_PURPOSE";
            default:
                return "UNKNOWN";
        }
    }

} // namespace hedge::io