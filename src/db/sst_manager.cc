#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <ranges>
#include <shared_mutex>
#include <unistd.h>

#include <error.hpp>

#include "db/range_iterator.h"
#include "db/sst.h"
#include "fs/fs.hpp"
#include "index_ops.h"
#include "io/io_executor.h"
#include "io/io_requests.hpp"
#include "perf_counter.h"
#include "sst_manager.h"
#include "tmc/aw_resume_on.hpp"
#include "tmc/latch.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/sync.hpp"
#include "types.h"
#include "utils.h"
#include "xxh64.hpp"

namespace hedge::db
{

    sst_manager::sst_manager(const config& cfg,
                             std::shared_ptr<io::io_executor> compaction_pool,
                             std::shared_ptr<sharded_page_cache> page_cache)
        : _cfg(cfg), _compaction_pool(std::move(compaction_pool)), _page_cache(std::move(page_cache))
    {
        size_t num_partitions = 1UL << cfg.num_partition_exponent;
        size_t partition_size = (1 << 16) / num_partitions;
        for(size_t i = 0; i < num_partitions; ++i)
        {
            auto partition_id = static_cast<uint16_t>(((i + 1) * partition_size) - 1);
            auto state = std::make_unique<_partition_state>();
            state->levels.resize(cfg.max_num_levels);
            state->permissions.resize(cfg.max_num_levels);
            state->created.resize(cfg.max_num_levels, 0);
            for(auto& s : state->permissions)
                s = std::make_shared<tmc::semaphore>(1);
            state->stats_per_lvl.resize(cfg.max_num_levels);

            this->_sorted_indices.emplace(partition_id, std::move(state));
        }
    }

    void sst_manager::launch_compaction_worker()
    {
        auto make_compaction_job = [](sst_manager* sst_manager) -> tmc::task<void>
        {
            while(true)
            {
                auto compact_all = co_await sst_manager->_compaction_scheduler_signal_chan.pull();

                if(!compact_all)
                    break;

                if(auto ok = sst_manager->_schedule_compaction(compact_all.value()); !ok)
                    sst_manager->_logger.log("Compaction job failed: ", ok.error().to_string());

                sst_manager->_compaction_jobs_in_flight.fetch_sub(1, std::memory_order::relaxed);
            }
        };

        tmc::post(*this->_compaction_pool, make_compaction_job(this));
    }

    tmc::task<void> sst_manager::push_new_ssts_to_l0(std::vector<sst> new_ssts, std::optional<compaction_stats> flush_stats_opt)
    {
        std::vector<uint16_t> affected_partitions;

        // The flush stats are already aggregated across partitions
        // However, stats are counted per partition
        // We adapt the stats to this scheme
        if (flush_stats_opt)
        {
            flush_stats_opt->input_bytes /= new_ssts.size();
            flush_stats_opt->output_bytes /= new_ssts.size();
        }

        for(auto& new_sorted_index : new_ssts)
        {
            auto prefix = new_sorted_index.upper_bound();

            auto sorted_index_ptr = std::make_shared<sst>(std::move(new_sorted_index));

            auto it = this->_sorted_indices.find(prefix);
            assert(it != this->_sorted_indices.end() && "Partition not found in pre-initialized map");

            {
                auto& state = *it->second;
                std::lock_guard lk(state.mutex);
                auto& l0 = state.levels[0];
                l0.emplace_back(std::move(sorted_index_ptr));
                state.levels_seq_num.fetch_add(1, std::memory_order::release);
                affected_partitions.push_back(prefix);

                if (flush_stats_opt.has_value())
                    state.stats_per_lvl[0].emplace_back(*flush_stats_opt);
            }
        }

        if(affected_partitions.empty())
            return tmc::task<void>{};

        // Use latch to wait for persist tasks to complete on compaction executor
        std::unique_ptr<tmc::latch> completion_latch = std::make_unique<tmc::latch>(affected_partitions.size());

        auto persist_partition = [](sst_manager* mgr, uint16_t pid, tmc::latch* latch) -> tmc::task<void>
        {
            auto status = co_await mgr->_persist_partition_state(pid);
            if(!status)
                mgr->_logger.log("Failed to persist partition state after flush: ", status.error().to_string());
            latch->count_down();
        };

        for(unsigned short affected_partition : affected_partitions)
        {
            tmc::post(*this->_compaction_pool, persist_partition(this, affected_partition, completion_latch.get()));
        }

        // Wait for all persist tasks to complete
        return [](std::unique_ptr<tmc::latch> latch) -> tmc::task<void>
        {
            co_await *latch;
        }(std::move(completion_latch));
    }

    hedge::expected<partition_t> sst_manager::acquire_partition_snapshot(size_t partition_id) const
    {
        auto sorted_indices_it = this->_sorted_indices.find(partition_id);
        if(sorted_indices_it == this->_sorted_indices.end())
            return hedge::error("Partition not found in sorted indices", errc::KEY_NOT_FOUND);

        auto& state = *sorted_indices_it->second;
        std::shared_lock lk(state.mutex);
        return state.levels;
    }

    tmc::task<expected<value_t>> sst_manager::lookup_async(const key_t& key, size_t partition_id)
    {
        auto maybe_partition = this->acquire_partition_snapshot(partition_id);
        if(!maybe_partition)
            co_return hedge::error(maybe_partition.error());

        auto partition = std::move(maybe_partition.value());

        // Note: we are iterating in reverse order to check the newest indices first, as they are more likely to contain the key due to temporal locality.

        // Indices are sorted by epoch: newest (and most recent key first)
        uint64_t key_hash = xxh64::hash((const char*)key.data(), key.size(), sst::QF_SEED);

        std::optional<value_t> value_opt;

        for(const auto& [level_idx, level] : partition | std::views::enumerate)
        {
            for(const auto& [sst_index, sst_ptr] : level | std::views::enumerate | std::views::reverse)
            {
                if(!sst_ptr->probe_filter(key_hash))
                    continue;

                auto maybe_value_ptr = co_await sst_ptr->lookup_async(key, this->_page_cache);
                if(!maybe_value_ptr.has_value() && maybe_value_ptr.error().code() != errc::KEY_NOT_FOUND)
                    co_return hedge::error(std::format("An error occurred while reading index at path {}: {}", sst_ptr->path().string(), maybe_value_ptr.error().to_string()));

                if(maybe_value_ptr.has_value())
                {
                    if(std::holds_alternative<tombstone_t>(maybe_value_ptr.value()))
                    {
                        value_opt = tombstone_t{};
                        break;
                    }
                    value_opt = maybe_value_ptr.value();
                    break;
                }
            }

            if(value_opt.has_value())
                break;
        }

        if(!value_opt.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        co_return std::move(value_opt.value());
    }

    hedge::expected<range_iterator> sst_manager::make_range_iterator(std::optional<key_t> lower, std::optional<key_t> upper, size_t matching_partition_id, size_t read_ahead_size) const
    {
        auto maybe_partition = this->acquire_partition_snapshot(matching_partition_id);
        if(!maybe_partition)
            return hedge::error(maybe_partition.error());

        auto partition = std::move(maybe_partition.value());
        return range_iterator::make_new(nullptr, &partition, std::move(lower), std::move(upper), read_ahead_size);
    }

    tmc::task<void> sst_manager::_make_compaction_task(
        std::shared_ptr<tmc::semaphore> can_write,
        std::shared_ptr<tmc::semaphore> next_can_write,
        size_t level,
        std::vector<sst_ptr_t> inputs,
        bool discard_deleted_keys)
    {
        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = this->_cfg.compaction_read_ahead_size_bytes,
            .new_index_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed),
            .base_path = this->_cfg.partitions_path,
            .discard_deleted_keys = discard_deleted_keys,
            .create_new_with_odirect = this->_cfg.use_odirect_for_ssts,
            .populate_cache_with_output = false,
        };

        compaction_stats compaction_stats;

        std::vector<const sst*> input_ptrs;
        input_ptrs.reserve(inputs.size());
        for(const auto& i : inputs)
        {
            input_ptrs.emplace_back(i.get());
            compaction_stats.input_bytes += i->file_size();
        }

        compaction_stats.num_inputs = inputs.size();
        compaction_stats.time_start = std::chrono::high_resolution_clock::now();
        hedge::expected<sst> merge_result =
            co_await index_ops::k_way_merge_async2(merge_config,
                                                   input_ptrs,
                                                   this->_page_cache);
        compaction_stats.time_end = std::chrono::high_resolution_clock::now();

        std::vector<size_t> input_file_ids;
        input_file_ids.reserve(inputs.size());
        for(const auto& i : inputs)
            input_file_ids.emplace_back(i->id());

        // Merge went ok (including file writing and flushing)
        if(merge_result)
        {
            auto output_ptr = std::make_shared<sst>(std::move(merge_result.value()));
            compaction_stats.output_bytes = output_ptr->file_size();
            compaction_stats.items_merged = output_ptr->size();

            // Apply index update under exclusive lock — all inputs belong to the same partition
            auto partition_prefix = inputs[0]->upper_bound();
            auto partition_it = this->_sorted_indices.find(partition_prefix);
            auto& state = *partition_it->second;

            // Wait for permission
            co_await *can_write;
            std::unique_lock lk(state.mutex);

            auto& sst_level = state.levels[level];

            // Remove inputs by File ID (safe even if other jobs inserted into the middle of the range)
            std::erase_if(sst_level, [&input_file_ids](const sst_ptr_t& sst)
                          { return std::ranges::find(input_file_ids, sst->id()) != input_file_ids.end(); });

            const size_t next_level_idx = std::min(level + 1, this->_cfg.max_num_levels - 1);

            auto& target_level = state.levels[next_level_idx];
            target_level.push_back(std::move(output_ptr));
            state.levels_seq_num.fetch_add(1, std::memory_order::release);

            if(this->_cfg.acquire_compaction_stats)
                state.stats_per_lvl[next_level_idx].emplace_back(compaction_stats);

            lk.unlock();

            // Give the permission to next in the compaction chain
            next_can_write->release();

            this->schedule_compaction(false);

            auto persist_status = co_await _persist_partition_state(partition_prefix);
            if(!persist_status)
            {
                this->_logger.log("Failed to persist partition state: ", persist_status.error().to_string());
            }
            else
            {
                for(const auto& input : inputs)
                    unlink(input->path().c_str());
            }
        }
        else
        {
            // Handle error case — no write occurred, but still forward the permission
            // so the chain is never broken (failure to release here deadlocks all
            // subsequent tasks on this (partition, level) chain)
            for(const auto& input : inputs)
                input->set_compaction_state(compaction_progress_state::COMPACTION_ERROR);

            this->_logger.log("Self-completing compaction failed: ", merge_result.error().to_string());
        }

        if(level == 0)
            this->_update_pending_compactions_counter(-static_cast<int32_t>(inputs.size())); // This might release backpressure

        std::lock_guard total_lk(this->_total_pending_compactions_mutex);
        this->_total_pending_compacting_sst_count -= static_cast<int64_t>(inputs.size());
        this->_pending_compactions_cv.notify_all();
    }

    // Serialization format for partition state
    // Each level is represented as a line of text, with SST filenames separated by ';'
    // e.g.
    // (level_0) sst_00012.sst;sst_00015.sst;sst_00020.sst\n
    // (level_1) sst_00010.sst;sst_00018.sst\n
    // ...
    // Unused levels are represented as empty lines
    std::string sst_manager::_serialize_levels(const partition_t& levels)
    {
        std::string result;
        for(const auto& level : levels)
        {
            bool first = true;
            for(const auto& sst_ptr : level)
            {
                if(!first)
                    result += ';';
                result += sst_ptr->path().filename().string();
                first = false;
            }
            result += '\n';
        }
        result.resize(PAGE_SIZE_IN_BYTES, '\0');
        return result;
    }

    tmc::task<hedge::status> sst_manager::_persist_partition_state(uint16_t partition_id)
    {
        auto it = this->_sorted_indices.find(partition_id);
        if(it == this->_sorted_indices.end())
            co_return hedge::ok();

        auto& state = *it->second;

        // Lazily open state file and directory fd (under unique_lock to prevent double-open)
        {
            std::unique_lock lk(state.mutex);
            if(state.state_file.fd() == -1)
            {
                auto [dir_prefix, file_prefix] = format_prefix(partition_id);
                auto dir_path = this->_cfg.partitions_path / dir_prefix;
                auto state_path = dir_path / sst_manager::MANIFEST_FILENAME;

                if(!state.dir_fd)
                    state.dir_fd = ::open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);

                const bool exists = std::filesystem::exists(state_path);
                const auto mode = exists ? fs::file::open_mode::read_write
                                         : fs::file::open_mode::read_write_new;
                auto maybe_file = fs::file::from_path(state_path, mode, false, PAGE_SIZE_IN_BYTES);
                if(!maybe_file)
                {
                    this->_logger.log("Failed to open partition state file: ", maybe_file.error().to_string());
                    co_return hedge::error("Failed to open partition state file: " + maybe_file.error().to_string());
                }
                state.state_file = std::move(maybe_file.value());
            }
        }

        // Optimistic snapshot: record generation, then serialize outside any lock
        partition_t snapshot;
        size_t snap_gen;
        {
            std::shared_lock lk(state.mutex);
            snapshot = state.levels;
            snap_gen = state.levels_seq_num.load(std::memory_order::acquire);
        }

        auto buf = _serialize_levels(snapshot);

        {
            std::lock_guard wlk(state.state_write_mutex);

            // If levels changed since snapshot, re-serialize the current state
            if(state.levels_seq_num.load(std::memory_order::acquire) != snap_gen)
            {
                std::shared_lock lk(state.mutex);
                snapshot = state.levels;
                buf = _serialize_levels(snapshot);
            }

            if(::pwrite(state.state_file.fd(), buf.data(), PAGE_SIZE_IN_BYTES, 0) < 0)
            {
                this->_logger.log("pwrite failed for partition state file: ", strerror(errno));
                co_return hedge::error(std::string("pwrite failed for partition state file: ") + strerror(errno));
            }
        }

        auto fsync_res = co_await io::fdatasync(state.state_file.fd());
        if(fsync_res < 0)
        {
            this->_logger.log("fdatasync failed for partition state file: ", strerror(-fsync_res));
            co_return hedge::error(std::string("fdatasync failed for partition state file: ") + strerror(-fsync_res));
        }

        int32_t dir_res = co_await io::fdatasync(*state.dir_fd);
        if(dir_res < 0)
            co_return hedge::error(std::string("fdatasync dir failed: ") + strerror(-dir_res));

        co_return hedge::ok();
    }

    void sst_manager::create_buckets_from_level(compaction_buckets_t& out_buckets,
                                                size_t level_idx,
                                                const level_t& level,
                                                size_t min_merge_width,
                                                size_t max_merge_width)
    {
        if(level.size() < min_merge_width)
            return;

        // Initialize candidate set
        std::vector<sst_ptr_t> candidates;
        candidates.reserve(min_merge_width * 4);

        auto it = level.begin();
        auto skip_non_pickable_ssts = [&level](auto& it)
        {
            while(it != level.end() && !(*it)->pickable_for_compaction())
                it = std::next(it);
        };

        skip_non_pickable_ssts(it);
        if(it == level.end())
            return;

        candidates.emplace_back(*it);

        for(it = std::next(it); it != level.end(); ++it)
        {
            // Form candidate set
            if((*it)->pickable_for_compaction() && (candidates.size() < max_merge_width))
            {
                candidates.emplace_back(*it);

                if(std::next(it) != level.end())
                    continue;
            }

            // Try emitting candidate set
            if(candidates.size() >= min_merge_width)
            {
                for(auto& c : candidates)
                    c->set_compaction_state(compaction_progress_state::IN_COMPACTION);

                out_buckets.insert({level_idx, std::exchange(candidates, {})});
            }

            skip_non_pickable_ssts(it);
            if(it == level.end())
                break;

            candidates.clear();
            candidates.emplace_back(*it);
        }
    }

    sst_manager::compaction_buckets_per_partition_t sst_manager::pick_ssts_into_buckets(
        const partition_snapshot_map_t& index_snapshot,
        size_t min_merge_width,
        size_t max_merge_width)
    {
        assert(min_merge_width >= 2 && min_merge_width < max_merge_width);

        compaction_buckets_per_partition_t buckets_per_partitions;
        buckets_per_partitions.reserve(index_snapshot.size());

        // For every partition, prepare the compaction buckets
        for(const auto& [partition_prefix, partition] : index_snapshot)
        {
            buckets_per_partitions.emplace_back(partition_prefix, compaction_buckets_t{});
            auto& buckets = buckets_per_partitions.back().second;

            for(const auto& [level_idx, level] : std::views::enumerate(partition))
            {
                sst_manager::create_buckets_from_level(buckets, level_idx, level, min_merge_width, max_merge_width);
            }
        }

        return buckets_per_partitions;
    }

    void sst_manager::_update_pending_compactions_counter(int32_t count)
    {
        // If count > 0 might start applying backpressure (when triggering a compaction)
        // If count < 0 might release backpressure (when a compaction finished)

        std::lock_guard lk(this->_pending_compactions_mutex);

        {
            const auto threshold = [this] -> int64_t
            {
                if(!this->_cfg.ssts_in_l0_block_write_threshold.has_value())
                    return std::numeric_limits<int64_t>::max();

                return static_cast<int64_t>(*this->_cfg.ssts_in_l0_block_write_threshold) * (1 << this->_cfg.num_partition_exponent);
            }();

            this->_pending_compacting_sst_for_backpressure_count += count;
            assert(this->_pending_compacting_sst_for_backpressure_count >= 0);

            bool pressure_is_currently_applied = this->_compaction_backpressure.ref().load(std::memory_order::relaxed);

            if(!pressure_is_currently_applied && this->_pending_compacting_sst_for_backpressure_count > threshold)
            {
                this->_compaction_backpressure.ref().store(true, std::memory_order::release);
            }
            else if(pressure_is_currently_applied && this->_pending_compacting_sst_for_backpressure_count < threshold)
            {
                this->_compaction_backpressure.ref().store(false, std::memory_order::release);
                this->_compaction_backpressure.notify_all();
            }
        }
    }

    void sst_manager::launch_compaction_tasks(
        const partition_snapshot_map_t& index_snapshot,
        sst_manager::compaction_buckets_per_partition_t&& buckets_per_partition,
        size_t /*min_merge_width*/,
        size_t /*max_merge_width*/)
    {
        auto run_tasks_in_sequence = [](std::vector<tmc::task<void>> tasks) -> tmc::task<void>
        {
            for(auto& t : tasks)
                co_await std::move(t);
            co_return;
        };

        std::vector<tmc::task<void>> last_level_tasks;

        for(auto& [partition_prefix, buckets] : buckets_per_partition)
        {
            const size_t current_last_level = index_snapshot.at(partition_prefix).size();

            for(size_t level = 0; level < current_last_level; ++level)
            {
                // Given a partition and a level, iterates over every bucket; the bucket are sorted by epoch
                // If we consider the epoch range as the [min, max] epochs of the SSTs within a bucket,
                // Then the buckets for a level are sorted by (non overlapping) epoch ranges
                //
                // The resulting merged SSTs must be pushed following the same chronological order
                // This is for avoiding merging SSTs containing non-adjacent epoch ranges in a following merging round
                auto [begin, end] = buckets.equal_range(level);

                std::vector<tmc::task<void>> tasks;
                tasks.reserve(std::distance(begin, end));

                for(auto bucket_it = begin; bucket_it != end; ++bucket_it)
                {
                    size_t bucket_input_count = bucket_it->second.size();

                    if(level == 0)
                        this->_update_pending_compactions_counter(static_cast<int32_t>(bucket_input_count));

                    {
                        std::lock_guard total_lk(this->_total_pending_compactions_mutex);
                        this->_total_pending_compacting_sst_count += static_cast<int64_t>(bucket_input_count);
                    }

                    const auto max_num_levels = this->_cfg.max_num_levels;
                    auto test_can_discard_keys = [&]() -> bool
                    {
                        const bool merge_to_bottommost_level = level + 1 == current_last_level ||
                                                               level + 1 == max_num_levels - 1;

                        // Only the first bucket within the level (i.e. the oldest) is allowed to discard keys
                        if(bucket_it != begin)
                            return false;

                        return merge_to_bottommost_level;
                    };

                    // Set-up permissions-chain between coro writing on the same level
                    // Is initialized with 0-count because this `task` will give the permission to the next one by releasing the sempahore.
                    auto next_can_write = std::make_shared<tmc::semaphore>(0);
                    auto can_write = std::exchange(this->_sorted_indices[partition_prefix]->permissions[level], next_can_write);

                    const auto target_level = std::min(level + 1, max_num_levels - 1);
                    auto& level_created = this->_sorted_indices[partition_prefix]->created[target_level]; // Thread-safe, occurs on braid (serialized execution)

                    const bool can_discard_keys =
                        test_can_discard_keys() &&
                        (target_level == max_num_levels || level_created == 0); // Confirms that the next level is empty and no other tasks has intention to write to it.
                                                                                // There is a chance that a compaction that will write to the bottommost level,
                                                                                // did not manage to publish the new SST yet;
                                                                                // `level_created` is needed for signaling this intention, and protecting from erroneously discarding records
                                                                                // Does not apply if it is the last level

                    level_created = 1; // Here: the resulting SST is unpublished, but we still signal that there will be something

                    auto task = this->_make_compaction_task(
                        std::move(can_write),
                        std::move(next_can_write),
                        level,
                        std::move(bucket_it->second),
                        can_discard_keys);

                    // Queue the task
                    tasks.emplace_back(std::move(task));
                }

                if(!tasks.empty())
                {
                    tmc::post(*this->_compaction_pool,
                              //    tasks.begin(), tasks.end(),
                              run_tasks_in_sequence(std::move(tasks)));
                }
            }
        }
    }

    hedge::status sst_manager::_schedule_compaction(bool compact_all)
    {
        const size_t min_merge_width = compact_all ? 2 : this->_cfg.min_merge_width;
        const size_t max_merge_width = compact_all ? std::numeric_limits<size_t>::max() : this->_cfg.max_merge_width;

        partition_snapshot_map_t index_snapshot;

        for(const auto& [partition_id, state_ptr] : this->_sorted_indices)
        {
            std::shared_lock lk(state_ptr->mutex);
            index_snapshot.emplace(partition_id, state_ptr->levels);
        }

        {
            // Pick sst to form buckets to be merged (size-tiered-like compaction)
            sst_manager::compaction_buckets_per_partition_t buckets_per_partition = sst_manager::pick_ssts_into_buckets(index_snapshot, min_merge_width, max_merge_width);

            thread_local std::random_device rd;
            thread_local std::mt19937 gen(rd());
            std::ranges::shuffle(buckets_per_partition, gen); // Shuffle partitions to improve load distribution

            // Asynchronously Launch the jobs on the executor
            this->launch_compaction_tasks(index_snapshot, std::move(buckets_per_partition), min_merge_width, max_merge_width);
        }

        return hedge::ok();
    }

    void sst_manager::schedule_compaction(bool compact_all)
    {
        // Signal the compaction worker to start a new round of compaction
        this->_compaction_scheduler_signal_chan.post(compact_all);
    }

    void sst_manager::wait_for_compactions_to_finish()
    {

        [[maybe_unused]] auto make_wait_job = [](sst_manager* sst_manager) -> tmc::task<void>
        {
            co_await sst_manager->_compaction_scheduler_signal_chan.drain();
        };

        // Wait for all in-flight self-completing tasks on executor pool
        std::unique_lock lk(this->_total_pending_compactions_mutex);
        this->_pending_compactions_cv.wait(lk, [this]()
                                           { return this->_total_pending_compacting_sst_count == 0; });
    }

    [[nodiscard]] double sst_manager::read_amplification_factor()
    {
        double total_read_amplification = 0.0;

        for(const auto& [prefix, state_ptr] : this->_sorted_indices)
        {
            std::shared_lock lk(state_ptr->mutex);
            total_read_amplification += state_ptr->levels.size();
        }

        if(this->_sorted_indices.empty())
            return 0.0;

        return total_read_amplification / this->_sorted_indices.size();
    }

    uint64_t sst_manager::max_seq_nr() const
    {
        uint64_t result = 0;
        for(const auto& [_, state] : this->_sorted_indices)
        {
            std::shared_lock lock(state->mutex);
            for(const auto& level : state->levels)
            {
                for(const auto& sst : level)
                    result = std::max(result, sst->max_seq_nr());
            }
        }
        return result;
    }

    void sst_manager::print_tree_structure() const
    {
        for(const auto& [partition_id, state_ptr] : this->_sorted_indices)
        {
            std::shared_lock lk(state_ptr->mutex);
            const auto& levels = state_ptr->levels;

            bool has_ssts = false;
            for(const auto& level : levels)
            {
                if(!level.empty())
                {
                    has_ssts = true;
                    break;
                }
            }

            if(!has_ssts)
                continue;

            std::cout << "Partition " << partition_id << ":\n";

            for(size_t l = 0; l < levels.size(); ++l)
            {
                const auto& level = levels[l];
                if(level.empty())
                    continue;

                size_t total_size = 0;
                for(const auto& s : level)
                    total_size += s->file_size();

                double avg_size = (double)total_size / level.size();

                double variance = 0.0;
                for(const auto& s : level)
                {
                    double diff = (double)s->file_size() - avg_size;
                    variance += diff * diff;
                }
                double stddev = std::sqrt(variance / level.size());

                std::cout << "  Level " << l
                          << ": " << level.size() << " SSTs"
                          << ", avg size: " << (avg_size / 1024.0) << " KB"
                          << ", stddev: " << (stddev / 1024.0) << " KB"
                          << ", total: " << (total_size / 1024.0) << " KB\n";
            }
        }
    }

    void sst_manager::print_compaction_stats() const
    {
        std::vector<std::vector<compaction_stats>> all_compaction_stats_per_level(this->_cfg.max_num_levels);

        // Group the stats
        for(const auto& state_ptr : this->_sorted_indices | std::views::values)
        {
            std::shared_lock lk(state_ptr->mutex);
            for(size_t lvl = 0; lvl < this->_cfg.max_num_levels; lvl++)
            {
                const auto& stats_at_level = state_ptr->stats_per_lvl[lvl];
                for(const auto& stat : stats_at_level)
                    all_compaction_stats_per_level[lvl].emplace_back(stat);
            }
        }

        // '_global' suffix refers to level-aggregate statistics
        size_t bytes_written_global{};
        size_t items_merged_global{};

        auto min_time_point_global = std::chrono::high_resolution_clock::time_point::max();
        auto max_time_point_global = std::chrono::high_resolution_clock::time_point::min();

        struct time_segment
        {
            std::chrono::high_resolution_clock::time_point time_start;
            std::chrono::high_resolution_clock::time_point time_end;
        };

        // Needed for finding "holes" for having the true Wall clock time spent on compaction
        std::vector<time_segment> time_segments;

        // Start collecting global statistics
        for(size_t lvl = 0; lvl < this->_cfg.max_num_levels; lvl++)
        {
            for(const auto& stat : all_compaction_stats_per_level[lvl])
            {
                min_time_point_global = std::min(min_time_point_global, stat.time_start);
                max_time_point_global = std::max(max_time_point_global, stat.time_end);

                time_segments.emplace_back(stat.time_start, stat.time_end);
            }
        }

        // Find how much time has been spent in idle between global start and global end
        std::sort(time_segments.begin(), time_segments.end(), [](const time_segment& lhs, const time_segment& rhs)
        {
            return lhs.time_start < rhs.time_start;
        });

        size_t time_idling_us_global{0};

        auto curr_segment = time_segments.begin();
        for (size_t idx = 1; idx < time_segments.size(); idx++)
        {
            if (curr_segment->time_end >= time_segments[idx].time_start)
            {
                curr_segment->time_end = std::max(curr_segment->time_end, time_segments[idx].time_end);
                continue;
            }

            time_idling_us_global += std::chrono::duration_cast<std::chrono::microseconds>(time_segments[idx].time_start - curr_segment->time_end).count();
            curr_segment->time_start = time_segments[idx].time_start;
            curr_segment->time_end = time_segments[idx].time_end;
        }


        const double total_time_us_global = std::chrono::duration_cast<std::chrono::microseconds>(max_time_point_global - min_time_point_global).count() - time_idling_us_global;
        double cumulative_bytes_per_us_global{0.0};
        double cumulative_items_per_us_global{0.0};

        for(size_t lvl = 0; lvl < this->_cfg.max_num_levels; lvl++)
        {
            size_t bytes_written_at_lvl{};
            size_t items_merged_at_lvl{};

            auto min_time_point = std::chrono::high_resolution_clock::time_point::max();
            auto max_time_point = std::chrono::high_resolution_clock::time_point::min();

            for(const auto& stat : all_compaction_stats_per_level[lvl])
            {
                bytes_written_at_lvl += stat.output_bytes;
                items_merged_at_lvl += stat.items_merged;

                min_time_point = std::min(min_time_point, stat.time_start);
                min_time_point_global = std::min(min_time_point_global, stat.time_start);

                max_time_point = std::max(max_time_point, stat.time_end);
                max_time_point_global = std::max(max_time_point_global, stat.time_end);
            }

            double cumulative_bytes_per_us{0.0};
            double cumulative_items_per_us{0.0};

            // For computing the aggregated stats across parallel compactions, we weigh a compaction by its time-share of the total compaction time
            const double total_time_us = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(max_time_point - min_time_point).count());

            for(const auto& stat : all_compaction_stats_per_level[lvl])
            {
                const double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(stat.time_end - stat.time_start).count();
                const double time_share = static_cast<double>(duration_us) / static_cast<double>(total_time_us);
                const double time_share_global = static_cast<double>(duration_us) / static_cast<double>(total_time_us_global);

                cumulative_bytes_per_us += (static_cast<double>(stat.output_bytes) / duration_us) * time_share;
                cumulative_items_per_us += (static_cast<double>(stat.items_merged) / duration_us) * time_share;

                cumulative_bytes_per_us_global += (static_cast<double>(stat.output_bytes) / duration_us) * time_share_global;
                cumulative_items_per_us_global += (static_cast<double>(stat.items_merged) / duration_us) * time_share_global;

                bytes_written_global += stat.output_bytes;
                items_merged_global += stat.items_merged;
            }

            if(all_compaction_stats_per_level[lvl].empty())
                continue;

            // B/us correspond to MiB/s
            size_t mb_per_sec = static_cast<size_t>(cumulative_bytes_per_us * 1'000'000.0 / MiB);
            size_t items_per_sec = static_cast<size_t>(cumulative_items_per_us * 1'000'000);

            std::cout << "Compaction throughput at level " << lvl << " : " << mb_per_sec << " MB/s, " << items_per_sec << " entries/s\n"
                      << "Bytes written at level: " << static_cast<size_t>(static_cast<double>(bytes_written_at_lvl / MB)) << " MB\n"
                      << "Num compactions at level: " << all_compaction_stats_per_level[lvl].size() << " compactions\n";
        }

        size_t mb_per_sec_global = static_cast<size_t>(cumulative_bytes_per_us_global * 1'000'000.0 / MiB);
        size_t items_per_sec_global = static_cast<size_t>(cumulative_items_per_us_global * 1'000'000);
        size_t num_compactions = std::accumulate(all_compaction_stats_per_level.begin(), all_compaction_stats_per_level.end(), 0, [](size_t acc, const auto& o)
        {
            return acc + o.size();
        });

        std::cout << "Compaction throughput global: : " << mb_per_sec_global << " MB/s, " << items_per_sec_global << " entries/s\n"
                  << "Bytes written global: " << static_cast<size_t>(static_cast<double>(bytes_written_global / MB)) << " MB\n"
                  << "Num compactions at level: " << num_compactions << " compactions\n";
    }

} // namespace hedge::db
