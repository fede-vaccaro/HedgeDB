#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <ranges>
#include <shared_mutex>
#include <unordered_set>

#include <error.hpp>

#include "async/wait_group.h"
#include "index_ops.h"
#include "io_executor.h"
#include "perf_counter.h"
#include "sst_manager.h"
#include "utils.h"
#include "xxh64.hpp"

namespace hedge::db
{

    sst_manager::sst_manager(const config& cfg,
                             std::shared_ptr<sharded_page_cache> page_cache)
        : _cfg(cfg), _page_cache(std::move(page_cache))
    {
        _compation_executor_pool.resize(cfg.compaction_io_workers);
        for(auto& ex : _compation_executor_pool)
            ex = async::executor_context::make_new(32);

        size_t num_partitions = 1UL << cfg.num_partition_exponent;
        size_t partition_size = (1 << 16) / num_partitions;
        for(size_t i = 0; i < num_partitions; ++i)
        {
            auto partition_id = static_cast<uint16_t>(((i + 1) * partition_size) - 1);
            auto state = std::make_unique<partition_state>();
            state->levels.resize(cfg.max_num_levels);
            _sorted_indices.emplace(partition_id, std::move(state));
        }
    }

    hedge::expected<std::unique_ptr<sst_manager>> sst_manager::load(const config& cfg,
                                                                     std::shared_ptr<sharded_page_cache> page_cache)
    {
        auto mgr = std::make_unique<sst_manager>(cfg, page_cache);

        if(!std::filesystem::exists(cfg.indices_path))
            return mgr;

        size_t max_epoch = 0;

        for(const auto& entry : std::filesystem::recursive_directory_iterator(cfg.indices_path))
        {
            if(!entry.is_regular_file())
                continue;

            auto maybe_sst = sst::load(entry.path(), cfg.use_odirect_for_indices);
            if(!maybe_sst)
                return maybe_sst.error();

            auto loaded = std::move(maybe_sst.value());
            const size_t epoch = loaded.epoch();
            const size_t partition_id = loaded.upper_bound();

            auto it = mgr->_sorted_indices.find(static_cast<uint16_t>(partition_id));
            if(it == mgr->_sorted_indices.end())
                continue;

            max_epoch = std::max(max_epoch, epoch);

            auto& state = *it->second;
            auto& l0 = state.levels[0];

            auto sst_ptr = std::make_shared<sst>(std::move(loaded));
            auto pos = std::ranges::lower_bound(l0, sst_ptr,
                                                [](const sst_ptr_t& a, const sst_ptr_t& b)
                                                { return a->epoch() < b->epoch(); });
            l0.insert(pos, std::move(sst_ptr));
        }

        mgr->_flush_iteration.store(max_epoch + 1, std::memory_order::relaxed);

        return mgr;
    }

    void sst_manager::push_indices(std::vector<sst> new_indices)
    {
        for(auto& new_sorted_index : new_indices)
        {
            auto prefix = new_sorted_index.upper_bound();

            auto sorted_index_ptr = std::make_shared<sst>(std::move(new_sorted_index));

            auto it = this->_sorted_indices.find(prefix);
            assert(it != this->_sorted_indices.end() && "Partition not found in pre-initialized map");

            auto& state = *it->second;
            std::lock_guard lk(state.mutex);

            auto& l0 = state.levels[0];

            l0.emplace_back(std::move(sorted_index_ptr));

            assert(check_is_sorted_by_epoch(l0) && "Sorted indices vector is not sorted by epoch after memtable flush");
        }
    }

    async::task<expected<value_t>> sst_manager::lookup_async(const key_t& key, size_t matching_partition_id)
    {
        partition_t partition; // Local copy for safe iteration

        {
            auto sorted_indices_it = this->_sorted_indices.find(matching_partition_id);

            if(sorted_indices_it == this->_sorted_indices.end())
                co_return hedge::error("Key partition not found in sorted indices", errc::KEY_NOT_FOUND);

            auto& state = *sorted_indices_it->second;
            std::shared_lock lk(state.mutex);
            partition = state.levels;
        }

        // Note: we are iterating in reverse order to check the newest indices first, as they are more likely to contain the key due to temporal locality.

        // Indices are sorted by epoch: newest (and most recent key first)
        uint64_t key_hash = xxh64::hash((const char*)key.data(), key.size(), 0xDEADBEEF);

        std::optional<value_t> value_opt;
        size_t ssts_visited = 0;

        for(const auto& level : partition)
        {
            for(const auto& sst_ptr : std::views::reverse(level))
            {
                ++ssts_visited;
                auto maybe_value_ptr = co_await sst_ptr->lookup_async(key, this->_page_cache, key_hash);
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

        prof::get<"sst_visited_per_lookup">().add(ssts_visited);

        if(!value_opt.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        co_return std::move(value_opt.value());
    }

    async::task<> sst_manager::_make_compaction_task(compaction_output& output,
                                                     std::vector<sst_ptr_t> inputs,
                                                     std::shared_ptr<async::wait_group> wg)
    {
        // set lower priority for thread
        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = this->_cfg.compaction_read_ahead_size_bytes,
            .new_index_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed),
            .base_path = this->_cfg.indices_path,
            .discard_deleted_keys = false,
            .create_new_with_odirect = this->_cfg.use_odirect_for_indices,
            .populate_cache_with_output = false,
            .try_reading_from_cache = true,
        };

        std::vector<const sst*> indices;
        indices.reserve(inputs.size());
        for(const auto& i : inputs)
            indices.emplace_back(i.get());

        hedge::expected<sst> merge_result = co_await index_ops::k_way_merge_async2(merge_config,
                                                                                   indices,
                                                                                   async::this_thread_executor(),
                                                                                   this->_page_cache);

        hedge::expected<sst_ptr_t> result = [&]() -> hedge::expected<sst_ptr_t>
        {
            if(!merge_result)
                return merge_result.error();

            return std::make_shared<sst>(std::move(merge_result.value()));
        }();

        size_t input_bytes = std::accumulate(inputs.begin(), inputs.end(), 0UL, [](size_t sum, const sst_ptr_t& ptr)
                                             { return sum + ptr->file_size(); });

        {
            std::lock_guard lk(output.m);
            output.results.emplace_back(
                compaction_output::compaction_args{
                    .inputs = inputs,
                    .output = result,
                    .input_bytes = input_bytes,
                    .output_bytes = (result.has_value() ? result.value()->file_size() : 0),
                });
        }

        wg->decr();
    }

    async::task<> sst_manager::_make_self_completing_compaction_task(size_t level, std::vector<sst_ptr_t> inputs)
    {
        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = this->_cfg.compaction_read_ahead_size_bytes,
            .new_index_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed),
            .base_path = this->_cfg.indices_path,
            .discard_deleted_keys = false,
            .create_new_with_odirect = this->_cfg.use_odirect_for_indices,
            .populate_cache_with_output = false,
            .try_reading_from_cache = true,
        };

        std::vector<const sst*> indices;
        indices.reserve(inputs.size());
        for(const auto& i : inputs)
            indices.emplace_back(i.get());

        hedge::expected<sst> merge_result = co_await index_ops::k_way_merge_async2(merge_config,
                                                                                   indices,
                                                                                   async::this_thread_executor(),
                                                                                   this->_page_cache);

        size_t input_count = inputs.size();
        std::unordered_set<size_t> input_ids;
        input_ids.reserve(input_count);
        for(const auto& i : inputs)
            input_ids.insert(i->id());

        if(merge_result)
        {
            auto output_ptr = std::make_shared<sst>(std::move(merge_result.value()));

            // Apply index update under exclusive lock — all inputs belong to the same partition
            auto partition_prefix = inputs[0]->upper_bound();
            auto partition_it = this->_sorted_indices.find(partition_prefix);
            auto& state = *partition_it->second;
            std::unique_lock lk(state.mutex);

            auto& sst_level = state.levels[level];

            // Mark inputs for deletion
            for(const auto& input : inputs)
                input->set_delete_on_obj_destruction(true);

            // Remove inputs by ID (safe even if other jobs inserted into the middle of the range)
            std::erase_if(sst_level, [&input_ids](const sst_ptr_t& sst)
                          { return input_ids.contains(sst->id()); });

            size_t bucket_avg_size = std::accumulate(
                inputs.begin(), inputs.end(),
                0UL, [](size_t sum, const sst_ptr_t& ptr)
                { return sum + ptr->file_size(); });
            bucket_avg_size /= inputs.size();

            const size_t next_level_idx = std::min(level + ((output_ptr->file_size() / bucket_avg_size) / this->_cfg.min_merge_width), this->_cfg.max_num_levels - 1);

            auto& target_level = state.levels[next_level_idx];
            auto pos = std::ranges::lower_bound(target_level, output_ptr,
                                                [](const sst_ptr_t& a, const sst_ptr_t& b)
                                                { return a->epoch() < b->epoch(); });
            target_level.insert(pos, std::move(output_ptr));

            if(target_level.size() > this->_cfg.min_merge_width)
            {
                lk.unlock();
                this->trigger_compaction(false);
            }
        }
        else
        {
            for(const auto& input : inputs)
                input->set_compaction_state(compaction_progress_state::COMPACTION_ERROR);

            this->_logger.log("Self-completing compaction failed: ", merge_result.error().to_string());
        }

        // Always decrement counter and remove in-flight IDs
        {
            std::lock_guard lk(this->_pending_compactions_mutex);
            this->_pending_compacting_sst_count -= input_count;

            const size_t threshold = 16 * (1UL << this->_cfg.num_partition_exponent);
            bool under_pressure = this->_pending_compacting_sst_count > threshold;
            bool curr = this->_compaction_backpressure.load(std::memory_order::relaxed);
            if(curr != under_pressure)
            {
                this->_compaction_backpressure.store(under_pressure, std::memory_order::release);
                this->_compaction_backpressure.notify_all();
            }

            this->_pending_compactions_cv.notify_all();
        }
    }

    hedge::expected<sst_manager::compaction_stats> sst_manager::_compaction_job_size_tiered(bool /*compact_all*/)
    {
        compaction_stats stats{};
        partition_snapshot_map_t index_snapshot;

        for(const auto& [partition_id, state_ptr] : this->_sorted_indices)
        {
            std::shared_lock lk(state_ptr->mutex);
            index_snapshot.emplace(partition_id, state_ptr->levels);
        }

        {
            // 1. Compute buckets of SSTs to be merged (size-tiered)
            using buckets_t = std::multimap<size_t, std::vector<sst_ptr_t>>; // The key value represents the bucket for a level
            using buckets_per_partiton_t = std::vector<std::pair<uint16_t, buckets_t>>;

            buckets_per_partiton_t buckets_per_partitions;
            buckets_per_partitions.reserve(index_snapshot.size());

            // Snapshot in-flight IDs for filtering
            [[maybe_unused]] size_t jobs_count = 0;

            // Iterate for every partition
            for(auto& [partition_prefix, partition] : index_snapshot)
            {
                buckets_per_partitions.emplace_back(partition_prefix, buckets_t{});
                auto& buckets = buckets_per_partitions.back().second;

                for(const auto& [level_id, level] : std::views::enumerate(partition))
                {
                    if(level.size() < this->_cfg.min_merge_width)
                        continue;

                    std::vector<sst_ptr_t> candidates;
                    candidates.reserve(this->_cfg.min_merge_width);

                    auto it = level.begin();
                    while(it != level.end() && !(*it)->pickable_for_compaction())
                        it = std::next(it);

                    if(it == level.end())
                        continue;

                    candidates.emplace_back(*it);
                    size_t sizes_sum = (*it)->file_size(); // For computing the average

                    for(it = std::next(it); it != level.end(); ++it)
                    {
                        size_t file_size = (*it)->file_size();
                        auto new_avg = (double)(sizes_sum + file_size) / (candidates.size() + 1);

                        const bool pickable_for_compaction = (*it)->pickable_for_compaction();

                        // Form candidate set
                        if(pickable_for_compaction && (file_size <= new_avg * this->_cfg.bucket_ratio) && (candidates.size() < this->_cfg.max_merge_width))
                        {
                            candidates.emplace_back(*it);
                            sizes_sum += file_size;

                            if(std::next(it) != level.end())
                                continue;
                        }

                        // Try emitting candidate set
                        if(candidates.size() >= this->_cfg.min_merge_width)
                        {
                            for(auto& c : candidates)
                                c->set_compaction_state(compaction_progress_state::IN_COMPACTION);

                            buckets.insert({level_id, std::exchange(candidates, {})});
                        }

                        // Skip SSTs not pickable for compaction
                        while(it != level.end() && !(*it)->pickable_for_compaction())
                            it = std::next(it);

                        if(it == level.end())
                            break;

                        // Move to the next bucket, reset candidates
                        candidates.clear();
                        candidates.emplace_back(*it);
                        sizes_sum = (*it)->file_size();
                    }
                }
            }

            // 2. For each bucket: register in-flight IDs, increment pending count, submit self-completing task
            thread_local std::random_device rd;
            thread_local std::mt19937 gen(rd());
            std::shuffle(buckets_per_partitions.begin(), buckets_per_partitions.end(), gen); // Shuffle partitions to improve load distribution

            for(auto& [partition_prefix, buckets] : buckets_per_partitions)
            {

                for(size_t level = 0; level < this->_cfg.max_num_levels; ++level)
                {
                    auto [begin, end] = buckets.equal_range(level);

                    std::vector<async::task<void>> tasks;
                    tasks.reserve(std::distance(begin, end));

                    for(auto it = begin; it != end; ++it)
                    {
                        size_t bucket_input_count = it->second.size();

                        // Increment pending count and update backpressure
                        {
                            std::lock_guard lk(this->_pending_compactions_mutex);
                            this->_pending_compacting_sst_count += bucket_input_count;

                            const auto threshold = 16 * (1UL << this->_cfg.num_partition_exponent);
                            bool under_pressure = this->_pending_compacting_sst_count > threshold;
                            auto curr = this->_compaction_backpressure.load(std::memory_order::relaxed);
                            if(curr != under_pressure) // Avoid unnecessary stores
                            {
                                this->_compaction_backpressure.store(under_pressure, std::memory_order::release);
                                this->_compaction_backpressure.notify_all();
                            }
                        }

                        // Submit self-completing task
                        auto task = this->_make_self_completing_compaction_task(level, std::move(it->second));
                        tasks.emplace_back(std::move(task));
                    }

                    if(!tasks.empty())
                    {
                        auto run_tasks_in_sequence = [](std::vector<async::task<>> tasks) -> async::task<>
                        {
                            for(auto& t : tasks)
                                co_await t;
                        };
                        jobs_count++;
                        this->_compation_executor_pool[this->_executor_idx++ % this->_compation_executor_pool.size()]->submit_io_task(run_tasks_in_sequence(std::move(tasks)));
                    }
                }
            }

            // Return immediately — tasks complete independently
            return compaction_stats{};
        }

        return stats;
    }

    void sst_manager::trigger_compaction(bool compact_all)
    {
        if(compact_all)
        {
            // compact_all=true: synchronous — wait for flush, run compaction, block until done
            this->_compaction_worker.submit(
                [this]()
                {
                    auto start_processing_t = std::chrono::high_resolution_clock::now();

                    auto maybe_stats = this->_compaction_job_size_tiered(true);
                    if(!maybe_stats)
                        this->_logger.log("Compaction job failed: ", maybe_stats.error().to_string());

                    auto t1 = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - start_processing_t);

                    auto& stats = maybe_stats.value();

                    double write_throughput_mbs = stats.output_bytes / ((double)duration.count() / 1000000.0) / (1000.0 * 1000.0);

                    if(stats.output_bytes > 0)
                    {
                        this->_logger.log("Total duration for compaction: ", (double)duration.count() / 1000.0, " ms",
                                          ", MB written: ", stats.output_bytes / (1000.0 * 1000.0),
                                          ", MB read: ", stats.input_bytes / (1000.0 * 1000.0),
                                          ", throughput: ", write_throughput_mbs, " MB/s",
                                          ", items merged: ", stats.items_merged,
                                          ", items merged/s: ", (size_t)((double)stats.items_merged / ((double)duration.count() / 1000000.0)),
                                          ", num input files: ", stats.num_inputs);

                        prof::get<"merge_mb_written">().add(stats.output_bytes / (1000.0 * 1000.0));
                        prof::get<"merge_throughput_mbs">().add(write_throughput_mbs);
                    }
                });
        }
        else
        {
            constexpr size_t max_in_flight_compaction_triggers = 16;

            if(this->_compaction_jobs_in_flight.load(std::memory_order::relaxed) >= max_in_flight_compaction_triggers)
                return;

            this->_compaction_jobs_in_flight.fetch_add(1, std::memory_order::relaxed);

            // compact_all=false: non-blocking — submit compaction job, set promise immediately
            this->_compaction_worker.submit(
                [this]()
                {
                    auto maybe_stats = this->_compaction_job_size_tiered(false);
                    if(!maybe_stats)
                        this->_logger.log("Compaction job failed: ", maybe_stats.error().to_string());
                    this->_compaction_jobs_in_flight.fetch_sub(1, std::memory_order::relaxed);
                });
        }
    }

    void sst_manager::wait_for_compactions_to_finish()
    {
        this->_compaction_worker.wait_for_all_jobs();
        // Wait for all in-flight self-completing tasks on executor pool
        std::unique_lock lk(this->_pending_compactions_mutex);
        this->_pending_compactions_cv.wait(lk, [this]()
                                           { return this->_pending_compacting_sst_count == 0; });
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

} // namespace hedge::db
