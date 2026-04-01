#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <coroutine>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <ranges>
#include <shared_mutex>
#include <unistd.h>
#include <unordered_set>

#include <error.hpp>

#include "fs/fs.hpp"
#include "index_ops.h"
#include "io/io_executor.h"
#include "io/io_requests.hpp"
#include "perf_counter.h"
#include "sst_manager.h"
#include "tmc/aw_resume_on.hpp"
#include "tmc/spawn_many.hpp"
#include "tmc/sync.hpp"
#include "utils.h"
#include "xxh64.hpp"

namespace hedge::db
{

    sst_manager::sst_manager(const config& cfg,
                             std::shared_ptr<io::io_executor> compaction_pool,
                             std::shared_ptr<sharded_page_cache> page_cache)
        : _cfg(cfg), _compaction_pool(std::move(compaction_pool)), _page_cache(std::move(page_cache))
    {
        // _compation_executor_pool.resize(cfg.compaction_io_workers);
        // size_t idx = 0;
        // for(auto& ex : _compation_executor_pool)
        // {
        // ex = async::executor_context::make_new(32);
        // ex->set_thread_name("compact-wrk-" + std::to_string(idx++));
        // }

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
                                                                    std::shared_ptr<io::io_executor> compaction_pool,
                                                                    std::shared_ptr<sharded_page_cache> page_cache)
    {
        auto mgr = std::make_unique<sst_manager>(cfg, std::move(compaction_pool), std::move(page_cache));

        if(!std::filesystem::exists(cfg.indices_path))
            return mgr;

        auto flush_iteration_from_filename = [](const std::filesystem::path& p) -> size_t
        {
            // Filename is <partition_prefix>.00012
            // -> extension is ".00012"
            // -> we skip the dot, parse "00012" and get '12' as size_t
            return std::stoull(p.extension().string().substr(1));
        };

        size_t max_flush_iteration = 0;

        for(auto& [partition_id, state_ptr] : mgr->_sorted_indices)
        {
            auto& state = *state_ptr;
            auto [dir_prefix, file_prefix] = format_prefix(partition_id);
            auto dir_path = cfg.indices_path / dir_prefix;

            if(!std::filesystem::exists(dir_path))
                continue;

            auto state_path = dir_path / with_extension(file_prefix, ".tiers");
            const bool state_file_existed = std::filesystem::exists(state_path);

            {
                const auto mode = state_file_existed ? fs::file::open_mode::read_write
                                                     : fs::file::open_mode::read_write_new;
                auto maybe_file = fs::file::from_path(state_path, mode, false, PAGE_SIZE_IN_BYTES);
                if(maybe_file)
                    state.state_file = std::move(maybe_file.value());
            }

            bool loaded_from_state = false;

            if(state_file_existed && state.state_file.fd() != -1)
            {
                std::string buf(PAGE_SIZE_IN_BYTES, '\0');
                ssize_t n = ::pread(state.state_file.fd(), buf.data(), PAGE_SIZE_IN_BYTES, 0);
                if(n > 0)
                {
                    auto maybe_levels = _deserialize_levels(buf, dir_path, cfg.max_num_levels, cfg.use_odirect_for_indices);
                    if(maybe_levels)
                    {
                        state.levels = std::move(maybe_levels.value());
                        for(const auto& level : state.levels)
                        {
                            for(const auto& sst_ptr : level)
                            {
                                max_flush_iteration = std::max(max_flush_iteration, flush_iteration_from_filename(sst_ptr->path()));
                            }
                        }
                        loaded_from_state = true;
                    }
                }
            }

            if(loaded_from_state)
            {
                // Collect tracked filenames to detect orphans on disk
                std::unordered_set<std::string> tracked_filenames;
                for(const auto& level : state.levels)
                {
                    for(const auto& sst_ptr : level)
                        tracked_filenames.insert(sst_ptr->path().filename().string());
                }

                std::vector<std::string> orphans;
                for(const auto& entry : std::filesystem::directory_iterator(dir_path))
                {
                    if(!entry.is_regular_file() || entry.path().extension() == ".tiers")
                        continue;

                    auto filename = entry.path().filename().string();
                    if(!tracked_filenames.contains(filename))
                    {
                        max_flush_iteration = std::max(max_flush_iteration,
                                                       flush_iteration_from_filename(entry.path()));
                        orphans.push_back(filename);
                    }
                }

                if(!orphans.empty())
                {
                    std::string orphan_list;
                    for(const auto& o : orphans)
                        orphan_list += " " + o;
                    return hedge::error("Orphan SST files detected in partition " + dir_prefix + ":" + orphan_list);
                }
            }

            if(!loaded_from_state)
            {
                for(const auto& entry : std::filesystem::directory_iterator(dir_path))
                {
                    if(!entry.is_regular_file())
                        continue;
                    if(entry.path().extension() == ".tiers")
                        continue;

                    auto maybe_sst = sst::load(entry.path(), cfg.use_odirect_for_indices);
                    if(!maybe_sst)
                        return maybe_sst.error();

                    auto loaded = std::move(maybe_sst.value());
                    max_flush_iteration = std::max(max_flush_iteration, flush_iteration_from_filename(loaded.path()));

                    auto& l0 = state.levels[0];
                    auto sst_ptr = std::make_shared<sst>(std::move(loaded));
                    auto pos = std::ranges::lower_bound(l0, sst_ptr,
                                                        [](const sst_ptr_t& a, const sst_ptr_t& b)
                                                        { return a->epoch() < b->epoch(); });
                    l0.insert(pos, std::move(sst_ptr));
                }
            }
        }

        mgr->_flush_iteration.store(max_flush_iteration + 1, std::memory_order::relaxed);

        return mgr;
    }

    tmc::task<void> sst_manager::push_new_indices(std::vector<sst> new_indices)
    {
        std::vector<uint16_t> affected_partitions;

        for(auto& new_sorted_index : new_indices)
        {
            auto prefix = new_sorted_index.upper_bound();

            auto sorted_index_ptr = std::make_shared<sst>(std::move(new_sorted_index));

            auto it = this->_sorted_indices.find(prefix);
            assert(it != this->_sorted_indices.end() && "Partition not found in pre-initialized map");

            auto& state = *it->second;

            {
                std::lock_guard lk(state.mutex);
                auto& l0 = state.levels[0];
                l0.emplace_back(std::move(sorted_index_ptr));
                state.levels_seq_num.fetch_add(1, std::memory_order::release);
                assert(check_is_sorted_by_epoch(l0) && "Sorted indices vector is not sorted by epoch after memtable flush");
                affected_partitions.push_back(prefix);
            }
        }

        // Deduplicate partition IDs
        std::ranges::sort(affected_partitions);
        auto unique = std::ranges::unique(affected_partitions);
        affected_partitions.erase(
            unique.begin(),
            unique.end());

        if(affected_partitions.empty())
            return tmc::task<void>{};

        auto persist = [](sst_manager* sst_manager, uint16_t pid) -> tmc::task<void>
        {
            auto status = co_await sst_manager->_persist_partition_state(pid);
            if(!status)
                sst_manager->_logger.log("Failed to persist partition state after flush: ", status.error().to_string());
        };

        std::vector<tmc::task<void>> tasks;
        tasks.reserve(affected_partitions.size());

        for(unsigned short affected_partition : affected_partitions)
            tasks.emplace_back(persist(this, affected_partition));

        auto spawn_persist = [](sst_manager* sst_manager, std::vector<tmc::task<void>> tasks) -> tmc::task<void>
        {
            co_await tmc::resume_on(*sst_manager->_compaction_pool);
            co_await tmc::spawn_many(tasks.begin(), tasks.end());
        };

        return spawn_persist(this, std::move(tasks));
    }

    tmc::task<expected<value_t>> sst_manager::lookup_async(const key_t& key, size_t matching_partition_id)
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

        // std::array<std::array<uint8_t, 64>, 64> probe_result_matrix;

        // for(const auto& [level_id, level] : partition | std::views::enumerate)
        // {
        //     for(const auto& sst_ptr : std::views::reverse(level))
        //         sst_ptr->prefetch_filter_slot(key_hash);
        // }

        // for(const auto& [level_id, level] : partition | std::views::enumerate)
        // {
        //     for(const auto& [sst_index, sst_ptr] : level | std::views::enumerate)
        //     {
        //         probe_result_matrix[level_id][sst_index] = sst_ptr->probe_filter(key_hash) ? 1 : 0;
        //     }
        // }

        for(const auto& [level_idx, level] : partition | std::views::enumerate)
        {
            for(const auto& [sst_index, sst_ptr] : level | std::views::enumerate | std::views::reverse)
            {
                ++ssts_visited;

                if(!sst_ptr->probe_filter(key_hash))
                    continue;

                // if(probe_result_matrix[level_idx][sst_index] == 0)
                //     continue;

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

    tmc::task<void> sst_manager::_make_self_completing_compaction_task(size_t level, std::vector<sst_ptr_t> inputs, size_t input_min_merge_width)
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

            // Remove inputs by ID (safe even if other jobs inserted into the middle of the range)
            std::erase_if(sst_level, [&input_ids](const sst_ptr_t& sst)
                          { return input_ids.contains(sst->id()); });

            size_t bucket_avg_size = std::accumulate(
                inputs.begin(), inputs.end(),
                0UL, [](size_t sum, const sst_ptr_t& ptr)
                { return sum + ptr->file_size(); });
            bucket_avg_size /= inputs.size();

            const size_t next_level_idx = std::min(level + ((output_ptr->file_size() / bucket_avg_size) / input_min_merge_width), this->_cfg.max_num_levels - 1);

            auto& target_level = state.levels[next_level_idx];
            auto pos = std::ranges::lower_bound(target_level, output_ptr,
                                                [](const sst_ptr_t& a, const sst_ptr_t& b)
                                                { return a->epoch() < b->epoch(); });
            target_level.insert(pos, std::move(output_ptr));
            state.levels_seq_num.fetch_add(1, std::memory_order::release);

            const bool needs_compaction = target_level.size() > input_min_merge_width;
            lk.unlock();

            if(needs_compaction)
                this->trigger_compaction(false);

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
            for(const auto& input : inputs)
                input->set_compaction_state(compaction_progress_state::COMPACTION_ERROR);

            this->_logger.log("Self-completing compaction failed: ", merge_result.error().to_string());
        }

        // Always decrement counter and remove in-flight IDs
        {
            std::lock_guard lk(this->_pending_compactions_mutex);
            this->_pending_compacting_sst_count -= input_count;

            // Backpressure only applies to L0
            if(level == 0)
            {
                this->_pending_compacting_sst_in_l0_count -= input_count;
                constexpr size_t stop_inserts_l0_threshold = 20;
                const size_t threshold = stop_inserts_l0_threshold * (1UL << this->_cfg.num_partition_exponent);
                bool release_pressure = this->_pending_compacting_sst_in_l0_count < threshold / 2;
                if(release_pressure && this->_compaction_backpressure.ref().load(std::memory_order::relaxed))
                {
                    this->_logger.log("Compaction backpressure released: pending compacting SST count in L0 is ", this->_pending_compacting_sst_in_l0_count, " below threshold ", threshold / 2);
                    this->_compaction_backpressure.ref().store(false, std::memory_order::release);
                    this->_compaction_backpressure.notify_all();
                }
            }

            this->_pending_compactions_cv.notify_all();
        }
    }

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

    static std::vector<std::string_view> split_by(std::string_view s, char delim)
    {
        std::vector<std::string_view> parts;
        size_t pos = 0;
        while(pos <= s.size())
        {
            size_t next = s.find(delim, pos);
            if(next == std::string_view::npos)
            {
                parts.emplace_back(s.substr(pos));
                break;
            }
            parts.emplace_back(s.substr(pos, next - pos));
            pos = next + 1;
        }
        return parts;
    }

    hedge::expected<sst_manager::partition_t> sst_manager::_deserialize_levels(std::string_view content,
                                                                               const std::filesystem::path& dir_path,
                                                                               size_t max_num_levels,
                                                                               bool use_odirect)
    {
        partition_t result(max_num_levels);
        size_t level_idx = 0;
        size_t pos = 0;

        while(level_idx < max_num_levels && pos < content.size())
        {
            size_t nl = content.find('\n', pos);
            if(nl == std::string_view::npos)
                break;

            std::string_view line = content.substr(pos, nl - pos);
            pos = nl + 1;

            if(!line.empty())
            {
                auto& level = result[level_idx];
                for(auto fname : split_by(line, ';'))
                {
                    if(fname.empty())
                        continue;
                    auto sst_path = dir_path / std::string(fname);
                    auto maybe_sst = sst::load(sst_path, use_odirect);
                    if(!maybe_sst)
                        return maybe_sst.error();
                    level.emplace_back(std::make_shared<sst>(std::move(maybe_sst.value())));
                }
            }

            ++level_idx;
        }

        return result;
    }

    tmc::task<hedge::status> sst_manager::_persist_partition_state(uint16_t partition_id)
    {
        auto it = this->_sorted_indices.find(partition_id);
        if(it == this->_sorted_indices.end())
            co_return hedge::ok();

        auto& state = *it->second;

        // Lazily open state file (under unique_lock to prevent double-open)
        {
            std::unique_lock lk(state.mutex);
            if(state.state_file.fd() == -1)
            {
                auto [dir_prefix, file_prefix] = format_prefix(partition_id);
                auto state_path = this->_cfg.indices_path / dir_prefix / with_extension(file_prefix, ".tiers");

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
        co_return hedge::ok();
    }

    hedge::expected<sst_manager::compaction_stats> sst_manager::_compaction_job_size_tiered(bool compact_all)
    {
        const size_t min_merge_width = compact_all ? 2 : this->_cfg.min_merge_width;
        const size_t max_merge_width = compact_all ? std::numeric_limits<size_t>::max() : this->_cfg.max_merge_width;

        compaction_stats stats{};
        partition_snapshot_map_t index_snapshot;

        for(const auto& [partition_id, state_ptr] : this->_sorted_indices)
        {
            std::shared_lock lk(state_ptr->mutex);
            index_snapshot.emplace(partition_id, state_ptr->levels);
        }
        {
            // 1. Compute buckets of SSTs to be merged (size-tiered)
            using bucket_t = std::vector<sst_ptr_t>;
            using buckets_t = std::multimap<size_t, bucket_t>;                          // level_id -> list of buckets (each bucket is a list of SSTs)
            using buckets_per_partiton_t = std::vector<std::pair<uint16_t, buckets_t>>; // partition_id -> buckets (per level)

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
                    if(level.size() < min_merge_width)
                        continue;

                    std::vector<sst_ptr_t> candidates;
                    candidates.reserve(min_merge_width);

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
                        if(pickable_for_compaction && (file_size <= new_avg * this->_cfg.bucket_ratio) && (candidates.size() < max_merge_width))
                        {
                            candidates.emplace_back(*it);
                            sizes_sum += file_size;

                            if(std::next(it) != level.end())
                                continue;
                        }

                        // Try emitting candidate set
                        if(candidates.size() >= min_merge_width)
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
            std::ranges::shuffle(buckets_per_partitions, gen); // Shuffle partitions to improve load distribution
            auto run_tasks_in_sequence = [](std::vector<tmc::task<void>> tasks) -> tmc::task<void>
            {
                for(auto& t : tasks)
                    co_await std::move(t);
                co_return;
            };

            constexpr bool OPTIMIZE_FOR_SPACE_AMP = false;

            std::vector<tmc::task<void>> last_level_tasks;

            for(auto& [partition_prefix, buckets] : buckets_per_partitions)
            {
                const size_t last_level = index_snapshot.at(partition_prefix).size();

                for(size_t level = 0; level < last_level; ++level)
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

                    for(auto it = begin; it != end; ++it)
                    {
                        size_t bucket_input_count = it->second.size();

                        // Always increment pending count
                        {
                            std::lock_guard lk(this->_pending_compactions_mutex);
                            this->_pending_compacting_sst_count += bucket_input_count;

                            // Backpressure only applies to L0
                            if(level == 0)
                            {
                                this->_pending_compacting_sst_in_l0_count += bucket_input_count;
                                constexpr size_t stop_inserts_l0_threshold = 20;
                                const auto threshold = stop_inserts_l0_threshold * (1UL << this->_cfg.num_partition_exponent);
                                bool should_pressure = this->_pending_compacting_sst_in_l0_count > threshold;
                                if(should_pressure && !this->_compaction_backpressure.ref().load(std::memory_order::relaxed)) // Avoid unnecessary stores
                                {
                                    this->_compaction_backpressure.ref().store(true, std::memory_order::release);
                                    this->_compaction_backpressure.notify_all();
                                }
                            }
                        }

                        // Submit self-completing task
                        auto task = this->_make_self_completing_compaction_task(level, std::move(it->second), min_merge_width);
                        tasks.emplace_back(std::move(task));
                    }

                    if(!tasks.empty())
                    {
                        jobs_count++;

                        if(!OPTIMIZE_FOR_SPACE_AMP || (level == 0) || (level != last_level))
                        {
                            // auto compaction_executor_idx = this->_compactor_executor_id++ % this->_compation_executor_pool.size();
                            // this->_compation_executor_pool[compaction_executor_idx]->submit_io_task(run_tasks_in_sequence(std::move(tasks)));
                            tmc::post(*this->_compaction_pool, run_tasks_in_sequence(std::move(tasks)));
                        }
                        else
                        {
                            last_level_tasks.emplace_back(run_tasks_in_sequence(std::move(tasks)));
                        }
                    }
                }
            }

            if(OPTIMIZE_FOR_SPACE_AMP)
            {
                tmc::post(*this->_compaction_pool, run_tasks_in_sequence(std::move(last_level_tasks)));
            }

            // Return immediately — tasks complete independently
            return compaction_stats{};
        }

        return stats;
    }

    void sst_manager::trigger_compaction(bool compact_all)
    {
        constexpr size_t max_in_flight_compaction_triggers = 16;

        if(!compact_all && this->_compaction_jobs_in_flight.load(std::memory_order::relaxed) >= max_in_flight_compaction_triggers)
            return;

        this->_compaction_jobs_in_flight.fetch_add(1, std::memory_order::relaxed);

        // compact_all=false: non-blocking — submit compaction job, set promise immediately
        auto make_compaction_job = [](sst_manager* sst_manager, bool compact_all) -> tmc::task<void>
        {
            if(!sst_manager->_compaction_braid.has_value())
                sst_manager->_compaction_braid.emplace();

            co_await tmc::resume_on(*sst_manager->_compaction_braid);

            auto maybe_stats = sst_manager->_compaction_job_size_tiered(compact_all);
            if(!maybe_stats)
                sst_manager->_logger.log("Compaction job failed: ", maybe_stats.error().to_string());
            sst_manager->_compaction_jobs_in_flight.fetch_sub(1, std::memory_order::relaxed);
        };

        tmc::post(*this->_compaction_pool, make_compaction_job(this, compact_all));
    }

    void sst_manager::wait_for_compactions_to_finish()
    {

        auto make_wait_job = [](sst_manager* sst_manager) -> tmc::task<void>
        {
            if(!sst_manager->_compaction_braid.has_value())
                sst_manager->_compaction_braid.emplace();

            co_await tmc::resume_on(*sst_manager->_compaction_braid);
        };

        tmc::post_waitable(*this->_compaction_pool, make_wait_job(this)).wait();

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
