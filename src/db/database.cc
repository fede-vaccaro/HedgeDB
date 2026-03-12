#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <error.hpp>
#include <variant>

#include "async/wait_group.h"
#include "cache.h"
#include "database.h"
#include "db/memtable.h"
#include "index_ops.h"
#include "io_executor.h"
#include "perf_counter.h"
#include "sst.h"
#include "types.h"
#include "utils.h"
#include "value_table.h"

namespace hedge::db
{
    expected<std::shared_ptr<database>> database::make_new(const std::filesystem::path& base_path, const db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_indices_path = base_path / "indices";
        db->_values_path = base_path / "values";
        db->_config = config;

        // Minimal config validation
        if(config.num_partition_exponent > db_config::MAX_PARTITION_EXPONENT)
            return hedge::error("num_partition_exponent must be <= " + std::to_string(db_config::MAX_PARTITION_EXPONENT));

        if(config.keys_in_mem_before_flush < db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH)
            return hedge::error("keys_in_mem_before_flush must be >= " + std::to_string(db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH));

        if(std::filesystem::exists(db->_base_path))
            return hedge::error("Database path already exists: " + db->_base_path.string());

        // Create necessary directories
        std::filesystem::create_directories(db->_base_path);
        std::filesystem::create_directories(db->_indices_path);
        std::filesystem::create_directories(db->_values_path);

        db->_compation_executor_pool.resize(config.compaction_io_workers);
        for(auto& ex : db->_compation_executor_pool)
            ex = async::executor_context::make_new(32);

        // Init clock cache
        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1) // Minimum 1 MB page cache
            db->_page_cache = std::make_shared<sharded_page_cache>(config.index_page_clock_cache_size_bytes, async::executor_pool::static_pool().size() * 4);

        // Setup memindex
        db->_memtable.~memtable();
        auto* memtable_mem = &db->_memtable;
        ::new(memtable_mem) memtable(
            memtable_config{
                .max_inserts_cap = config.keys_in_mem_before_flush,
                .memory_budget_cap = 48 * 1024 * 1024, // default
                .auto_compaction = config.auto_compaction,
                .use_odirect = config.use_odirect_for_indices,
                .num_writer_threads = async::executor_pool::static_pool().size(),
                .flush_io_workers = config.flush_io_workers,
            },
            config.num_partition_exponent,
            db->_indices_path,
            &db->_flush_iteration,
            make_push_indices_callback(db),
            make_compaction_callback(db),
            db->_page_cache,
            &db->_compaction_backpressure);

        // Create first value table
        auto first_value_table = value_table::make_new(db->_values_path, 0);
        if(!first_value_table)
            return hedge::error("Failed to create initial value table: " + first_value_table.error().to_string());

        db->_current_value_table = std::move(first_value_table.value());
        db->_last_table_id = 0;

        // Create pipelined value table
        auto pipelined_value_table = value_table::make_new(db->_values_path, 1);
        if(!pipelined_value_table)
            return hedge::error("Failed to create next in pipeline value table: " + pipelined_value_table.error().to_string());

        db->_pipelined_value_table = std::move(pipelined_value_table.value());

        // Init point cache (avoid this)
        if(config.index_point_cache_size_bytes > 1024 * 1024 * 1) // Mininum 1MB entry cache
            db->_index_point_cache = std::make_shared<point_cache>(config.index_point_cache_size_bytes);

        // reserve value tables map
        db->_value_tables.reserve(4096);

        // init write buffers
        db->_write_buffers.resize(async::executor_pool::static_pool().size());

        for(auto& write_buffer_ptr : db->_write_buffers)
        {
            write_buffer_ptr = std::make_unique<thread_write_buffer>(WRITE_BUFFER_DEFAULT_SIZE);
            auto vtable = db->_current_value_table.load();
            auto allocated_offset = vtable->allocate_pages_for_write(WRITE_BUFFER_DEFAULT_SIZE);
            write_buffer_ptr->set(vtable, allocated_offset.value());
        }

        return db;
    }

    std::function<void(std::vector<sst>)> database::make_push_indices_callback(const std::shared_ptr<database>& db)
    {
        return [db](std::vector<sst> new_indices)
        {
            std::lock_guard sorted_index_lk(db->_sorted_index_mutex);

            for(auto& new_sorted_index : new_indices)
            {
                auto prefix = new_sorted_index.upper_bound();

                auto sorted_index_ptr = std::make_shared<sst>(std::move(new_sorted_index));
                db->_sorted_indices[prefix].emplace_back(std::move(sorted_index_ptr));

                assert(check_is_sorted_by_epoch(db->_sorted_indices[prefix]) && "Sorted indices vector is not sorted by epoch after memtable flush");
            }
        };
    }

    std::function<void()> database::make_compaction_callback(const std::shared_ptr<database>& db)
    {
        return [db]()
        {
            (void)db->compact_sorted_indices(false);
        };
    }

    expected<std::shared_ptr<database>> database::load(const std::filesystem::path& /*base_path*/, const db::db_config& /*config*/)
    {
        return hedge::error("load not implemented");
    }

    async::task<hedge::status> database::put_async(const key_t& key, const byte_buffer_t& value)
    {
        // prof::counter_guard guard(prof::get<"put_async">());

        if(value.size() < 512) [[likely]]
        {
            this->_memtable.put(key, value, hedge::value_type::IN_PLACE_VALUE);
            co_return hedge::ok();
        }

        // --- Try writing value to buffer first ---
        auto _allocate_space_for_write_buffer = [this](thread_write_buffer& write_buffer) -> hedge::status
        {
            std::shared_ptr<value_table> value_table_local = this->_current_value_table.load();
            expected<size_t> allocation = value_table_local->allocate_pages_for_write(WRITE_BUFFER_DEFAULT_SIZE);

            constexpr size_t MAX_RETRIES = 10;
            size_t retry_it = 0;

            // Running even more than 1 retry should be extremely rare
            while(!allocation && allocation.error().code() == hedge::errc::VALUE_TABLE_NOT_ENOUGH_SPACE && retry_it++ < MAX_RETRIES)
            {
                auto maybe_updated_value_table = this->_rotate_value_table(value_table_local);
                if(!maybe_updated_value_table)
                    return hedge::error("Failed to rotate value table: " + maybe_updated_value_table.error().to_string());

                value_table_local = std::move(maybe_updated_value_table.value());

                allocation = value_table_local->allocate_pages_for_write(WRITE_BUFFER_DEFAULT_SIZE);
            }

            if(!allocation)
                return hedge::error("Failed to allocate from value table: " + allocation.error().to_string());

            write_buffer.set(value_table_local, allocation.value());
            return hedge::ok();
        };

        static thread_local size_t THIS_THREAD_IDX = database::_thread_count.fetch_add(1);
        assert(THIS_THREAD_IDX < this->_write_buffers.size());

        const auto& write_buffer_ref = this->_write_buffers[THIS_THREAD_IDX];
        if(!write_buffer_ref->is_set())
        {
            auto status = _allocate_space_for_write_buffer(*write_buffer_ref);
            if(!status)
                co_return status;
        }

        expected<value_ptr_t> maybe_write = hedge::error("");

        maybe_write = write_buffer_ref->write(key, value);

        // TODO: Currently is not supported the case when the value is greater than the buffer capacity
        // If so, we should write directly on file
        if(!maybe_write && maybe_write.error().code() == hedge::errc::BUFFER_FULL)
        {
            auto buffer_flush_status = co_await write_buffer_ref->flush(async::this_thread_executor());
            if(!buffer_flush_status)
                co_return buffer_flush_status;

            _allocate_space_for_write_buffer(*write_buffer_ref);
            maybe_write = write_buffer_ref->write(key, value);
        }

        // --- Update Memtable ---
        this->_memtable.put(key, maybe_write.value(), hedge::value_type::VALUE_PTR);

        co_return hedge::ok();
    }

    // async::task<hedge::status> database::put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor)
    // {
    //     co_return co_await this->put_async(key, value, executor);
    // }

    async::task<expected<value_t>> database::_find_value(const key_t& key)
    {
        // prof::counter_guard guard(prof::get<"find_value_in_sst">());

        // Step 1: Check the memtable first (contains the most recent data).
        std::optional<value_t> value_opt;

        value_opt = this->_memtable.get(key);

        if(value_opt.has_value())
            co_return std::move(value_opt.value());

        // Step 2: If not found in memtable and cache, search the relevant sorted index files.
        size_t matching_partition_id = this->_find_matching_partition_for_key(key);
        std::vector<sst_ptr_t> sorted_indices_local; // Local copy for safe iteration

        {
            std::shared_lock lk(this->_sorted_index_mutex);
            auto sorted_indices_it = this->_sorted_indices.find(matching_partition_id);

            if(sorted_indices_it == this->_sorted_indices.end())
                co_return hedge::error("Key partition not found in sorted indices", errc::KEY_NOT_FOUND);

            sorted_indices_local = sorted_indices_it->second;
        }

        // Note: we are iterating in reverse order to check the newest indices first, as they are more likely to contain the key due to temporal locality.

        // Indices are sorted by epoch: newest (and most recent key first)
        uint64_t key_hash = std::hash<key_t>{}(key);
        for(const auto& sorted_index_ptr : std::ranges::reverse_view(sorted_indices_local))
        {
            auto maybe_value_ptr = co_await sorted_index_ptr->lookup_async(key, this->_page_cache, key_hash);
            if(!maybe_value_ptr.has_value() && maybe_value_ptr.error().code() != errc::KEY_NOT_FOUND)
                co_return hedge::error(std::format("An error occurred while reading index at path {}: {}", sorted_index_ptr->path().string(), maybe_value_ptr.error().to_string()));

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

        if(!value_opt.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        co_return std::move(value_opt.value());
    }

    std::shared_ptr<value_table> database::_find_value_table_by_id(uint32_t id)
    {
        if(this->_current_value_table.load(std::memory_order::relaxed)->id() == id)
            return this->_current_value_table;

        {
            std::shared_lock lk(this->_value_tables_mutex);
            auto it = this->_value_tables.find(id);
            if(it != this->_value_tables.end())
                return it->second;
        }
        return nullptr;
    }

    async::task<expected<database::byte_buffer_t>> database::get_async(const key_t& key)
    {
        auto maybe_value = co_await this->_find_value(key);

        if(!maybe_value)
            co_return maybe_value.error();

        auto value = std::move(maybe_value.value());

        if(std::holds_alternative<std::vector<uint8_t>>(value))
        {
            co_return std::move(std::get<std::vector<uint8_t>>(value));
        }

        if(std::holds_alternative<tombstone_t>(value))
        {
            co_return hedge::error("Key is deleted", errc::DELETED);
        }

        if(!std::holds_alternative<value_ptr_t>(value))
            co_return hedge::error("Invalid value type found for key");

        const auto& value_ptr = std::get<value_ptr_t>(value);

        auto table_ptr = this->_find_value_table_by_id(value_ptr.table_id());

        // Try reading from write_buffer
        for(auto& write_buf : this->_write_buffers)
        {
            auto maybe_file = write_buf->try_read(value_ptr);
            if(maybe_file)
                co_return std::move(maybe_file.value().binaries);
        }

        auto maybe_file = co_await table_ptr->read_async(value_ptr.offset(), value_ptr.size());

        if(!maybe_file)
            co_return hedge::error(std::format("Error reading value from table {}: {}", table_ptr->path().string(), maybe_file.error().to_string()));

        // TODO: Implement optional paranoid check: compare the key in the read header (`maybe_file.value().header.key`)
        // with the requested `key` to detect potential data corruption or pointer errors.

        co_return std::move(maybe_file.value().binaries);
    }

    size_t database::_find_matching_partition_for_key(const key_t& key) const
    {
        // Calculate the range of key prefixes covered by each partition.
        // Total key space is 2^16 (for 16-bit prefix). Number of partitions is 2^exponent.
        size_t partition_size = (1 << 16) / (1 << this->_config.num_partition_exponent);

        // Use the utility function to find the upper-bound prefix ID for the partition containing the key.
        size_t matching_partition_id = hedge::find_partition_prefix_for_key(key, partition_size); // Corrected type

        return matching_partition_id;
    }

    async::task<hedge::status> database::remove_async(const key_t& /* key */)
    {
        co_return hedge::error("remove_async not implemented yet");
    }

    hedge::expected<std::shared_ptr<value_table>> database::_rotate_value_table(std::shared_ptr<value_table> rotating)
    {
        auto t0 = std::chrono::high_resolution_clock::now();

        // This guarantees that the CAS will fail
        // Better do an early exit without holding the mutex
        if(auto curr = this->_current_value_table.load(std::memory_order::relaxed); curr != rotating)
            return curr;

        auto pending = this->_pipelined_value_table.load(std::memory_order::relaxed);

        while(pending == nullptr)
        {
            this->_pipelined_value_table.wait(nullptr);
            pending = this->_pipelined_value_table.load();
        }

        size_t next_id{};

        if(!this->_current_value_table.compare_exchange_strong(rotating, pending))
            return rotating;

        this->_pipelined_value_table.store(nullptr);

        next_id = this->_last_table_id.fetch_add(1, std::memory_order_relaxed) + 2; // The currently pipelined value table is already last + 1

        // TODO: fix this race condition: between the CAS and the expression, the old value won't be available
        {
            std::lock_guard lk(this->_value_tables_mutex);
            this->_value_tables[rotating->id()] = rotating;
        }

        this->_value_table_worker.submit(
            [this, next_id]()
            {
                auto t0 = std::chrono::high_resolution_clock::now();
                auto new_value_table = value_table::make_new(this->_values_path, next_id);
                if(!new_value_table)
                {
                    this->_logger.log("Failed to create new value table: " + new_value_table.error().to_string());
                    return;
                }

                this->_pipelined_value_table.store(new_value_table.value());
                this->_pipelined_value_table.notify_all();

                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                this->_logger.log("Prepared new value table in ", (double)duration.count() / 1000.0, " ms");
            });

        auto t1 = std::chrono::high_resolution_clock::now();
        [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

        // this->_logger.log("Value table rotated in ", (double)duration.count(), " us, pushing job required ", (double)submit_job_duration.count());

        return this->_current_value_table.load();
    }

    async::task<> database::_make_compaction_task(compaction_output& output,
                                                  std::vector<database::sst_ptr_t> inputs,
                                                  std::shared_ptr<async::wait_group> wg)
    {
        // set lower priority for thread
        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
            .new_index_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed),
            .base_path = this->_indices_path,
            .discard_deleted_keys = false,
            .create_new_with_odirect = this->_config.use_odirect_for_indices,
            .populate_cache_with_output = false,
            .try_reading_from_cache = true,
        };

        std::vector<const sst*> indices;
        indices.reserve(indices.size());
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

    async::task<> database::_make_self_completing_compaction_task(std::vector<database::sst_ptr_t> inputs)
    {
        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
            .new_index_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed),
            .base_path = this->_indices_path,
            .discard_deleted_keys = false,
            .create_new_with_odirect = this->_config.use_odirect_for_indices,
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
            // std::cout << "Launching compactions with input indices: \n"
            //           << std::accumulate(inputs.begin(), inputs.end(), std::string{}, [](std::string acc, const sst_ptr_t& ptr)
            //                              { return acc + " - " + ptr->path().string() + " id: " + std::to_string(ptr->id()) + " size: " + std::to_string(ptr->file_size()) + "\n"; });

            // std::cout << "Output index: " << merge_result.value().path().string() << " id: " << merge_result.value().id() << "\n";
            auto output_ptr = std::make_shared<sst>(std::move(merge_result.value()));

            // Apply index update under exclusive lock
            std::lock_guard lk(this->_sorted_index_mutex);

            // All inputs belong to the same partition
            auto partition_prefix = inputs[0]->upper_bound();
            auto& partition_ssts = this->_sorted_indices[partition_prefix];

            auto begin_it = std::ranges::find_if(partition_ssts, [&inputs](const sst_ptr_t& sst)
                                                 { return sst->id() == (*inputs.rbegin())->id(); });

            auto end_it = std::next(std::ranges::find_if(partition_ssts, [&inputs](const sst_ptr_t& sst)
                                                         { return sst->id() == (*inputs.begin())->id(); }));

            if(std::distance(begin_it, end_it) != (int64_t)inputs.size())
            {
                std::string from_indices_list;
                std::string input_list;

                for(auto it = begin_it; it != end_it; ++it)
                    from_indices_list += std::format("{} {}; \n", (*it)->path().string(), (*it)->id());

                for(const auto& input : inputs)
                {
                    input_list += std::format("{} {}; \n", (input)->path().string(), (input)->id());
                }

                throw std::runtime_error("Mismatch between compaction and range in SST list: " +
                                         std::to_string(std::distance(begin_it, end_it)) + " vs " + std::to_string(inputs.size()) +
                                         " from_indices_list: \n" + from_indices_list + " input_list :\n" + input_list);
            }
            // Remove inputs from fs
            for(auto it = begin_it; it != end_it; ++it)
            {
                (*it)->set_delete_on_obj_destruction(true);
                (*it)->set_compaction_state(compaction_progress_state::COMPACTED);
            }
            // Replaces one of the inputs with the new value
            begin_it->swap(output_ptr);

            // Remove the other inputs
            partition_ssts.erase(std::next(begin_it), end_it);
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

            bool under_pressure = this->_pending_compacting_sst_count > 16 * (1UL << this->_config.num_partition_exponent);
            this->_compaction_backpressure.store(under_pressure, std::memory_order::release);

            this->_pending_compactions_cv.notify_all();
        }
    }

    hedge::expected<database::compaction_stats> database::_compaction_job_size_tiered(bool /*compact_all*/)
    {
        compaction_stats stats{};
        sorted_indices_map_t indices_snapshot;

        {
            std::shared_lock lk(this->_sorted_index_mutex);
            indices_snapshot = this->_sorted_indices;
        }

        {
            // 1. Compute buckets of SSTs to be merged (size-tiered)
            using buckets_t = std::multimap<size_t, std::vector<database::sst_ptr_t>>; // The key value represents the bucket size
            using buckets_per_partiton_t = std::unordered_map<uint16_t, buckets_t>;

            buckets_per_partiton_t buckets_per_partitions;

            constexpr double bucket_factor = 1.5;
            constexpr size_t min_merge_width = 4;
            // constexpr size_t max_merge_width = 32;
            constexpr size_t max_merge_width = std::numeric_limits<size_t>::max();

            // Snapshot in-flight IDs for filtering
            [[maybe_unused]] size_t jobs_count = 0;

            // Iterate for every partition
            for(auto& [partition_prefix, ssts] : indices_snapshot)
            {
                buckets_t& buckets = (buckets_per_partitions[partition_prefix] = buckets_t{});

                if(ssts.size() < min_merge_width)
                    continue;

                std::vector<sst_ptr_t> candidates;
                candidates.reserve(min_merge_width);

                auto it = ssts.rbegin();
                while(it != ssts.rend() && !(*it)->pickable_for_compaction())
                    it = std::next(it);

                if(it == ssts.rend())
                    continue;

                candidates.emplace_back(*it);

                size_t sizes_sum = (*it)->file_size(); // For computing the average

                for(it = std::next(it); it != ssts.rend(); ++it)
                {
                    size_t file_size = (*it)->file_size();
                    auto new_avg = (double)(sizes_sum + file_size) / (candidates.size() + 1);

                    const bool pickable_for_compaction = (*it)->pickable_for_compaction();

                    // Form candidate set
                    if(pickable_for_compaction && (file_size <= new_avg * bucket_factor) && (candidates.size() < max_merge_width))
                    {
                        // std::cout << "Adding to candidate set: " << (*it)->path().string() << " id: " << (*it)->id() << " size: " << file_size << " new_avg: " << new_avg << "\n";
                        candidates.emplace_back(*it);
                        sizes_sum += file_size;

                        if(std::next(it) != ssts.rend())
                            continue;
                    }

                    // Try emitting candidate set
                    if(candidates.size() >= min_merge_width)
                    {
                        for(auto& c : candidates)
                            c->set_compaction_state(compaction_progress_state::IN_COMPACTION);

                        // std::cout << "Emitting bucket bucket_ref_size: " << static_cast<size_t>(static_cast<double>(sizes_sum) / candidates.size()) << "\n"
                        //           << std::accumulate(candidates.begin(), candidates.end(), std::string{}, [](std::string acc, const sst_ptr_t& ptr)
                        //                              { return acc + " - " + ptr->path().string() + " id: " + std::to_string(ptr->id()) + " size: " + std::to_string(ptr->file_size()) + "\n"; });

                        auto bucket_reference_size = static_cast<size_t>(static_cast<double>(sizes_sum) / candidates.size());
                        buckets.insert({bucket_reference_size, std::exchange(candidates, {})});
                    }

                    // Skip SSTs not pickable for compaction
                    while(it != ssts.rend() && !(*it)->pickable_for_compaction())
                        it = std::next(it);

                    if(it == ssts.rend())
                        break;

                    // Move to the next bucket, reset candidates
                    candidates.clear();
                    candidates.emplace_back(*it);
                    sizes_sum = (*it)->file_size();
                }
            }

            // 2. For each bucket: register in-flight IDs, increment pending count, submit self-completing task
            size_t executor_idx = 0;

            for(auto& [partition_prefix, buckets] : buckets_per_partitions)
            {
                for(auto& [bucket_ref_size, inputs] : buckets)
                {
                    size_t bucket_input_count = inputs.size();

                    // Increment pending count and update backpressure
                    {
                        std::lock_guard lk(this->_pending_compactions_mutex);
                        this->_pending_compacting_sst_count += bucket_input_count;

                        const auto threshold = 32 * (1UL << this->_config.num_partition_exponent);
                        bool under_pressure = this->_pending_compacting_sst_count > threshold;
                        this->_compaction_backpressure.store(under_pressure, std::memory_order::release);
                    }

                    // Submit self-completing task
                    auto task = this->_make_self_completing_compaction_task(std::move(inputs));
                    this->_compation_executor_pool[executor_idx++ % this->_compation_executor_pool.size()]->submit_io_task(std::move(task));
                    jobs_count++;
                }
            }

            // Return immediately — tasks complete independently
            return compaction_stats{};
        }

        return stats;
    }

    hedge::expected<database::compaction_stats> database::_compaction_job_size_check(bool compact_all)
    {
        // this->_logger.log("Starting compaction job");

        sorted_indices_map_t indices_local_copy;

        {
            std::shared_lock lk(this->_sorted_index_mutex);

            // Acquire shared ownership of the current sorted indices for compaction
            // Every newly added sorted index during compaction will not be considered in this job
            indices_local_copy = this->_sorted_indices;
        }

        // It is needed in case new sorted indices are pushed back during compaction
        // We only want to remove the original indices that were present at the start of compaction
        std::unordered_map<size_t, size_t> initial_vec_sizes;
        for(auto& [prefix, vec] : indices_local_copy)
            initial_vec_sizes.emplace(prefix, vec.size());

        compaction_stats stats{};

        // Main compaction loop
        std::vector<hedge::error> errors;
        auto wg = async::wait_group::make_shared();

        wg->set(indices_local_copy.size()); // One task per partition

        auto iteration_id = this->_flush_iteration.fetch_add(1, std::memory_order::relaxed);

        auto make_compaction_sub_task = [this,
                                         &stats,
                                         &errors,
                                         iteration_id](
                                            size_t indices_to_merge,
                                            std::vector<sst_ptr_t>& ordered_indices_vec,
                                            std::shared_ptr<async::wait_group> wg) -> async::task<void>
        {
            // set lower priority for thread
            auto merge_config = hedge::db::index_ops::merge_config{
                .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
                .new_index_id = iteration_id,
                .base_path = this->_indices_path,
                .discard_deleted_keys = false,
                .create_new_with_odirect = this->_config.use_odirect_for_indices,
                .populate_cache_with_output = false,
                .try_reading_from_cache = true,
            };

            std::vector<const sst*> index_ptrs;
            index_ptrs.reserve(indices_to_merge);

            for(auto it = ordered_indices_vec.rbegin(); it != ordered_indices_vec.rbegin() + indices_to_merge; ++it)
                index_ptrs.emplace_back(it->get());

            auto maybe_compacted_table = co_await index_ops::k_way_merge_async2(
                merge_config,
                index_ptrs,
                async::this_thread_executor(),
                this->_page_cache);

            if(!maybe_compacted_table)
            {
                errors.emplace_back(std::format("An error occurred on compaction: {}", maybe_compacted_table.error().to_string()));
            }
            else
            {
                size_t input_bytes = 0;
                size_t items_merged = 0;
                for(auto it = ordered_indices_vec.rbegin(); it != ordered_indices_vec.rbegin() + indices_to_merge; ++it)
                {
                    input_bytes += (*it)->file_size();
                    items_merged += (*it)->size();
                }

                // Remove temporary (or not) tables
                for(auto it = ordered_indices_vec.rbegin(); it != ordered_indices_vec.rbegin() + indices_to_merge; ++it)
                    it->get()->set_delete_on_obj_destruction(true);

                // Replace the two merged indices with the new one
                size_t bytes_written = maybe_compacted_table.value().file_size();

                // erase the last `indices_to_merge` elements
                for(size_t i = 0; i < indices_to_merge; ++i)
                    ordered_indices_vec.pop_back();

                ordered_indices_vec.insert(ordered_indices_vec.end(), std::make_shared<sst>(std::move(maybe_compacted_table.value())));

                assert(check_is_sorted_by_epoch(ordered_indices_vec) && "Sorted indices vector is not sorted by epoch after compaction");

                // Update stats
                std::atomic_ref<size_t>(stats.input_bytes).fetch_add(input_bytes, std::memory_order::relaxed);
                std::atomic_ref<size_t>(stats.output_bytes).fetch_add(bytes_written, std::memory_order::relaxed);
                std::atomic_ref<size_t>(stats.num_inputs).fetch_add(indices_to_merge, std::memory_order::relaxed);
                std::atomic_ref<size_t>(stats.items_merged).fetch_add(items_merged, std::memory_order::relaxed);
            }

            wg->decr();
        };

        size_t total_included_indices = 0;

        std::vector<async::task<>> compaction_tasks;
        compaction_tasks.reserve(indices_local_copy.size());

        for(auto& [prefix, partition_vec] : indices_local_copy)
        {
            if(partition_vec.size() <= 1) // Nothing to compact
            {
                wg->decr();
                continue;
            }

            // Compact by epoch: newest indices get merged together first
            // It is expected that size increases with index seniority
            assert(check_is_sorted_by_epoch(partition_vec) && "Sorted indices vector is not sorted by epoch before compaction");

            // TODO: Check if issues might arise after bulk deletes
            size_t included_indices = 1;
            size_t cumulative_size = partition_vec.rbegin()->get()->size();

            if(compact_all)
            {
                included_indices = partition_vec.size();

                cumulative_size = std::accumulate(
                    partition_vec.begin(),
                    partition_vec.end(),
                    0UL,
                    [](size_t acc, const sst_ptr_t& idx_ptr)
                    {
                        return acc + idx_ptr->size();
                    });
            }
            else
            {

                // https://github.com/facebook/rocksdb/wiki/universal-compaction#3-compaction-triggered-by-number-of-sorted-runs-while-respecting-size_ratio
                // This approach is aimed at minimizing write amplification and making the most out of the write bandwidth
                // Take, for example, an indices with 6M items and a fresh flushed one with 1M items
                // assume target_compaction_size_ratio = 1.0
                // candidate_size / cumulative_size would be 6 which is > size_ratio: it's not worth to rewrite that 6M table because would be for the most part basically a copy operation
                //
                // Let's assume these are the sizes in millions of the first 5 indices: 1 1 1 2 6
                // candidate_size = 1, cumulative_size = 1: 1/1 <= 1.0 -> included indices = {1, 1}
                // candidate_size = 1, cumulative_size = 2: 1/(1+1) = 0.5 <= 1.0 -> included indices = {1, 1, 1}
                // candidate_size = 2, cumulative_size = 3: 2/(1+1+1) = 0.67 <= 1.0 -> included indices = {1, 1, 1, 2}
                // candidate_size = 6, cumulative_size = 5: 6/5 = 1.2 > 1.0 -> STOP (do not include index with 6M items)
                for(auto it = partition_vec.rbegin() + 1; it != partition_vec.rend(); ++it)
                {
                    size_t candidate_size = it->get()->size();
                    if((double)cumulative_size / this->_config.target_compaction_size_ratio <= (double)candidate_size ||
                       (double)candidate_size <= this->_config.target_compaction_size_ratio * (double)cumulative_size)
                    {
                        cumulative_size += it->get()->size();
                        ++included_indices;

                        constexpr size_t max_merge_width = 32;
                        if(included_indices >= max_merge_width)
                        {
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }

            constexpr auto min_merge_width = 4;

            if(included_indices < min_merge_width)
            {
                wg->decr();
                continue;
            }

            total_included_indices += included_indices;
            compaction_tasks.emplace_back(make_compaction_sub_task(
                included_indices,
                partition_vec,
                wg));

            // TODO: If only two sorted indices are left to compact, the merge_config.filter_deleted_keys should be true
            // Otherwise, if
            // - filter_delete_keys is `true`
            // - A key is in more than two sorted indices and is deleted in one of them
            // The key would appear again, because we lost track of it after merging the first two indices
        }

        {
            std::lock_guard lk(this->_pending_compactions_mutex);
            this->_pending_compacting_sst_count += total_included_indices;
        }

        size_t compaction_executor_id = 0;
        for(auto& t : compaction_tasks)
        {
            const auto& executor = this->_compation_executor_pool[compaction_executor_id++ % this->_compation_executor_pool.size()];
            executor->submit_io_task(std::move(t));
        }

        // Barrier waiting for this round of compaction sub-tasks to complete
        // TODO: This could be improved since we can the sorted indices belonging
        // to each partition can be compacted independently
        bool done = wg->wait_for(this->_config.compaction_timeout);

        if(!done)
            return hedge::error("compaction timeout.");

        // TODO: In case there are errors occurred at mid-term during compaction,
        // We won't loose the old sorted indices, but we might end up in stale state.
        // This is due to not tracking which merges succeeded and which failed.
        // So it will be hard or even impossible to reload the database correctly
        // because on file system there will be a mix of old and new sorted indices.
        // This should be solved by having a manifest file
        if(!errors.empty())
        {
            for(auto& error : errors)
                this->_logger.log("Compaction error: ", error.to_string());

            return hedge::error("Some errors occurred during compaction. Check the logs for details.");
        }

        {
            // After compaction, we can safely decrease the count of pending compactions
            std::lock_guard lk(this->_pending_compactions_mutex);
            this->_pending_compacting_sst_count -= total_included_indices;
            this->_pending_compactions_cv.notify_all();
        }

        // Finalize compaction: replace the database's sorted indices with the compacted ones
        // (so far, we have only worked on local copies)
        {
            std::unique_lock lk(this->_sorted_index_mutex);

            for(auto& [prefix, new_partition_vec] : indices_local_copy)
            {
                auto& db_sorted_indices_vec = this->_sorted_indices[prefix];

                auto range_start = db_sorted_indices_vec.begin();
                auto range_end = range_start + initial_vec_sizes[prefix]; // In case new indices were pushed back while we were busy with compaction here

                std::for_each(range_start, range_end, [](const std::shared_ptr<sst>& sorted_index)
                              { sorted_index->set_delete_on_obj_destruction(true); }); // Mark old indices for deletion
                                                                                       // At any time, there might be some coroutine owning the sorted_index shared_ptr,
                                                                                       // so actual deletion will happen when the last shared_ptr goes completely out of scope

                // Again: we cannot clear the entire vector because we could accidentally delete some table that was added in the mean while
                // Here, some indices might get out of scope and get deleted if no other shared_ptr owns them
                db_sorted_indices_vec.erase(range_start, range_end);

                db_sorted_indices_vec.insert(db_sorted_indices_vec.begin(),
                                             new_partition_vec.begin(),
                                             new_partition_vec.end());

                // This is needed when the `new_sorted_index_vec` still contains some of the original sorted indices shared_ptrs.
                // Otherwise, we would accidentally delete them.
                std::ranges::for_each(new_partition_vec, [](const std::shared_ptr<sst>& sorted_index)
                                      { sorted_index->set_delete_on_obj_destruction(false); });
            }
        }

        // this->_logger.log("Compaction job completed successfully");

        return stats;
    }

    std::future<expected<database::compaction_stats>> database::compact_sorted_indices(bool compact_all)
    {
        auto compaction_promise_ptr = std::make_shared<std::promise<hedge::expected<compaction_stats>>>();
        std::future<hedge::expected<compaction_stats>> compaction_future = compaction_promise_ptr->get_future();

        if(compact_all)
        {
            // compact_all=true: synchronous — wait for flush, run compaction, block until done
            this->_compaction_worker.submit(
                [db = this->shared_from_this(), promise = std::move(compaction_promise_ptr)]()
                {
                    auto future = db->_memtable.wait_for_flush();
                    future.wait();

                    auto start_processing_t = std::chrono::high_resolution_clock::now();

                    auto maybe_stats = db->_compaction_job_size_tiered(true);
                    if(!maybe_stats)
                        db->_logger.log("Compaction job failed: ", maybe_stats.error().to_string());

                    auto t1 = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - start_processing_t);

                    auto& stats = maybe_stats.value();

                    double write_throughput_mbs = stats.output_bytes / ((double)duration.count() / 1000000.0) / (1000.0 * 1000.0);

                    if(stats.output_bytes > 0)
                    {
                        db->_logger.log("Total duration for compaction: ", (double)duration.count() / 1000.0, " ms",
                                        ", MB written: ", stats.output_bytes / (1000.0 * 1000.0),
                                        ", MB read: ", stats.input_bytes / (1000.0 * 1000.0),
                                        ", throughput: ", write_throughput_mbs, " MB/s",
                                        ", items merged: ", stats.items_merged,
                                        ", items merged/s: ", (size_t)((double)stats.items_merged / ((double)duration.count() / 1000000.0)),
                                        ", num input files: ", stats.num_inputs);

                        prof::get<"merge_mb_written">().add(stats.output_bytes / (1000.0 * 1000.0));
                        prof::get<"merge_throughput_mbs">().add(write_throughput_mbs);
                    }

                    promise->set_value(stats);
                });
        }
        else
        {
            // compact_all=false: non-blocking — submit compaction job, set promise immediately
            this->_compaction_worker.submit(
                [db = this->shared_from_this(), promise = std::move(compaction_promise_ptr)]()
                {
                    auto maybe_stats = db->_compaction_job_size_tiered(false);
                    if(!maybe_stats)
                        db->_logger.log("Compaction job failed: ", maybe_stats.error().to_string());

                    promise->set_value(maybe_stats);
                });
        }

        return compaction_future;
    }

    void database::wait_for_compactions_to_finish()
    {
        this->_memtable.wait_for_flush().wait();
        this->_compaction_worker.wait_for_all_jobs();
        // Wait for all in-flight self-completing tasks on executor pool
        std::unique_lock lk(this->_pending_compactions_mutex);
        this->_pending_compactions_cv.wait(lk, [this]()
                                           { return this->_pending_compacting_sst_count == 0; });
    }

    [[nodiscard]] double database::read_amplification_factor()
    {
        std::shared_lock lk(this->_sorted_index_mutex);
        double total_read_amplification = 0.0;

        for(const auto& [prefix, indices] : this->_sorted_indices)
            total_read_amplification += indices.size();

        if(this->_sorted_indices.empty())
            return 0.0;

        return total_read_amplification / this->_sorted_indices.size();
    }

    hedge::status database::flush()
    {
        return hedge::error("flush not implemented");
    }

} // namespace hedge::db