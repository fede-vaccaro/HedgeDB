#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <error.hpp>

#include "async/wait_group.h"
#include "cache.h"
#include "database.h"
#include "db/memtable.h"
#include "index_ops.h"
#include "io_executor.h"
#include "perf_counter.h"
#include "sorted_index.h"
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
            db->_page_cache = std::make_shared<shared_page_cache>(config.index_page_clock_cache_size_bytes, async::executor_pool::static_pool().size() * 4);

        // Setup memindex
        db->_memtable.~memtable();
        auto* memtable_mem = &db->_memtable;
        ::new(memtable_mem) memtable(
            memtable_config{
                .max_inserts_cap = config.keys_in_mem_before_flush,
                .memory_budget_cap = 128 * 1024 * 1024, // default
                .auto_compaction = config.auto_compaction,
                .use_odirect = config.use_odirect_for_indices,
                .num_writer_threads = async::executor_pool::static_pool().size(),
            },
            config.num_partition_exponent,
            db->_indices_path,
            &db->_flush_iteration,
            make_push_indices_callback(db),
            make_compaction_callback(db),
            db->_page_cache);

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
            write_buffer_ptr = std::make_unique<write_buffer>(WRITE_BUFFER_DEFAULT_SIZE);
            auto vtable = db->_current_value_table.load();
            auto allocated_offset = vtable->allocate_pages_for_write(WRITE_BUFFER_DEFAULT_SIZE);
            write_buffer_ptr->set(vtable, allocated_offset.value());
        }

        return db;
    }

    std::function<void(std::vector<sorted_index>)> database::make_push_indices_callback(const std::shared_ptr<database>& db)
    {
        return [db](std::vector<sorted_index> new_indices)
        {
            std::lock_guard sorted_index_lk(db->_sorted_index_mutex);

            for(auto& new_sorted_index : new_indices)
            {
                new_sorted_index.clear_index();
                auto prefix = new_sorted_index.upper_bound();

                auto sorted_index_ptr = std::make_shared<sorted_index>(std::move(new_sorted_index));
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

    async::task<hedge::status> database::put_async(key_t key, const byte_buffer_t& value)
    {
        // prof::counter_guard guard(prof::get<"put_async">());

        // --- Try writing value to buffer first ---
        auto _allocate_space_for_write_buffer = [this](write_buffer& write_buffer) -> hedge::status
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
        this->_memtable.put(key, maybe_write.value());

        co_return hedge::ok();
    }

    // async::task<hedge::status> database::put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor)
    // {
    //     co_return co_await this->put_async(key, value, executor);
    // }

    async::task<expected<std::pair<value_ptr_t, std::shared_ptr<value_table>>>> database::_find_value_ptr_and_value_table(key_t key)
    {
        // Step 1: Check the memtable first (contains the most recent data).
        std::optional<value_ptr_t> value_ptr_opt;

        value_ptr_opt = this->_memtable.get(key);

        // Step 2: If not found in memtable and cache, search the relevant sorted index files.
        if(!value_ptr_opt.has_value())
        {
            size_t matching_partition_id = this->_find_matching_partition_for_key(key);
            std::vector<sorted_index_ptr_t> sorted_indices_local; // Local copy for safe iteration

            {
                std::shared_lock lk(this->_sorted_index_mutex);
                auto sorted_indices_it = this->_sorted_indices.find(matching_partition_id);

                if(sorted_indices_it == this->_sorted_indices.end())
                    co_return hedge::error("Key partition not found in sorted indices", errc::KEY_NOT_FOUND);

                sorted_indices_local = sorted_indices_it->second;
            }

            // Indices are sorted by epoch: newest (and most recent key first)
            // TODO: Add bloom filter support to skip unnecessary lookups.
            for(const auto& sorted_index_ptr : std::ranges::reverse_view(sorted_indices_local))
            {
                auto maybe_value_ptr = co_await sorted_index_ptr->lookup_async(key, this->_page_cache);
                if(!maybe_value_ptr.has_value())
                    co_return hedge::error(std::format("An error occurred while reading index at path {}: {}", sorted_index_ptr->path().string(), maybe_value_ptr.error().to_string()));

                if(auto& opt_value_ptr = maybe_value_ptr.value(); opt_value_ptr.has_value())
                {
                    value_ptr_opt = opt_value_ptr.value();
                    break; // Found the key in this index, no need to check older indices
                }
            }
        }

        if(!value_ptr_opt.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        if(value_ptr_opt.value().is_deleted())
            co_return hedge::error("Key deleted from index", errc::DELETED);

        // Step 4: Find the corresponding value table based on the table ID in the value pointer.
        auto table_finder = [this](uint32_t id) -> std::shared_ptr<value_table>
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
        };

        // Get the table pointer using the finder lambda.
        std::shared_ptr<value_table> table_ptr = table_finder(value_ptr_opt->table_id());
        if(table_ptr == nullptr)
            co_return hedge::error("Value table with ID " + std::to_string(value_ptr_opt->table_id()) + " not found");

        co_return {value_ptr_opt.value(), std::move(table_ptr)};
    }

    async::task<expected<database::byte_buffer_t>> database::get_async(key_t key)
    {
        auto maybe_value_ptr_table = co_await this->_find_value_ptr_and_value_table(key);

        if(!maybe_value_ptr_table)
            co_return maybe_value_ptr_table.error();

        auto [value_ptr, table_ptr] = std::move(maybe_value_ptr_table.value());

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

    async::task<hedge::status> database::remove_async(key_t key)
    {
        // First, locate the latest version of the key (value_ptr and its table).
        // This is necessary to potentially mark the entry deleted in the value log itself (if not already handled by GC).
        auto maybe_value_ptr_table = co_await this->_find_value_ptr_and_value_table(key);

        // TODO: We might want to allow deleting non-existing keys (idempotent delete)
        // and completely skip the lookup. This will create some overhead in the memtable
        // bu7t should not be inconsistent
        if(!maybe_value_ptr_table)
            co_return maybe_value_ptr_table.error();

        auto [value_ptr, table_ptr] = std::move(maybe_value_ptr_table.value());

        // TODO: As written above, we might want to allow deleting non-existing keys
        // Hence, we might want to always run this step
        this->_memtable.put(key, value_ptr_t::apply_delete(value_ptr));

        co_return hedge::ok();
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

    hedge::expected<size_t> database::_compaction_job(bool ignore_size_ratio)
    {
        this->_logger.log("Starting compaction job");

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

        std::atomic_size_t total_bytes_written{0};

        // Main compaction loop
        std::vector<hedge::error> errors;
        auto wg = async::wait_group::make_shared();

        wg->set(indices_local_copy.size()); // One task per partition

        auto make_compaction_sub_task = [this,
                                         &total_bytes_written,
                                         &errors,
                                         this_iteration_id = this->_flush_iteration.fetch_add(1)](
                                            size_t indices_to_merge,
                                            std::vector<sorted_index_ptr_t>& ordered_indices_vec,
                                            std::shared_ptr<async::wait_group> wg) -> async::task<void>
        {
            // set lower priority for thread
            auto merge_config = hedge::db::index_ops::merge_config{
                .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
                .new_index_id = this_iteration_id,
                .base_path = this->_indices_path,
                .discard_deleted_keys = false,
                .create_new_with_odirect = this->_config.use_odirect_for_indices,
                .populate_cache_with_output = true,
                .try_reading_from_cache = true,
            };

            std::vector<const sorted_index*> index_ptrs;
            index_ptrs.reserve(indices_to_merge);

            for(auto it = ordered_indices_vec.rbegin(); it != ordered_indices_vec.rbegin() + indices_to_merge; ++it)
                index_ptrs.emplace_back(it->get());

            auto maybe_compacted_table = co_await index_ops::k_way_merge_async(
                merge_config,
                index_ptrs,
                async::this_thread_executor(),
                this->_page_cache);

            if(!maybe_compacted_table)
            {
                errors.emplace_back(std::format("An error occurred on compaction: {}", errors.front().to_string()));
            }
            else
            {
                // Remove temporary (or not) tables
                for(auto it = ordered_indices_vec.rbegin(); it != ordered_indices_vec.rbegin() + indices_to_merge; ++it)
                    it->get()->set_delete_on_obj_destruction(true);

                // Replace the two merged indices with the new one
                size_t bytes_written = maybe_compacted_table.value().file_size();

                // erase the last `indices_to_merge` elements
                for(size_t i = 0; i < indices_to_merge; ++i)
                    ordered_indices_vec.pop_back();

                ordered_indices_vec.insert(ordered_indices_vec.end(), std::make_shared<sorted_index>(std::move(maybe_compacted_table.value())));

                assert(check_is_sorted_by_epoch(ordered_indices_vec) && "Sorted indices vector is not sorted by epoch after compaction");

                total_bytes_written.fetch_add(bytes_written, std::memory_order::relaxed);
            }

            wg->decr();
        };

        size_t compaction_executor_id = 0;
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

            if(ignore_size_ratio)
            {
                included_indices = partition_vec.size();

                cumulative_size = std::accumulate(
                    partition_vec.begin(),
                    partition_vec.end(),
                    0UL,
                    [](size_t acc, const sorted_index_ptr_t& idx_ptr)
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
                // candidate_size = 6, cumulative_size = 5: 6/5 = 1.2 > 0 -> STOP (do not include index with 6M items)
                for(auto it = partition_vec.rbegin() + 1; it != partition_vec.rend(); ++it)
                {
                    size_t candidate_size = it->get()->size();
                    if((double)candidate_size / (double)cumulative_size <= this->_config.target_compaction_size_ratio)
                    {
                        cumulative_size += it->get()->size();
                        ++included_indices;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            if(included_indices < 2)
            {
                wg->decr();
                continue;
            }

            // TODO: If only two sorted indices are left to compact, the merge_config.filter_deleted_keys should be true
            // Otherwise, if
            // - filter_delete_keys is `true`
            // - A key is in more than two sorted indices and is deleted in one of them
            // The key would appear again, because we lost track of it after merging the first two indices

            // this->_logger.log("Triggering compaction of ", std::to_string(included_indices), " indices for ", std::to_string(cumulative_size / 1000), "K key pairs");
            // for(auto it = partition_vec.rbegin(); it != partition_vec.rbegin() + included_indices; ++it)
            // {
            //     this->_logger.log(" - Index path: ", (*it)->path().string(),
            //                       ", epoch: ", (*it)->epoch(),
            //                       ", size: ", (*it)->size() / 1000, "K key pairs");
            // }
            const auto& executor = this->_compation_executor_pool[compaction_executor_id++ % this->_compation_executor_pool.size()];
            executor->submit_io_task(make_compaction_sub_task(included_indices, partition_vec, wg));
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

        // Finalize compaction: replace the database's sorted indices with the compacted ones
        // (so far, we have only worked on local copies)
        {
            std::unique_lock lk(this->_sorted_index_mutex);

            for(auto& [prefix, new_partition_vec] : indices_local_copy)
            {
                auto& db_sorted_indices_vec = this->_sorted_indices[prefix];

                auto range_start = db_sorted_indices_vec.begin();
                auto range_end = range_start + initial_vec_sizes[prefix]; // In case new indices were pushed back while we were busy with compaction here

                std::for_each(range_start, range_end, [](const std::shared_ptr<sorted_index>& sorted_index)
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
                std::ranges::for_each(new_partition_vec, [](const std::shared_ptr<sorted_index>& sorted_index)
                                      { sorted_index->set_delete_on_obj_destruction(false); });
            }
        }

        this->_logger.log("Compaction job completed successfully");

        return total_bytes_written.load();
    }

    std::future<expected<size_t>> database::compact_sorted_indices(bool ignore_size_ratio)
    {
        auto compaction_promise_ptr = std::make_shared<std::promise<hedge::expected<size_t>>>();
        std::future<hedge::expected<size_t>> compaction_future = compaction_promise_ptr->get_future();

        this->_compaction_worker.submit(
            [db = this->shared_from_this(), promise = std::move(compaction_promise_ptr), ignore_size_ratio]()
            {
                db->_memtable.wait_for_flush().wait();

                auto start_processing_t = std::chrono::high_resolution_clock::now();

                auto maybe_bytes_written = db->_compaction_job(ignore_size_ratio);
                if(!maybe_bytes_written)
                    db->_logger.log("Compaction job failed: ", maybe_bytes_written.error().to_string());

                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - start_processing_t);

                double throughput_mbs = maybe_bytes_written.value() / ((double)duration.count() / 1000000.0) / (1000.0 * 1000.0);

                if(maybe_bytes_written.value() > 0)
                {
                    db->_logger.log("Total duration for compaction: ", (double)duration.count() / 1000.0, " ms",
                                    ", MB written: ", maybe_bytes_written.value() / (1000.0 * 1000.0),
                                    ", throughput: ", throughput_mbs, " MB/s");

                    prof::get<"merge_mb_written">().add(maybe_bytes_written.value() / (1000.0 * 1000.0));
                    prof::get<"merge_throughput_mbs">().add(throughput_mbs);
                }

                // todo: fix std::future_error No associated state if the future was already retrieved with wait_for()
                promise->set_value(std::move(maybe_bytes_written));
            });

        this->_logger.log("Compaction job submitted to background worker.");

        return compaction_future;
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