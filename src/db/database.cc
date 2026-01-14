#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>

#include <error.hpp>

#include "async/wait_group.h"
#include "cache.h"
#include "database.h"
#include "db/mem_index.h"
#include "index_ops.h"
#include "io_executor.h"
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

        // TODO: Write a manifest file to store configuration and database state (e.g., last table ID).
        // This might be relevant for correctly loading the database later.

        db->_mem_index.reserve(config.keys_in_mem_before_flush);

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

        // Init clock cache
        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1) // Minimum 1 MB page cache
            db->_page_cache = std::make_shared<shared_page_cache>(config.index_page_clock_cache_size_bytes, async::executor_pool::static_pool().size() * 4);

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

    expected<std::shared_ptr<database>> database::load(const std::filesystem::path& base_path, const db::db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_indices_path = base_path / "indices";
        db->_values_path = base_path / "values";

        // TODO: consinstency check
        db->_config = config;

        db->_mem_index.reserve(config.keys_in_mem_before_flush);

        // Load SSTs
        size_t max_flush_iteration = 0;
        for(const auto& entry : std::filesystem::directory_iterator(db->_indices_path))
        {
            if(!entry.is_directory())
                continue;

            // parse from hex to uint16_t
            uint16_t partition_id = std::stoul(entry.path().filename().string(), nullptr, 16);
            db->_sorted_indices[partition_id] = std::vector<sorted_index_ptr_t>{};

            for(const auto& file_entry : std::filesystem::directory_iterator(entry.path()))
            {
                if(file_entry.is_regular_file() && file_entry.path().filename() == file_entry.path().filename())
                {
                    auto maybe_index = index_ops::load_sorted_index(file_entry.path(), config.use_odirect_for_indices, false);
                    if(!maybe_index)
                        return hedge::error("Failed to load sorted index from file " + file_entry.path().string() + ": " + maybe_index.error().to_string());

                    db->_sorted_indices[partition_id].push_back(std::make_shared<sorted_index>(std::move(maybe_index.value())));

                    size_t flush_iteration = std::stoul(file_entry.path().extension().string().substr(1), nullptr, 10);
                    max_flush_iteration = std::max(max_flush_iteration, flush_iteration);
                }
            }
        }

        db->_flush_iteration = max_flush_iteration + 1;

        uint32_t max_last_table_id{0};
        for(const auto& entry : std::filesystem::directory_iterator(db->_values_path))
        {
            if(entry.is_regular_file() && entry.path().extension() == value_table::TABLE_FILE_EXTENSION)
            {
                auto table = value_table::load(entry.path(), fs::file::open_mode::read_only, true);
                if(!table)
                    return hedge::error("Failed to load value table from file " + entry.path().string() + ": " + table.error().to_string());

                max_last_table_id = std::max(max_last_table_id, table.value()->id());
                db->_value_tables[table.value()->id()] = std::move(table.value());
            }
        }

        auto first_value_table = value_table::make_new(db->_values_path, max_last_table_id + 1);
        if(!first_value_table)
            return hedge::error("Failed to create initial value table: " + first_value_table.error().to_string());

        db->_current_value_table = std::move(first_value_table.value());
        db->_last_table_id = 0;

        auto pipelined_value_table = value_table::make_new(db->_values_path, max_last_table_id + 2);
        if(!pipelined_value_table)
            return hedge::error("Failed to create next in pipeline value table: " + pipelined_value_table.error().to_string());

        db->_pipelined_value_table = std::move(pipelined_value_table.value());

        db->_last_table_id = max_last_table_id + 1; // pipelined table is last_table_id + 1

        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1) // Minimum 1 MB page cache
            db->_page_cache = std::make_shared<shared_page_cache>(config.index_page_clock_cache_size_bytes, async::executor_pool::static_pool().size() * 4);

        if(config.index_point_cache_size_bytes > 1024 * 1024 * 1) // Mininum 1MB entry cache
            db->_index_point_cache = std::make_shared<point_cache>(config.index_point_cache_size_bytes);

        db->_write_buffers.resize(async::executor_pool::static_pool().size());

        for(auto& write_buffer_ptr : db->_write_buffers)
        {
            write_buffer_ptr = std::make_unique<write_buffer>(WRITE_BUFFER_DEFAULT_SIZE);
            auto vtable = db->_current_value_table.load();
            auto allocated_offset = vtable->allocate_pages_for_write(WRITE_BUFFER_DEFAULT_SIZE);
            write_buffer_ptr->set(vtable, allocated_offset.value());
        }

        return hedge::error("Load database from path not implemented yet");
    }

    async::task<hedge::status> database::put_async(key_t key, const byte_buffer_t& value)
    {
        if(auto size = this->_mem_index_size.load(); size >= this->_config.keys_in_mem_before_flush) // Check if the memtable needs flushing
        {
            if(this->_mem_index_size.compare_exchange_strong(size, 0))
            {
                // It might happen that some items get pushed _before_ acquiring the lock: this implies that _mem_index_size and the actual size are desynced,
                // but is not really problematic: it will only happen that there will be more element in the new sst than expected

                std::unique_lock lk(this->_mem_index_mutex);
                auto old_mem_index = std::move(this->_mem_index);

                this->_mem_index = mem_index{};
                this->_mem_index.reserve(this->_config.keys_in_mem_before_flush);

                size_t curr_flush_iteration = this->_flush_iteration.fetch_add(1, std::memory_order_relaxed);

                // map's pointers are stable
                mem_index* flushing_mem_index = nullptr;

                {
                    std::unique_lock lk_pending(this->_pending_flushes_mutex);
                    this->_pending_flushes[curr_flush_iteration] = std::move(old_mem_index);
                    flushing_mem_index = &this->_pending_flushes[curr_flush_iteration];
                }

                this->_flush_worker.submit(
                    [this, flushing_mem_index, curr_flush_iteration]()
                    {
                        if(auto status = this->_flush_mem_index(flushing_mem_index, curr_flush_iteration); !status)
                        {
                            this->_logger.log("An error occurred while flushing the mem_index: " + status.error().to_string());
                            return;
                        }

                        if(this->_config.auto_compaction)
                            (void)this->compact_sorted_indices(false);
                    });
            }
        }

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

        static thread_local size_t this_thread_idx = database::_thread_count.fetch_add(1);
        assert(this_thread_idx < this->_write_buffers.size());

        const auto& write_buffer_ref = this->_write_buffers[this_thread_idx];
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

        // --- Update Memtable 7---
        {
            std::unique_lock lk(this->_mem_index_mutex);
            this->_mem_index.put(key, maybe_write.value());
            this->_mem_index_size.fetch_add(1);
        }

        co_return hedge::ok();
    }

    // async::task<hedge::status> database::put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor)
    // {
    //     co_return co_await this->put_async(key, value, executor);
    // }

    auto CHRONO_NOW() { return std::chrono::high_resolution_clock::now(); }

    async::task<expected<std::pair<value_ptr_t, std::shared_ptr<value_table>>>> database::_find_value_ptr_and_value_table(key_t key)
    {
        // Step 1: Check the memtable first (contains the most recent data).
        std::optional<value_ptr_t> value_ptr_opt;

        {
            std::shared_lock lk(this->_mem_index_mutex);
            value_ptr_opt = this->_mem_index.get(key);
        }

        // Step 1.1: Also check pending flushes
        // We reverse iterate: if there are multiple pending flushes, the most recent is the correct one
        {
            std::shared_lock lk(this->_pending_flushes_mutex);
            for(auto& pending_flush : std::ranges::reverse_view(this->_pending_flushes))
            {
                auto& pending_memtable = pending_flush.second;
                value_ptr_opt = pending_memtable.get(key);
                if(value_ptr_opt.has_value())
                    break;
            }
        }

        // Step 1.2: Check entry cache
        std::optional<value_ptr_t> cached_key;
        if(!value_ptr_opt && this->_index_point_cache)
        {
            cached_key = this->_index_point_cache->lookup(key);
            value_ptr_opt = cached_key;
        }

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

            // Need to search ALL indices in the partition and find the one with the highest priority `value_ptr_t`.
            // TODO: Add bloom filter support to skip unnecessary lookups.
            for(const auto& sorted_index_ptr : sorted_indices_local)
            {
                auto maybe_value_ptr = co_await sorted_index_ptr->lookup_async(key, this->_page_cache);
                if(!maybe_value_ptr.has_value())
                    co_return hedge::error(std::format("An error occurred while reading index at path {}: {}", sorted_index_ptr->path().string(), maybe_value_ptr.error().to_string()));

                if(auto& opt_value_ptr = maybe_value_ptr.value(); opt_value_ptr.has_value())
                {
                    if(!value_ptr_opt.has_value() || (value_ptr_opt && *opt_value_ptr < *value_ptr_opt))
                        value_ptr_opt = opt_value_ptr.value();
                }
            }
        }

        if(!value_ptr_opt.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        if(value_ptr_opt.value().is_deleted())
            co_return hedge::error("Key deleted from index", errc::DELETED);

        // Update cache if possible
        if(cached_key != value_ptr_opt && this->_index_point_cache)
            this->_index_point_cache->put(key, value_ptr_opt.value());

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
        {
            std::unique_lock lk(this->_mem_index_mutex);
            this->_mem_index.put(key, value_ptr_t::apply_delete(value_ptr));
        }
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

    hedge::status database::_flush_mem_index(mem_index* memtable_to_flush, size_t flush_iteration)
    {
        auto t0 = std::chrono::high_resolution_clock::now();

        this->_logger.log("Flushing mem index to ", this->_indices_path);

        auto partitioned_sorted_indices = index_ops::flush_mem_index(this->_indices_path,
                                                                     memtable_to_flush,
                                                                     this->_config.num_partition_exponent,
                                                                     flush_iteration,
                                                                     this->_config.use_odirect_for_indices);

        if(!partitioned_sorted_indices)
            return hedge::error("An error occurred while flushing the mem index: " + partitioned_sorted_indices.error().to_string());

        {
            std::lock_guard lk(this->_pending_flushes_mutex);
            this->_pending_flushes.erase(flush_iteration);
        }

        {
            std::lock_guard lk(this->_sorted_index_mutex);

            for(auto& new_sorted_index : partitioned_sorted_indices.value())
            {
                new_sorted_index.clear_index();
                auto prefix = new_sorted_index.upper_bound();

                auto sorted_index_ptr = std::make_shared<sorted_index>(std::move(new_sorted_index));
                this->_sorted_indices[prefix].emplace_back(std::move(sorted_index_ptr));
            }
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        this->_logger.log("Mem index flushed in ", (double)duration.count() / 1000.0, " ms");

        return hedge::ok();
    }

    hedge::status database::_compaction_job(bool ignore_ratio)
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

        bool stability_reached = false; // @see database::compact_sorted_indices() to understand compaction stability

        // Main compaction loop
        while(!stability_reached)
        {
            stability_reached = true;

            std::vector<hedge::error> errors;
            auto wg = async::wait_group::make_shared();

            wg->set(indices_local_copy.size()); // One task per partition

            auto make_compaction_sub_task = [this,
                                             &errors,
                                             this_iteration_id = this->_flush_iteration.fetch_add(1)](
                                                std::vector<sorted_index_ptr_t>& ordered_indices_vec,
                                                std::shared_ptr<async::wait_group> wg) -> async::task<void>
            {
                auto smallest_index_it = ordered_indices_vec.begin() + (ordered_indices_vec.size() - 1);
                auto second_smallest_index_it = smallest_index_it - 1;

                auto merge_config = hedge::db::index_ops::merge_config{
                    .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
                    .new_index_id = this_iteration_id,
                    .base_path = this->_indices_path,
                    .discard_deleted_keys = false,
                    .create_new_with_odirect = this->_config.use_odirect_for_indices,
                    .precache_output_vec = true,
                };

                auto maybe_compacted_table = co_await index_ops::two_way_merge_async(
                    merge_config,
                    **second_smallest_index_it,
                    **smallest_index_it,
                    async::this_thread_executor(),
                    this->_page_cache);

                if(!maybe_compacted_table)
                {
                    errors.emplace_back(
                        std::format("An error occurred while compacting {} and {}: {}",
                                    (*smallest_index_it)->path().string(),
                                    (*second_smallest_index_it)->path().string(),
                                    maybe_compacted_table.error().to_string()));
                }
                else
                {
                    // Remove temporary (or not) tables
                    (*smallest_index_it)->set_delete_on_obj_destruction(true);
                    (*second_smallest_index_it)->set_delete_on_obj_destruction(true);

                    // Replace the two merged indices with the new one
                    ordered_indices_vec.erase(smallest_index_it);
                    ordered_indices_vec.erase(second_smallest_index_it);

                    ordered_indices_vec.emplace_back(std::make_shared<sorted_index>(std::move(maybe_compacted_table.value())));
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

                // Compacts the two smallest sorted indices in the partition if the size ratio condition
                // This minimizes the number of overall operations if we stick to the two_ways_merge strategy
                // K-way merge could be more efficient but is more complex to implement
                std::ranges::sort(
                    partition_vec,
                    [](const sorted_index_ptr_t& lhs, const sorted_index_ptr_t& rhs)
                    {
                        return lhs->size() > rhs->size();
                    });

                auto smallest_index = static_cast<double>(partition_vec[partition_vec.size() - 1]->size());
                auto second_smallest_index = static_cast<double>(partition_vec[partition_vec.size() - 2]->size());

                if(!ignore_ratio && smallest_index / second_smallest_index <= this->_config.target_compaction_size_ratio)
                {
                    wg->decr();
                    continue;
                }

                stability_reached = false;

                // TODO: If only two sorted indices are left to compact, the merge_config.filter_deleted_keys should be true
                // Otherwise, if
                // - filter_delete_keys is `true`
                // - A key is in more than two sorted indices and is deleted in one of them
                // The key would appear again, because we lost track of it after merging the first two indices
                const auto& executor = this->_compation_executor_pool[compaction_executor_id++ % this->_compation_executor_pool.size()];
                executor->submit_io_task(make_compaction_sub_task(partition_vec, wg));
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

                db_sorted_indices_vec.insert(db_sorted_indices_vec.end(),
                                             new_partition_vec.begin(),
                                             new_partition_vec.end());

                // This is needed when the `new_sorted_index_vec` still contains some of the original sorted indices shared_ptrs.
                // Otherwise, we would accidentally delete them.
                std::ranges::for_each(new_partition_vec, [](const std::shared_ptr<sorted_index>& sorted_index)
                                      { sorted_index->set_delete_on_obj_destruction(false); });
            }
        }

        this->_logger.log("Compaction job completed successfully");

        return hedge::ok();
    }

    std::future<hedge::status> database::compact_sorted_indices(bool ignore_ratio)
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        auto compaction_promise_ptr = std::make_shared<std::promise<hedge::status>>();
        std::future<hedge::status> compaction_future = compaction_promise_ptr->get_future();

        this->_compaction_worker.submit(
            [t0, weak_db = this->weak_from_this(), promise = std::move(compaction_promise_ptr), ignore_ratio]()
            {
                auto db = weak_db.lock();
                if(!db)
                {
                    promise->set_value(hedge::error("Database instance destroyed before compaction could run."));
                    db->_logger.log("Compaction job aborted: DB instance weak_ptr expired.");
                    return;
                }

                hedge::status status = db->_compaction_job(ignore_ratio);
                if(!status)
                    db->_logger.log("Compaction job failed: ", status.error().to_string());

                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                db->_logger.log("Total duration for compaction: ", (double)duration.count() / 1000.0, " ms");

                // todo: fix std::future_error No associated state if the future was already retrieved with wait_for()
                promise->set_value(std::move(status));
            });

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
        auto old_mem_index = std::move(this->_mem_index);

        if(auto status = this->_flush_mem_index(&old_mem_index, this->_flush_iteration.fetch_add(1, std::memory_order_relaxed)); !status)
            return hedge::error("An error occurred while flushing the mem_index: " + status.error().to_string());

        for(auto& write_buffer_ptr : this->_write_buffers)
        {
            auto flush_task = [&write_buffer_ptr]() -> async::task<hedge::status>
            {
                co_return co_await write_buffer_ptr->flush(async::this_thread_executor());
            };

            auto buffer_flush_status = async::executor_pool::executor_from_static_pool()->sync_submit(flush_task());
            if(!buffer_flush_status)
                return hedge::error("An error occurred while flushing a write buffer: " + buffer_flush_status.error().to_string());
        }

        this->_logger.log("Manual flush completed successfully.");
        return hedge::ok();
    }

} // namespace hedge::db