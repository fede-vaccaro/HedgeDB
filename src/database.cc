#include <algorithm>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "common.h"
#include "database.h"
#include "error.hpp"
#include "index.h"
#include "value_table.h"
#include "working_group.h"

namespace hedge::db
{
    expected<std::shared_ptr<database>> database::make_new(const std::filesystem::path& base_path, const db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_indices_path = base_path / "indices";
        db->_values_path = base_path / "values";
        db->_config = config;

        if(config.num_partition_exponent > db_config::MAX_PARTITION_EXPONENT)
            return hedge::error("num_partition_exponent must be <= " + std::to_string(db_config::MAX_PARTITION_EXPONENT));

        if(config.keys_in_mem_before_flush < db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH)
            return hedge::error("keys_in_mem_before_flush must be >= " + std::to_string(db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH));

        if(std::filesystem::exists(db->_base_path))
            return hedge::error("Database path already exists: " + db->_base_path.string());

        std::filesystem::create_directories(db->_base_path);
        std::filesystem::create_directories(db->_indices_path);
        std::filesystem::create_directories(db->_values_path);

        // todo write manifest file
        //

        // init inner data structures
        db->_mem_index.reserve(config.keys_in_mem_before_flush);

        auto new_value_table = value_table::make_new(db->_values_path, 0);
        if(!new_value_table)
            return hedge::error("Failed to create value table: " + new_value_table.error().to_string());

        db->_current_value_table = std::make_shared<value_table>(std::move(new_value_table.value()));
        return db;
    }

    expected<std::shared_ptr<database>> database::load(const std::filesystem::path& /* base_path */)
    {
        return hedge::error("Load database from path not implemented yet");
    }

    async::task<hedge::status> database::put_async(key_t key, const byte_buffer_t& value, const std::shared_ptr<async::executor_context>& executor)
    {
        // lazy flush the memtable if no space left in the memtable
        if(this->_mem_index.size() >= this->_config.keys_in_mem_before_flush)
        {
            if(auto status = this->_flush_mem_index(); !status)
                co_return hedge::error("An error occurred while flushing the mem_index: " + status.error().to_string());

            // trigger compaction
            if(this->_config.auto_compaction)
                this->compact_sorted_indices(false, executor);
        }

        // first it tries writing the data to the value table
        auto value_table = this->_current_value_table; // acquire ownership of the current value table
        auto reservation = value_table->get_write_reservation(value.size());

        // make a new value table if the space limit is reached
        if(!reservation && reservation.error().code() == hedge::errc::VALUE_TABLE_NOT_ENOUGH_SPACE)
        {
            if(auto status = this->_rotate_value_table(); !status)
                co_return hedge::error("Failed to rotate value table: " + status.error().to_string());

            value_table = this->_current_value_table; // acquire ownership of the new value table
            reservation = value_table->get_write_reservation(value.size());
        }

        if(!reservation)
            co_return hedge::error("Failed to reserve space in value table: " + reservation.error().to_string());

        // execute the actual writes
        auto write_response = co_await value_table->write_async(key, value, reservation.value(), executor);

        if(!write_response)
            co_return hedge::error("Failed to write value to value table: " + write_response.error().to_string());

        this->_mem_index.put(key, write_response.value());

        co_return hedge::ok();
    }

    async::task<hedge::status> database::put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor)
    {
        co_return co_await this->put_async(key, value, executor);
    }

    async::task<expected<std::pair<value_ptr_t, std::shared_ptr<value_table>>>> database::_find_value_ptr_and_value_table(key_t key, const std::shared_ptr<async::executor_context>& executor)
    {
        // try from memtable
        auto value_ptr = this->_mem_index.get(key);

        // fallback to sorted indices
        if(!value_ptr.has_value())
        {
            size_t matching_partition_id = this->_find_matching_partition_for_key(key);
            std::vector<sorted_index_ptr_t> sorted_indices;

            {
                std::lock_guard lk(this->_sorted_index_mutex);
                auto sorted_indices_it = this->_sorted_indices.find(matching_partition_id);

                if(sorted_indices_it == this->_sorted_indices.end())
                    co_return hedge::error("Cannot find matching sorted index", errc::KEY_NOT_FOUND);

                // acquire (temporary) ownership of the sorted_indices
                sorted_indices = sorted_indices_it->second;
            }

            // lookup for the key in every sorted index with same key
            for(auto& sorted_index_ptr : sorted_indices)
            {
                auto maybe_value_ptr = co_await sorted_index_ptr->lookup_async(key, executor);
                if(!maybe_value_ptr.has_value())
                    co_return hedge::error(std::format("An error occurred while reading index at path {}: {}", sorted_index_ptr->path().string(), maybe_value_ptr.error().to_string()));

                if(auto& opt_value_ptr = maybe_value_ptr.value(); opt_value_ptr.has_value())
                {
                    if(!value_ptr.has_value())
                        value_ptr = opt_value_ptr.value();
                    else if(value_ptr && *opt_value_ptr < *value_ptr)
                        value_ptr = opt_value_ptr.value();
                }
            }
        }

        if(!value_ptr.has_value())
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        if(value_ptr.value().is_deleted())
            co_return hedge::error("Key deleted from index", errc::DELETED);

        auto table_finder = [this](size_t id) -> std::shared_ptr<value_table>
        {
            if(this->_current_value_table->id() == id)
                return this->_current_value_table;

            {
                std::lock_guard lk(this->_value_tables_mutex);
                auto it = this->_value_tables.find(id);
                if(it != this->_value_tables.end())
                    return it->second;
            }
            return nullptr;
        };

        auto table_ptr = table_finder(value_ptr->table_id());
        if(table_ptr == nullptr)
            co_return hedge::error("Could not find the matching table " + std::to_string(value_ptr->table_id()));

        co_return {value_ptr.value(), std::move(table_ptr)};
    }

    async::task<expected<database::byte_buffer_t>> database::get_async(key_t key, const std::shared_ptr<async::executor_context>& executor)
    {
        auto maybe_value_ptr_table = co_await this->_find_value_ptr_and_value_table(key, executor);

        if(!maybe_value_ptr_table)
            co_return maybe_value_ptr_table.error();

        auto [value_ptr, table_ptr] = std::move(maybe_value_ptr_table.value());

        auto maybe_file = co_await table_ptr->read_async(value_ptr.offset(), value_ptr.size(), executor);

        if(!maybe_file)
            co_return hedge::error(std::format("An error occurred while reading from table {}: {}", table_ptr->path().string(), maybe_file.error().to_string()));

        // todo implement paranoid check between header and binaries

        co_return std::move(maybe_file.value().binaries);
    }

    size_t database::_find_matching_partition_for_key(const key_t& key) const
    {
        size_t partition_size = (1 << 16) / (1 << this->_config.num_partition_exponent);

        auto matching_partition_id = hedge::find_partition_prefix_for_key(key, partition_size);

        return matching_partition_id;
    }

    async::task<hedge::status> database::remove_async(key_t key, const std::shared_ptr<async::executor_context>& executor)
    {
        auto maybe_value_ptr_table = co_await this->_find_value_ptr_and_value_table(key, executor);

        if(!maybe_value_ptr_table)
            co_return maybe_value_ptr_table.error();

        auto [value_ptr, table_ptr] = std::move(maybe_value_ptr_table.value());

        auto table_delete_result = co_await table_ptr->delete_async(key, value_ptr.offset(), executor);

        if(!table_delete_result)
            co_return hedge::error("An error occurred while deleting an object from table: {}" + table_delete_result.error().to_string());

        this->_mem_index.put(
            key,
            value_ptr_t::apply_delete(value_ptr));

        co_return hedge::ok();
    }

    hedge::status database::_rotate_value_table()
    {
        this->_logger.log("Rotating value table");

        auto new_value_table = value_table::make_new(this->_values_path, ++this->_last_table_id);
        if(!new_value_table)
            return hedge::error("Failed to create new value table: " + new_value_table.error().to_string());

        {
            std::lock_guard lk(this->_value_tables_mutex);
            this->_value_tables[this->_current_value_table->id()] = std::move(this->_current_value_table);
        }

        this->_current_value_table = std::make_shared<value_table>(std::move(new_value_table.value()));

        return hedge::ok();
    }

    hedge::status database::_flush_mem_index()
    {
        this->_logger.log("Flushing mem index to ", this->_indices_path, " with number of items: ", this->_mem_index.size());

        if(this->_mem_index.size() < this->_config.keys_in_mem_before_flush)
            return hedge::error(std::format("Not enough keys in mem_index to flush: {} < {}", this->_mem_index.size(), this->_config.keys_in_mem_before_flush));

        std::vector<mem_index> vec_memtable;
        vec_memtable.emplace_back(std::move(this->_mem_index));

        auto partitioned_sorted_indices = index_ops::flush_mem_index(this->_indices_path, std::move(vec_memtable), this->_config.num_partition_exponent, this->_flush_iteration++);

        if(!partitioned_sorted_indices)
            return hedge::error("An error occurred while flushing the mem index: " + partitioned_sorted_indices.error().to_string());

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

        this->_mem_index = mem_index{};
        this->_mem_index.reserve(this->_config.keys_in_mem_before_flush);

        return hedge::ok();
    }

    hedge::status database::_compaction_job(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor)
    {
        this->_logger.log("Starting compaction job");

        sorted_indices_map_t indices_local_copy;

        {
            std::lock_guard lk(this->_sorted_index_mutex);
            indices_local_copy = this->_sorted_indices;
        }

        // we'll need it for later to know how to update the database's indices
        std::unordered_map<size_t, size_t> initial_vec_sizes;
        for(auto& [prefix, vec] : indices_local_copy)
            initial_vec_sizes.emplace(prefix, vec.size());

        bool stability_reached = false;

        while(!stability_reached)
        {
            stability_reached = true;

            std::vector<hedge::error> errors;
            async::working_group wg;

            wg.set(indices_local_copy.size());

            auto make_compaction_sub_task = [this, &wg, &executor, &errors, this_iteration_id = this->_flush_iteration++](std::vector<sorted_index_ptr_t>& index_vec) -> async::task<void>
            {
                auto last_it = index_vec.begin() + (index_vec.size() - 1);
                auto second_last_it = last_it - 1;

                auto merge_config = hedge::db::index_ops::merge_config{
                    .read_ahead_size = this->_config.compaction_read_ahead_size_bytes,
                    .new_index_id = this_iteration_id,
                    .base_path = this->_indices_path,
                };

                auto maybe_compacted_table = co_await index_ops::two_way_merge_async(
                    merge_config,
                    **second_last_it,
                    **last_it,
                    executor);

                if(!maybe_compacted_table)
                {
                    errors.emplace_back(
                        std::format("An error occurred while compacting {} and {}: {}",
                                    (*last_it)->path().string(),
                                    (*second_last_it)->path().string(),
                                    maybe_compacted_table.error().to_string()));
                }
                else
                {
                    index_vec.erase(last_it);
                    index_vec.erase(second_last_it);

                    index_vec.emplace_back(std::make_shared<sorted_index>(std::move(maybe_compacted_table.value())));
                }

                wg.decr();
            };

            for(auto& [prefix, index_vec] : indices_local_copy)
            {
                if(index_vec.size() <= 1) // nothing to compact
                {
                    wg.decr();
                    continue;
                }

                std::ranges::sort(
                    index_vec,
                    [](const sorted_index_ptr_t& a, const sorted_index_ptr_t& b)
                    {
                        return a->size() > b->size();
                    });

                auto lhs_size = static_cast<double>(index_vec[index_vec.size() - 1]->size());
                auto rhs_size = static_cast<double>(index_vec[index_vec.size() - 2]->size());

                if(!ignore_ratio && lhs_size / rhs_size <= this->_config.compaction_size_ratio)
                {
                    wg.decr();
                    continue;
                }

                stability_reached = false;

                // todo: if only two sorted indices are left to compact, the merge_config.filter_deleted_keys should be true
                executor->submit_io_task(make_compaction_sub_task(index_vec));
            }

            bool done = wg.wait_for(this->_config.compaction_timeout);

            if(!done)
                return hedge::error("compaction timeout.");

            if(!errors.empty())
            {
                for(auto& error : errors)
                    std::cerr << "compaction sub-task error: " + error.to_string() << std::endl;

                return hedge::error("compaction error.");
            }
        }

        // finalize compaction: replace the database's indices
        {
            std::lock_guard lk(this->_sorted_index_mutex);

            for(auto& [prefix, new_sorted_index_vec] : indices_local_copy) // remember that new_sorted_index_vecs holds the compacted sorted indices
            {
                auto& db_sorted_indices_vec = this->_sorted_indices[prefix];

                auto range_start = db_sorted_indices_vec.begin();
                auto range_end = range_start + initial_vec_sizes[prefix];

                std::for_each(range_start, range_end, [](const std::shared_ptr<sorted_index>& sorted_index)
                              { sorted_index->set_delete_on_obj_destruction(true); });

                db_sorted_indices_vec.erase(range_start, range_end); // we cannot clear the entire vector because we could accidentally delete some table that was added in the mean while

                db_sorted_indices_vec.insert(db_sorted_indices_vec.end(),
                                             std::move_iterator(new_sorted_index_vec.begin()),
                                             std::move_iterator(new_sorted_index_vec.end()));

                // because the indices in '[range_start, range_end]' might intersect with 'new_sorted_index_vec'
                std::ranges::for_each(db_sorted_indices_vec, [](const std::shared_ptr<sorted_index>& sorted_index)
                                      { sorted_index->set_delete_on_obj_destruction(false); });
            }
        }

        return hedge::ok();
    }

    std::future<hedge::status> database::compact_sorted_indices(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor)
    {
        auto compaction_promise_ptr = std::make_shared<std::promise<hedge::status>>();
        std::future<hedge::status> compaction_future = compaction_promise_ptr->get_future();

        this->compaction_worker.submit(
            [weak_db = this->weak_from_this(), promise = std::move(compaction_promise_ptr), executor, ignore_ratio]()
            {
                auto db = weak_db.lock();
                if(!db)
                {
                    std::cerr << "Cannot start compaction job. DB not available. returning." << std::endl;
                    return;
                }

                auto status = db->_compaction_job(ignore_ratio, executor);
                if(!status)
                    std::cerr << status.error().to_string() << std::endl;

                promise->set_value(std::move(status));
            });

        return compaction_future;
    }

    [[nodiscard]] double database::load_factor()
    {
        std::lock_guard lk(this->_sorted_index_mutex);
        double total_read_amplification = 0.0;

        for(const auto& [prefix, indices] : this->_sorted_indices)
            total_read_amplification += indices.size();

        return total_read_amplification / this->_sorted_indices.size();
    }

    async::task<hedge::status> database::_garbage_collect_table(std::shared_ptr<value_table> table, size_t id, const std::shared_ptr<async::executor_context>& executor)
    {
        auto maybe_new_table = value_table::make_new(this->_values_path, id, false);

        if(!maybe_new_table)
        {
            co_return hedge::error(std::format(
                "Failed to create new value table: {}",
                maybe_new_table.error().to_string()));
        }

        auto compacted_table = std::move(maybe_new_table.value());

        auto maybe_header_lock = co_await table->get_first_header_async(executor);

        if(!maybe_header_lock)
        {
            co_return hedge::error(std::format(
                "Failed to read next file header from value table (path: {}): {}",
                table->path().string(),
                maybe_header_lock.error().to_string()));
        }

        mem_index new_keys{};

        auto [header, lock] = std::move(maybe_header_lock.value());

        size_t offset{0};
        size_t size{header.file_size + sizeof(file_header)};

        while(true)
        {
            auto maybe_read_result = co_await table->read_file_and_next_header_async(offset, size, executor);

            auto& [file, next] = maybe_read_result.value();

            auto value_ptr = value_ptr_t(
                offset,
                size,
                compacted_table.id());

            if(!file.header.deleted_flag)
            {
                auto reservation = compacted_table.get_write_reservation(file.header.file_size);

                auto write_result = co_await compacted_table.write_async(file.header.key, file.binaries, reservation.value(), executor);

                if(!write_result)
                    co_return hedge::error(std::format("Failed to write file during gc: {}", write_result.error().to_string()));
            }
            else
            {
                value_ptr = value_ptr_t::apply_delete(value_ptr);
            }

            new_keys.put(header.key, value_ptr);

            std::tie(offset, size) = next;

            if(offset == std::numeric_limits<size_t>::max() && size == 0) // EOF
                break;
        }

        {
            std::lock_guard lk(this->_value_tables_mutex);
            this->_value_tables.erase(table->id());
            this->_value_tables.emplace(compacted_table.id(), std::make_shared<value_table>(std::move(compacted_table)));
        }

        if(new_keys.size() == 0)
            throw std::runtime_error("new compacted table should not be empty");

        std::vector<mem_index> vec_memtable;
        vec_memtable.emplace_back(std::move(new_keys));

        auto partitioned_sorted_indices = index_ops::flush_mem_index(this->_indices_path, std::move(vec_memtable), this->_config.num_partition_exponent, this->_flush_iteration++);

        if(!partitioned_sorted_indices)
            co_return hedge::error("An error occurred while flushing the mem index: " + partitioned_sorted_indices.error().to_string());

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

        {
            std::lock_guard lk(this->_value_tables_mutex);
            this->_value_tables[compacted_table.id()] = std::make_shared<value_table>(std::move(compacted_table));
        }

        table->set_delete_on_obj_destruction(true);

        co_return hedge::ok();
    }

    std::future<hedge::status> database::garbage_collect_tables(const std::shared_ptr<async::executor_context>& executor)
    {

        std::vector<std::weak_ptr<value_table>> tables;
        {
            std::lock_guard lk(this->_value_tables_mutex);

            tables.reserve(this->_value_tables.size());

            for(const auto& [id, table] : this->_value_tables)
                tables.emplace_back(std::weak_ptr<value_table>(table));
        }

        std::shared_ptr<std::promise<hedge::status>> gc_promise = std::make_shared<std::promise<hedge::status>>();
        auto future = gc_promise->get_future();

        this->gc_worker.submit(
            [this, executor, tables = std::move(tables), gc_promise = std::move(gc_promise)]()
            {
                for(const auto& wk_table : tables)
                {
                    auto table_ptr = wk_table.lock();

                    if(!table_ptr)
                    {
                        gc_promise->set_value(hedge::ok());
                        return;
                    }

                    auto status = executor->sync_submit(
                        [this, table_ptr, executor]() -> async::task<hedge::status>
                        {
                            this->_logger.log("Triggering gc for: ", table_ptr->path());
                             co_return co_await this->_garbage_collect_table(table_ptr, table_ptr->id(), executor); }());

                    if(!status)
                    {
                        gc_promise->set_value(status);
                        return;
                    }

                    gc_promise->set_value(status);
                }
            });

        return future;
    }

    hedge::status database::flush()
    {
        if(auto status = this->_flush_mem_index(); !status)
            return hedge::error("An error occurred while flushing the mem_index: " + status.error().to_string());

        return hedge::ok();
    }

} // namespace hedge::db