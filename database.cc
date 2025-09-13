#include <algorithm>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>

#include "common.h"
#include "database.h"
#include "error.hpp"
#include "index.h"
#include "value_table.h"
#include "working_group.h"

namespace hedgehog::db
{
    expected<std::shared_ptr<database>> database::make_new(const std::filesystem::path& base_path, const db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_indices_path = base_path / "indices";
        db->_values_path = base_path / "values";
        db->_config = config;

        if(config.num_partition_exponent > db_config::MAX_PARTITION_EXPONENT)
            return hedgehog::error("num_partition_exponent must be <= " + std::to_string(db_config::MAX_PARTITION_EXPONENT));

        if(config.keys_in_mem_before_flush < db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH)
            return hedgehog::error("keys_in_mem_before_flush must be >= " + std::to_string(db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH));

        if(std::filesystem::exists(db->_base_path))
            return hedgehog::error("Database path already exists: " + db->_base_path.string());

        std::filesystem::create_directories(db->_base_path);
        std::filesystem::create_directories(db->_indices_path);
        std::filesystem::create_directories(db->_values_path);

        // todo write manifest file
        //

        // init inner data structures
        db->_mem_index.reserve(config.keys_in_mem_before_flush);

        auto new_value_table = value_table::make_new(db->_values_path, 0);
        if(!new_value_table)
            return hedgehog::error("Failed to create value table: " + new_value_table.error().to_string());

        db->_current_value_table = std::make_shared<value_table>(std::move(new_value_table.value()));
        return db;
    }

    expected<std::shared_ptr<database>> database::load(const std::filesystem::path& /* base_path */)
    {
        return hedgehog::error("Load database from path not implemented yet");
    }

    async::task<hedgehog::status> database::put_async(key_t key, const byte_buffer_t& value, const std::shared_ptr<async::executor_context>& executor)
    {
        // lazy flush the memtable if no space left in the memtable
        if(this->_mem_index.size() >= this->_config.keys_in_mem_before_flush)
        {
            if(auto status = this->_flush_mem_index(); !status)
                co_return hedgehog::error("An error occurred while flushing the mem_index: " + status.error().to_string());

            // trigger compactation
            if(this->_config.auto_compactation)
                this->compact_sorted_indices(false, executor);
        }

        // first it tries writing the data to the value table
        auto value_table = this->_current_value_table; // acquire ownership of the current value table
        auto reservation = value_table->get_write_reservation(value.size());

        // make a new value table if the space limit is reached
        if(!reservation && reservation.error().code() == hedgehog::errc::VALUE_TABLE_NOT_ENOUGH_SPACE)
        {
            if(auto status = this->_rotate_value_table(); !status)
                co_return hedgehog::error("Failed to rotate value table: " + status.error().to_string());

            value_table = this->_current_value_table; // acquire ownership of the new value table
            reservation = value_table->get_write_reservation(value.size());
        }

        if(!reservation)
            co_return hedgehog::error("Failed to reserve space in value table: " + reservation.error().to_string());

        // execute the actual writes
        auto write_response = co_await value_table->write_async(key, value, reservation.value(), executor);

        if(!write_response)
            co_return hedgehog::error("Failed to write value to value table: " + write_response.error().to_string());

        this->_mem_index.add(key, write_response.value());

        co_return hedgehog::ok();
    }

    async::task<hedgehog::status> database::put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor)
    {
        co_return co_await this->put_async(key, value, executor);
    }

    async::task<expected<database::byte_buffer_t>> database::get_async(key_t key, const std::shared_ptr<async::executor_context>& executor)
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
                    co_return hedgehog::error("Cannot find matching sorted index", errc::KEY_NOT_FOUND);

                // acquire (temporary) ownership of the sorted_indices
                sorted_indices = sorted_indices_it->second;
            }

            // lookup for the key in every sorted index with same key
            for(auto& sorted_index_ptr : sorted_indices)
            {
                auto maybe_value_ptr = co_await sorted_index_ptr->lookup_async(key, executor);
                if(!maybe_value_ptr.has_value())
                    co_return hedgehog::error(std::format("An error occurred while reading index at path {}: {}", sorted_index_ptr->get_path().string(), maybe_value_ptr.error().to_string()));

                if(auto& opt_value_ptr = maybe_value_ptr.value(); opt_value_ptr.has_value())
                {
                    value_ptr = opt_value_ptr.value();
                    break;
                }
            }
        }

        if(!value_ptr.has_value())
            co_return hedgehog::error("Key not found", errc::KEY_NOT_FOUND);

        auto table_finder = [this](size_t id) -> std::shared_ptr<value_table>
        {
            if(this->_current_value_table->id() == id)
                return this->_current_value_table;

            auto it = this->_value_tables.find(id);
            if(it != this->_value_tables.end())
                return it->second;

            return nullptr;
        };

        auto table_ptr = table_finder(value_ptr->table_id);
        if(table_ptr == nullptr)
            co_return hedgehog::error("Could not find the matching table " + std::to_string(value_ptr->table_id));

        auto maybe_file = co_await table_ptr->read_async(value_ptr->offset, value_ptr->size, executor);
        if(!maybe_file)
            co_return hedgehog::error(std::format("An error occurred while reading from table {}: {}", table_ptr->fd().path().string(), maybe_file.error().to_string()));

        // todo implement paranoid check between header and binaries

        co_return std::move(maybe_file.value().binaries);
    }

    size_t database::_find_matching_partition_for_key(const key_t& key) const
    {
        size_t partition_size = (1 << 16) / (1 << this->_config.num_partition_exponent);

        auto matching_partition_id = hedgehog::find_partition_prefix_for_key(key, partition_size);

        return matching_partition_id;
    }

    // NOLINTNEXTLINE
    async::task<hedgehog::status> database::remove_async(key_t /* key */, const std::shared_ptr<async::executor_context>& /* executor */)
    {
        co_return hedgehog::error("Remove op implemented");
    }

    hedgehog::status database::_rotate_value_table()
    {
        this->_logger.log("Rotating value table");

        auto new_value_table = value_table::make_new(this->_values_path, this->_current_value_table->id() + 1);
        if(!new_value_table)
            return hedgehog::error("Failed to create new value table: " + new_value_table.error().to_string());

        this->_value_tables[this->_current_value_table->id()] = std::move(this->_current_value_table);
        this->_current_value_table = std::make_shared<value_table>(std::move(new_value_table.value()));

        return hedgehog::ok();
    }

    hedgehog::status database::_flush_mem_index()
    {
        this->_logger.log("Flushing mem index to ", this->_indices_path, " with number of items: ", this->_mem_index.size());

        if(this->_mem_index.size() < this->_config.keys_in_mem_before_flush)
            return hedgehog::error(std::format("Not enough keys in mem_index to flush: {} < {}", this->_mem_index.size(), this->_config.keys_in_mem_before_flush));

        std::vector<mem_index> vec_memtable;
        vec_memtable.emplace_back(std::move(this->_mem_index));

        auto partitioned_sorted_indices = index_ops::merge_and_flush(this->_indices_path, std::move(vec_memtable), this->_config.num_partition_exponent);

        if(!partitioned_sorted_indices)
            return hedgehog::error("An error occurred while flushing the mem index: " + partitioned_sorted_indices.error().to_string());

        {
            std::lock_guard lk(this->_sorted_index_mutex);

            for(auto& new_sorted_index : partitioned_sorted_indices.value())
            {
                new_sorted_index.clear_index();
                auto prefix = new_sorted_index.upper_bound();

                auto sorted_index_ptr = std::make_shared<sorted_index>(std::move(new_sorted_index));
                this->_sorted_indices[prefix].emplace_back(std::move(sorted_index_ptr)); // todo: there might be a race condition between this and the compactation job
            }
        }

        this->_mem_index = mem_index{};
        this->_mem_index.reserve(this->_config.keys_in_mem_before_flush);

        return hedgehog::ok();
    }

    hedgehog::status database::_compactation_job(const std::shared_ptr<async::executor_context>& executor)
    {
        this->_logger.log("Starting compaction job");

        sorted_indices_map_t indices;

        // we'll need it for later to know how to update the database's indices
        std::unordered_map<size_t, size_t> initial_vec_sizes;
        for(auto& [prefix, vec] : indices)
            initial_vec_sizes.emplace(prefix, indices.size());

        {
            std::lock_guard lk(this->_sorted_index_mutex);
            indices = this->_sorted_indices;
        }

        bool stability_reached = false;

        while(!stability_reached)
        {
            stability_reached = true;

            std::vector<hedgehog::error> errors;
            async::working_group wg;

            wg.set(indices.size());

            auto make_compactation_sub_task = [this, &wg, &executor, &errors](std::vector<sorted_index_ptr_t>& index_vec) -> async::task<void>
            {
                auto last_it = index_vec.begin() + (index_vec.size() - 1);
                auto second_last_it = last_it - 1;

                auto maybe_compacted_table = co_await index_ops::two_way_merge_async(
                    this->_config.compactation_read_ahead_size_bytes,
                    **second_last_it,
                    **last_it,
                    executor);

                if(!maybe_compacted_table)
                {
                    errors.emplace_back(
                        std::format("An error occurred while compacting {} and {}: {}",
                                    (*last_it)->get_path().string(),
                                    (*second_last_it)->get_path().string(),
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

            for(auto& [prefix, index_vec] : indices)
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
                        return a->size() >= b->size();
                    });

                auto lhs_size = static_cast<double>(index_vec[index_vec.size() - 1]->size());
                auto rhs_size = static_cast<double>(index_vec[index_vec.size() - 2]->size());

                if(lhs_size / rhs_size <= this->_config.compactation_size_ratio)
                {
                    wg.decr();
                    continue;
                }

                stability_reached = false;
                executor->submit_io_task(make_compactation_sub_task(index_vec));
            }

            bool done = wg.wait_for(this->_config.compacation_timeout);

            if(!done)
                return hedgehog::error("Compactation timeout.");

            if(!errors.empty())
            {
                for(auto& error : errors)
                    std::cerr << "Compactation sub-task error: " + error.to_string();

                return hedgehog::error("Compactation error.");
            }
        }

        // finalize compactation: replace the database's indices
        {
            std::lock_guard lk(this->_sorted_index_mutex);

            for(auto& [prefix, new_sorted_index_vec] : indices) // remember that new_sorted_index_vecs hold the compacted sorted indices
            {
                auto& db_sorted_indices_vec = this->_sorted_indices[prefix];

                auto range_start = db_sorted_indices_vec.begin();
                auto range_end = range_start + initial_vec_sizes[prefix];

                db_sorted_indices_vec.erase(range_start, range_end); // we cannot clear the entire vector because we could accidentally delete some table that was added in the mean while

                db_sorted_indices_vec.insert(db_sorted_indices_vec.end(),
                                             std::move_iterator(new_sorted_index_vec.begin()),
                                             std::move_iterator(new_sorted_index_vec.end()));
            }
        }

        return hedgehog::ok();
    }

    hedgehog::status database::compact_sorted_indices(bool wait_sync, const std::shared_ptr<async::executor_context>& executor)
    {
        auto compactation_promise_ptr = std::make_shared<std::promise<hedgehog::status>>();
        std::future<hedgehog::status> compactation_future = compactation_promise_ptr->get_future();

        this->async_worker.submit(
            [weak_db = this->weak_from_this(), promise = std::move(compactation_promise_ptr), executor]()
            {
                auto db = weak_db.lock();
                if(!db)
                {
                    std::cerr << "Cannot start compactation job. DB not available. returning." << std::endl;
                    return;
                }

                auto status = db->_compactation_job(executor);
                if(!status)
                    std::cerr << status.error().to_string() << std::endl;

                promise->set_value(std::move(status));
            });

        if(!wait_sync)
            return hedgehog::ok();

        return compactation_future.get();
    }

} // namespace hedgehog::db