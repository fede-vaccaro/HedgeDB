#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include <error.hpp>
#include <variant>

#include "cache.h"
#include "database.h"
#include "db/memtable.h"
#include "io/io_executor.h"
#include "io/static_pool.h"
#include "sst.h"
#include "types.h"
#include "utils.h"
#include "value_table.h"

namespace hedge::db
{
    hedge::status database::_validate_config(const db_config& config)
    {
        if(config.num_partition_exponent > db_config::MAX_PARTITION_EXPONENT)
            return hedge::error("num_partition_exponent must be <= " + std::to_string(db_config::MAX_PARTITION_EXPONENT));

        if(config.keys_in_mem_before_flush < db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH)
            return hedge::error("keys_in_mem_before_flush must be >= " + std::to_string(db_config::MIN_KEYS_IN_MEM_BEFORE_FLUSH));

        return hedge::ok();
    }

    void database::_init_memtable(database& db, const db_config& config)
    {
        db._memtable.emplace(
            memtable_config{
                .max_inserts_cap = config.keys_in_mem_before_flush,
                .memory_budget_cap = 128 * 1024 * 1024,
                .auto_compaction = config.auto_compaction,
                .use_odirect = config.use_odirect_for_indices,
                .num_writer_threads = io::static_pool::instance().num_threads(),
                // .flush_io_workers = config.flush_io_workers,
                .use_wal = !config.disable_wal,
            },
            config.num_partition_exponent,
            db._indices_path,
            &db._sst_manager->flush_iteration(),
            std::make_shared<io::io_executor>(config.flush_io_workers, 32, "flusher"),
            [&sst_mgr = *db._sst_manager](std::vector<sst> indices) -> tmc::task<void>
            { return sst_mgr.push_new_indices(std::move(indices)); },
            [&sst_mgr = *db._sst_manager]()
            { sst_mgr.trigger_compaction(false); },
            db._page_cache,
            &db._sst_manager->compaction_backpressure());
    }

    expected<std::shared_ptr<database>> database::make_new(const std::filesystem::path& base_path, const db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_indices_path = base_path / "indices";
        db->_values_path = base_path / "values";
        db->_config = config;

        if(auto status = _validate_config(config); !status)
            return status.error();

        if(std::filesystem::exists(db->_base_path))
            return hedge::error("Database path already exists: " + db->_base_path.string());

        // Create necessary directories
        std::filesystem::create_directories(db->_base_path);
        std::filesystem::create_directories(db->_indices_path);
        std::filesystem::create_directories(db->_values_path);

        // Init clock cache
        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1) // Minimum 1 MB page cache
            db->_page_cache = std::make_shared<sharded_page_cache>(config.index_page_clock_cache_size_bytes, io::static_pool::instance().num_threads() * 4);

        // Init sst_manager
        db->_sst_manager = std::make_unique<sst_manager>(
            sst_manager::config{
                .num_partition_exponent = config.num_partition_exponent,
                .max_num_levels = config.max_num_levels,
                .min_merge_width = config.min_merge_width,
                .max_merge_width = config.max_merge_width,
                .bucket_ratio = config.bucket_ratio,
                .compaction_read_ahead_size_bytes = config.compaction_read_ahead_size_bytes,
                ._compaction_io_workers = config.compaction_io_workers,
                .use_odirect_for_indices = config.use_odirect_for_indices,
                .indices_path = db->_indices_path,
            },
            std::make_shared<io::io_executor>(config.compaction_io_workers, 32, "compactor"),
            db->_page_cache);

        // Setup memtable
        _init_memtable(*db, config);

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
        db->_write_buffers.resize(io::static_pool::instance().num_threads());

        for(auto& write_buffer_ptr : db->_write_buffers)
        {
            write_buffer_ptr = std::make_unique<thread_write_buffer>(WRITE_BUFFER_DEFAULT_SIZE);
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
        db->_config = config;

        if(auto status = _validate_config(config); !status)
            return status.error();

        if(!std::filesystem::exists(db->_base_path))
            return hedge::error("Database path does not exist: " + db->_base_path.string());

        // Init page cache
        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1)
            db->_page_cache = std::make_shared<sharded_page_cache>(config.index_page_clock_cache_size_bytes, io::static_pool::instance().num_threads() * 4);

        // Load sst_manager from indices directory
        auto maybe_sst_mgr = sst_manager::load(
            sst_manager::config{
                .num_partition_exponent = config.num_partition_exponent,
                .max_num_levels = config.max_num_levels,
                .min_merge_width = config.min_merge_width,
                .max_merge_width = config.max_merge_width,
                .bucket_ratio = config.bucket_ratio,
                .compaction_read_ahead_size_bytes = config.compaction_read_ahead_size_bytes,
                ._compaction_io_workers = config.compaction_io_workers,
                .use_odirect_for_indices = config.use_odirect_for_indices,
                .indices_path = db->_indices_path,
            },
            std::make_shared<io::io_executor>(config.compaction_io_workers, 32),
            db->_page_cache);

        if(!maybe_sst_mgr)
            return maybe_sst_mgr.error();

        db->_sst_manager = std::move(maybe_sst_mgr.value());

        // Init empty memtable (needed for the read path; starts with no entries)
        _init_memtable(*db, config);

        // Replay WAL files from any prior crash
        if(!config.disable_wal)
        {
            auto wal_status = db->_memtable->replay_wal();
            if(!wal_status)
                return hedge::error("WAL replay failed: " + wal_status.error().to_string());
        }

        // Load value tables from values directory
        db->_value_tables.reserve(4096);

        if(!std::filesystem::exists(db->_values_path))
            return hedge::error("Values path does not exist: " + db->_values_path.string());

        uint32_t max_id = 0;

        for(const auto& entry : std::filesystem::directory_iterator(db->_values_path))
        {
            if(!entry.is_regular_file())
                continue;
            if(entry.path().extension() != value_table::TABLE_FILE_EXTENSION)
                continue;

            auto maybe_vt = value_table::load(entry.path(), fs::file::open_mode::read_only, false);
            if(!maybe_vt)
                return hedge::error("Failed to load value table: " + maybe_vt.error().to_string());

            auto vt = std::move(maybe_vt.value());
            uint32_t id = vt->id();
            max_id = std::max(max_id, id);
            db->_value_tables[id] = std::move(vt);
        }

        if(db->_value_tables.empty())
            return hedge::error("No value tables found in: " + db->_values_path.string());

        // Promote the highest-ID table to _current_value_table
        auto it = db->_value_tables.find(max_id);
        db->_current_value_table.store(it->second);
        db->_value_tables.erase(it);
        db->_last_table_id.store(max_id, std::memory_order::relaxed);

        return db;
    }

    tmc::task<hedge::status> database::put_async(const key_t& key, const byte_buffer_t& value)
    {
        // prof::counter_guard guard(prof::get<"put_async">());

        if(value.size() < 512) [[likely]]
        {
            co_return co_await this->_memtable->put_async(key, value, hedge::value_type::IN_PLACE_VALUE);
            // co_return this->_memtable.put_sync(key, value, value_type::IN_PLACE_VALUE);
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
            auto buffer_flush_status = co_await write_buffer_ref->flush();
            if(!buffer_flush_status)
                co_return buffer_flush_status;

            _allocate_space_for_write_buffer(*write_buffer_ref);
            maybe_write = write_buffer_ref->write(key, value);
        }

        // --- Update Memtable ---
        co_return co_await this->_memtable->put_async(key, maybe_write.value(), hedge::value_type::VALUE_PTR);
    }

    tmc::task<hedge::status> database::put_batch_async(std::span<const std::pair<key_t, byte_buffer_t>> entries)
    {
        static constexpr size_t MAX_BATCH_SIZE = 128;

        if(entries.size() > MAX_BATCH_SIZE)
            co_return hedge::error("put_batch_async: batch size exceeds maximum of 128 entries");

        for(const auto& [key, value] : entries)
        {
            if(value.size() >= 512)
                co_return hedge::error("put_batch_async only supports values < 512 bytes");
        }

        co_return co_await this->_memtable->put_batch_async(entries, hedge::value_type::IN_PLACE_VALUE);
    }

    tmc::task<expected<value_t>> database::_find_value(const key_t& key)
    {
        // prof::counter_guard guard(prof::get<"find_value_in_sst">());

        // Step 1: Check the memtable first (contains the most recent data).
        std::optional<value_t> value_opt;

        value_opt = this->_memtable->get(key);

        if(value_opt.has_value())
            co_return std::move(value_opt.value());

        // Step 2: If not found in memtable, search sorted indices via sst_manager.
        size_t matching_partition_id = this->_find_matching_partition_for_key(key);
        co_return co_await this->_sst_manager->lookup_async(key, matching_partition_id);
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

    tmc::task<expected<database::byte_buffer_t>> database::get_async(const key_t& key)
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

    tmc::task<hedge::status> database::remove_async(const key_t& /* key */)
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

        tmc::post(io::static_pool::instance(), [](database* database, size_t next_id) -> tmc::task<void>
                  {
                auto t0 = std::chrono::high_resolution_clock::now();
                auto new_value_table = value_table::make_new(database->_values_path, next_id);
                if(!new_value_table)
                {
                    database->_logger.log("Failed to create new value table: " + new_value_table.error().to_string());
                    co_return;
                }

                database->_pipelined_value_table.store(new_value_table.value());
                database->_pipelined_value_table.notify_all();

                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                database->_logger.log("Prepared new value table in ", (double)duration.count() / 1000.0, " ms"); }(this, next_id));

        auto t1 = std::chrono::high_resolution_clock::now();
        [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

        // this->_logger.log("Value table rotated in ", (double)duration.count(), " us, pushing job required ", (double)submit_job_duration.count());

        return this->_current_value_table.load();
    }

    void database::trigger_compaction(bool compact_all)
    {
        this->_sst_manager->trigger_compaction(compact_all);
    }

    void database::wait_for_compactions_to_finish()
    {
        this->_memtable->wait_for_flush().wait();
        this->_sst_manager->wait_for_compactions_to_finish();
    }

    [[nodiscard]] double database::read_amplification_factor()
    {
        return this->_sst_manager->read_amplification_factor();
    }

    void database::print_tree_structure() const
    {
        this->_sst_manager->print_tree_structure();
    }

    hedge::status database::flush()
    {
        return hedge::error("flush not implemented");
    }

} // namespace hedge::db
