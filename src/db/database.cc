#include <cmath>
#include <future>
#include <memory>
#include <string>
#include <utility>

#include <error.hpp>
#include <variant>

#include "cache.h"
#include "db/database.h"
#include "db/memtable.h"
#include "io/io_executor.h"
#include "io/static_pool.h"
#include "size_literals.h"
#include "sst.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{
    hedge::status database::_validate_config(const db_config& config)
    {
        if(config.num_partition_exponent > db_config::MAX_PARTITION_EXPONENT)
            return hedge::error("num_partition_exponent must be <= " + std::to_string(db_config::MAX_PARTITION_EXPONENT));

        // TODO: add more config validation

        return hedge::ok();
    }

    void database::_init_memtable(database& db, const db_config& config)
    {
        db._memtable.emplace(
            memtable_config{
                .memory_budget_cap = config.memtable_budget_bytes,
                .auto_compaction = config.auto_compaction,
                .use_odirect = config.use_direct_io,
                .num_writer_threads = config.num_writer_threads.value_or(io::static_pool::instance()->num_threads()),
                .use_wal = !config.disable_wal,
                .max_pending_flushes = config.max_pending_flushes,
                .acquire_flush_stats = config.acquire_flush_stats,
            },
            config.num_partition_exponent,
            db._partitions_path,
            &db._sst_manager->flush_iteration(),
            db._bg_pool,
            [&sst_mgr = *db._sst_manager](std::vector<sst> sst_batch, std::optional<compaction_stats> stats_opt) -> tmc::task<void>
            { return sst_mgr.push_new_ssts_to_l0(std::move(sst_batch), std::move(stats_opt)); },
            [&sst_mgr = *db._sst_manager]()
            { sst_mgr.schedule_compaction(false); },
            db._page_cache,
            &db._sst_manager->compaction_backpressure());
    }

    expected<std::shared_ptr<database>> database::make_new(const std::filesystem::path& base_path, const db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_partitions_path = base_path / "partitions";
        db->_config = config;
        db->_bg_pool = std::make_shared<io::io_executor>(
            io::executor_config{
                .name = "bg-thread",
                .queue_depth = 32,
                .type = io::executor_type::BACKGROUND,
                .n_threads = config.num_background_workers,
                .auto_detect = true,
            });

        if(auto status = _validate_config(config); !status)
            return status.error();

        if(std::filesystem::exists(db->_base_path))
            return hedge::error("Database path already exists: " + db->_base_path.string());

        // Create necessary directories
        std::filesystem::create_directories(db->_base_path);
        std::filesystem::create_directories(db->_partitions_path);
        fs::fsync_dir(db->_base_path.parent_path());
        fs::fsync_dir(db->_base_path);

        // Init clock cache
        if(config.index_page_clock_cache_size_bytes > 1024 * 1024 * 1) // Minimum 1 MB page cache
            db->_page_cache = std::make_shared<sharded_page_cache>(config.index_page_clock_cache_size_bytes, io::static_pool::instance()->num_threads() * 4);

        // Init sst_manager
        db->_sst_manager = std::make_unique<sst_manager>(
            sst_manager::config{
                .num_partition_exponent = config.num_partition_exponent,
                .max_num_levels = config.max_num_levels,
                .min_merge_width = config.min_merge_width,
                .max_merge_width = config.max_merge_width,
                .bucket_ratio = config.bucket_ratio,
                .compaction_read_ahead_size_bytes = config.compaction_read_ahead_size_bytes,
                .use_odirect_for_ssts = config.use_direct_io,
                .ssts_in_l0_block_write_threshold = config.ssts_in_l0_block_write_threshold,
                .partitions_path = db->_partitions_path,
                .acquire_compaction_stats = config.acquire_compaction_stats,
            },
            db->_bg_pool,
            db->_page_cache);

        // Setup memtable
        _init_memtable(*db, config);

        db->_sst_manager->launch_compaction_worker();

        return db;
    }

    expected<std::shared_ptr<database>> database::load(const std::filesystem::path& base_path, const db::db_config& config)
    {
        auto db = std::shared_ptr<database>(new database());

        db->_base_path = base_path;
        db->_partitions_path = base_path / "partitions";
        db->_config = config;
        db->_bg_pool = std::make_shared<io::io_executor>(
            io::executor_config{
                .name = "bg-thread",
                .queue_depth = 32,
                .type = io::executor_type::BACKGROUND,
                .n_threads = config.num_background_workers,
                .auto_detect = true,
            });

        if(auto status = _validate_config(config); !status)
            return status.error();

        if(!std::filesystem::exists(db->_base_path))
            return hedge::error("Database path does not exist: " + db->_base_path.string());

        // Init page cache
        if(config.index_page_clock_cache_size_bytes > 1 * GiB)
            db->_page_cache = std::make_shared<sharded_page_cache>(config.index_page_clock_cache_size_bytes, io::static_pool::instance()->num_threads() * 4);

        // Load sst_manager from "partitions" directory
        auto maybe_sst_mgr = sst_manager::load(
            sst_manager::config{
                .num_partition_exponent = config.num_partition_exponent,
                .max_num_levels = config.max_num_levels,
                .min_merge_width = config.min_merge_width,
                .max_merge_width = config.max_merge_width,
                .bucket_ratio = config.bucket_ratio,
                .compaction_read_ahead_size_bytes = config.compaction_read_ahead_size_bytes,
                .use_odirect_for_ssts = config.use_direct_io,
                .ssts_in_l0_block_write_threshold = config.ssts_in_l0_block_write_threshold,
                .partitions_path = db->_partitions_path,
            },
            db->_bg_pool,
            db->_page_cache);

        if(!maybe_sst_mgr)
            return maybe_sst_mgr.error();

        db->_sst_manager = std::move(maybe_sst_mgr.value());

        // Init empty memtable (needed for the read path; starts with no entries)
        _init_memtable(*db, config);

        // Replay WAL files from any prior crash, skipping entries already persisted in SSTs
        if(!config.disable_wal)
        {
            const uint64_t flushed_threshold = db->_sst_manager->max_seq_nr();
            auto wal_status = db->_memtable->replay_wal(flushed_threshold);
            if(!wal_status)
                return hedge::error("WAL replay failed: " + wal_status.error().to_string());
        }

        db->_sst_manager->launch_compaction_worker();

        return db;
    }

    hedge::status database::put(const key_t& key, const std::span<const std::byte>& value)
    {
        if (key.size() < hedge::MIN_KEY_LEN || key.size() > hedge::MAX_KEY_LEN) {
            return hedge::status{hedge::error{"Key size must be between 1 and 256 bytes", hedge::errc::GENERIC_ERROR}};
        }
        if (value.size() > hedge::MAX_VALUE_LEN) {
            return hedge::status{hedge::error{"Value size must not exceed 3072 bytes", hedge::errc::GENERIC_ERROR}};
        }
        return this->_memtable->put(key, value, hedge::value_type::IN_PLACE_VALUE);
    }

    tmc::task<expected<value_t>> database::_find_value(const key_t& key)
    {
        // prof::counter_guard guard(prof::get<"find_value_in_sst">());

        // Step 1: Check the memtable first (contains the most recent data).
        std::optional<value_t> value_opt;

        value_opt = this->_memtable->get(key);

        if(value_opt.has_value())
            co_return std::move(value_opt.value());

        // Step 2: If not found in memtable, search ssts via sst_manager.
        size_t matching_partition_id = this->_find_matching_partition_for_key(key);
        co_return co_await this->_sst_manager->lookup_async(key, matching_partition_id);
    }

    tmc::task<expected<database::byte_buffer_t>> database::get_async(const key_t& key)
    {
        auto maybe_value = co_await this->_find_value(key);

        if(!maybe_value)
            co_return maybe_value.error();

        auto value = std::move(maybe_value.value());

        if(std::holds_alternative<std::vector<std::byte>>(value))
            co_return std::move(std::get<std::vector<std::byte>>(value));

        if(std::holds_alternative<tombstone_t>(value))
            co_return hedge::error("deleted key", errc::DELETED);

        if(!std::holds_alternative<value_ptr_t>(value))
            co_return hedge::error("Invalid value type found for key");

        co_return hedge::error("kv separation not implemented");
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

    hedge::expected<range_iterator> database::scan(std::optional<key_t> lower, std::optional<key_t> upper, size_t read_ahead_size)
    {
        const key_t& bound_key = lower ? *lower : *upper;
        size_t partition_id = this->_find_matching_partition_for_key(bound_key);

        memtable::snapshot snap;
        if(this->_memtable.has_value())
            snap = this->_memtable->acquire_snapshot();

        auto maybe_partition = this->_sst_manager->acquire_partition_snapshot(partition_id);
        if(!maybe_partition)
            return hedge::error(maybe_partition.error());

        return range_iterator::make_new(
            std::move(snap), &maybe_partition.value(), std::move(lower), std::move(upper), read_ahead_size);
    }

    hedge::status database::remove(const key_t& key)
    {
        return this->_memtable->put(key, {}, hedge::value_type::TOMBSTONE);
    }

    void database::trigger_compaction(bool compact_all)
    {
        this->_sst_manager->schedule_compaction(compact_all);
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

    void database::print_compaction_stats() const
    {
        this->_sst_manager->print_compaction_stats();
    }

    hedge::status database::flush()
    {
        return hedge::error("flush not implemented");
    }

} // namespace hedge::db
