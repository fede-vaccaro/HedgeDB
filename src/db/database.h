#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>

#include <error.hpp>
#include <logger.h>
#include <shared_mutex>
#include <sys/types.h> // POSIX types, consider if needed or include specific headers like <fcntl.h> if used

#include "cache.h"
#include "io/io_executor.h"
#include "memtable.h"
#include "sst_manager.h"
#include "tmc/task.hpp"
#include "types.h"

namespace hedge::db
{
    /**
     * @brief Configuration settings for the database instance.
     * Defines thresholds, ratios, and behaviors related to flushing, compaction, and I/O.
     */
    struct db_config
    {
        // TODO: Add validation/sanitization for every config parameter during database creation/loading.

        /// Maximum allowed exponent for partitioning (2^16 = 65536 partitions max).
        static constexpr size_t MAX_PARTITION_EXPONENT = 16;
        /// Minimum number of keys required in the memtable before a flush is allowed.
        static constexpr size_t MIN_KEYS_IN_MEM_BEFORE_FLUSH = 1000;
        /// Memory budget in bytes for the memtable before a flush is triggered.
        size_t memtable_budget_bytes = 64 * MiB;
        /// Exponent determining the number of partitions (2^num_partition_exponent). Affects index file organization.
        size_t num_partition_exponent = 10;
        /// Ratio (rhs_size / lhs_size) threshold triggering compaction during a two-way merge. rhs is the smaller index.
        /// If the smaller index is less than this fraction of the larger one, the merge occurs.
        /// Higher bucket_ratio means less frequent compactions (lower write amplification) but higher read amplification (more data read during compaction).
        double bucket_ratio = 1.5;
        /// Amount of data read ahead from each sorted_index file during compaction merges.
        size_t compaction_read_ahead_size_bytes = 4 * MiB;
        /// Maximum time to wait for a compaction job (currently for sub-tasks within the job) before timing out.
        std::chrono::milliseconds compaction_timeout{120000};
        /// If true, compaction is automatically triggered when the memtable is flushed and merge conditions are met.
        bool auto_compaction = true;
        /// Maximum number of concurrent compaction tasks allowed. Will block execution of insertions if exceeded.
        size_t max_pending_compactions = 16;
        /// If true, use O_DIRECT flag for sorted_index file I/O to bypass
        bool use_odirect_for_indices = true;
        /// Use 0 no cache is desired; the cache size in bytes otherwise
        size_t index_page_clock_cache_size_bytes = 3UL * GiB;
        /// Experimental; it's like a giant memtable; do not use
        size_t index_point_cache_size_bytes = 0;
        /// Number of background workers to be used for compaction
        size_t compaction_io_workers = 4;
        /// Number of io_uring executor threads for parallel memtable flush
        size_t flush_io_workers = 4;
        /// Maximum number of concurrent memtable flushes allowed before backpressure
        size_t max_pending_flushes = 8;

        size_t max_num_levels = 40;

        size_t min_merge_width = 4;

        size_t max_merge_width = 16;

        bool disable_wal = false;
    };

    /**
     * @brief Main class representing the HedgeDB key-value store instance.
     * Manages the memtable, on-disk sorted indices, value tables, background workers, and configuration.
     * Provides asynchronous methods for Get, Put, and Remove operations.
     * Inherits enable_shared_from_this to allow safe creation of shared_ptrs within async tasks.
     */
    class database : public std::enable_shared_from_this<database>
    {
        // --- Immutable State (after initialization) ---
        std::filesystem::path _base_path;    ///< Root directory for the database.
        std::filesystem::path _indices_path; ///< Subdirectory storing sorted_index files.
        std::filesystem::path _values_path;  ///< Subdirectory storing value_table files.

        // --- Configuration ---
        db_config _config; ///< Database configuration settings.

        // --- Value tables ---
        alignas(64) mutable std::shared_mutex _value_tables_mutex{}; ///< Protects access to the `_value_tables` map.
        std::atomic_size_t _last_table_id{0};                        ///< Atomic ID counter for the next value_table file to be created.

        // Memtable & flushes
        std::optional<memtable> _memtable;

        // --- SST management and compaction ---
        std::unique_ptr<sst_manager> _sst_manager;

        // Index page cache
        std::shared_ptr<sharded_page_cache> _page_cache;

        // Background pool
        std::shared_ptr<io::io_executor> _bg_pool;

        // --- Utilities ---
        logger _logger{"database"}; ///< Logger instance for database-related messages.

    public:
        /// Type alias for byte buffers used for values.
        using byte_buffer_t = std::vector<uint8_t>;

        /**
         * @brief Asynchronously retrieves the value associated with a key.
         * Checks memtable first, then relevant sorted_index files, then reads from value_table.
         * @param key The key to look up.
         * @param executor The I/O executor context for disk operations.
         * @return An async task resolving to an expected containing the value (byte_buffer_t) or an error.
         */
        tmc::task<expected<byte_buffer_t>> get_async(const key_t& key);

        /**
         * @brief Asynchronously inserts or updates a key-value pair (by copying value).
         * Writes the value to the current value_table, then adds/updates the key->value_ptr in the memtable.
         * May trigger a memtable flush and/or compaction if thresholds are met.
         * @param key The key to insert/update.
         * @param value The value data (copied).
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure.
         */
        tmc::task<hedge::status> put_async(const key_t& key, const byte_buffer_t& value);

        /**
         * @brief Asynchronously marks a key as deleted.
         * Finds the latest value_ptr for the key (memtable or sorted_index), marks it as deleted
         * (either in the value_table header or by adding a tombstone entry to memtable),
         * and potentially updates the memtable with the tombstone.
         * @param key The key to remove.
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure (e.g., key not found).
         */
        tmc::task<hedge::status> remove_async(const key_t& key);

        /**
         * @brief Creates a scan iterator over [lower, upper) within a single partition.
         * The partition is determined from the lower bound key (or upper if lower is nullopt).
         */
        [[nodiscard]] hedge::expected<scan_iterator> scan(std::optional<key_t> lower, std::optional<key_t> upper);

        void trigger_compaction(bool compact_all);

        void wait_for_compactions_to_finish();

        /**
         * @brief Factory function to create a new database instance at the specified path.
         * Creates necessary directories and initializes the first value table.
         * @param base_path The root directory for the new database. Must not exist.
         * @param config Configuration settings for the database.
         * @return An expected containing a shared pointer to the new database instance or an error.
         */
        static expected<std::shared_ptr<database>> make_new(const std::filesystem::path& base_path, const db_config& config);
        /**
         * @brief Factory function to load an existing database instance from a path. (Not implemented yet).
         * TODO: Not implemented yet.
         * @param base_path The root directory of the existing database.
         * @return An expected containing a shared pointer to the loaded database instance or an error.
         */
        static expected<std::shared_ptr<database>> load(const std::filesystem::path& base_path, const db_config& config);

        /**
         * @brief Manually triggers a flush of the current memtable to disk if size criteria are met.
         * Useful for ensuring data persistence before shutdown or during testing.
         * @return Status indicating success or failure (e.g., i/o error).
         */
        hedge::status flush();

        /**
         * @brief Calculates the average read amplification factor.
         * Represents the average number of sorted_index files checked (or, disk lookups) per lookup.
         * Lower is better (1.0 is ideal after full compaction).
         * @return The average load factor across all partitions. Returns NaN if there are no partitions.
         */
        [[nodiscard]] double read_amplification_factor();

        /**
         * @brief Prints the LSM tree structure showing SST count and average file size per level per partition.
         */
        void print_tree_structure() const;

    private:
        /**
         * @brief Private default constructor. Use factory functions `make_new` or `load`.
         */
        database() = default;

        /**
         * @brief Determines the partition prefix ID for a given key based on the database configuration.
         * @param key The key to find the partition for.
         * @return The calculated partition prefix (upper bound) ID.
         */
        [[nodiscard]] size_t _find_matching_partition_for_key(const key_t& key) const;

        /**
         * @brief Internal helper to find the most recent `value_ptr_t` and the corresponding `value_table` for a key.
         * Searches memtable, then relevant sorted_indices. Handles deleted entries.
         * @param key The key to locate.
         * @param executor The I/O executor for potential sorted_index lookups.
         * @return An async task resolving to an expected containing the pair {value_ptr, value_table_ptr} or an error (e.g., KEY_NOT_FOUND, DELETED).
         */
        tmc::task<expected<value_t>> _find_value(const key_t& key);

        // Shared initialization helpers used by both make_new and load.
        static hedge::status _validate_config(const db_config& config);
        static void _init_memtable(database& db, const db_config& config);
    };

} // namespace hedge::db
