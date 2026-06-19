#pragma once

#include <chrono>
#include <filesystem>
#include <memory>

#include <error.hpp>
#include <logger.h>
#include <shared_mutex>
#include <sys/types.h> // POSIX types, consider if needed or include specific headers like <fcntl.h> if used

#include "cache.h"
#include "db/memtable.h"
#include "db/range_iterator.h"
#include "db/sst_manager.h"
#include "io/io_executor.h"
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
        /// Maximum allowed exponent for partitioning (2^16 = 65536 partitions max).
        static constexpr size_t MAX_PARTITION_EXPONENT = 16;
        /// Memory budget in bytes for the memtable before a flush is triggered.
        size_t memtable_budget_bytes = 64 * MiB;
        /// Exponent determining the number of partitions (2^num_partition_exponent). Affects index file organization.
        size_t num_partition_exponent = 4;
        /// Ratio (rhs_size / lhs_size) threshold triggering compaction during a two-way merge. rhs is the smaller index.
        /// If the smaller index is less than this fraction of the larger one, the merge occurs.
        /// Higher bucket_ratio means less frequent compactions (lower write amplification) but higher read amplification (more data read during compaction).
        double bucket_ratio = 1.5;
        /// Amount of data read ahead from each sorted_index file during compaction merges.
        size_t compaction_read_ahead_size_bytes = 2 * MiB;
        /// Maximum time to wait for a compaction job (currently for sub-tasks within the job) before timing out.
        std::chrono::milliseconds compaction_timeout{120000};
        /// If true, compaction is automatically triggered when the memtable is flushed and merge conditions are met.
        bool auto_compaction = true;
        /// Maximum number of concurrent compaction tasks allowed. Will block execution of insertions if exceeded.
        size_t max_pending_compactions = 16;
        /// If true, use O_DIRECT flag for sst files I/O to bypass page cache. It's used in read, compaction and flush paths.
        bool use_direct_io = true;
        /// If true, use O_DIRECT flag for WAL files I/O to bypass page cache.
        bool use_odirect_for_wal = true; // NOT implemented
        /// Use 0 no cache is desired; the cache size in bytes otherwise
        size_t index_page_clock_cache_size_bytes = 0;
        /// Number of background workers to be used for compaction. std::nullopt means "use static pool", 0 means "auto detect".
        std::optional<size_t> num_background_workers = std::nullopt;
        /// Number of writer slots (rw_sync stripes + per-thread WAL files). Must be >= the number
        /// of threads that concurrently issue put/remove (e.g. one per db_ctx). std::nullopt means
        /// "use the static pool thread count" (legacy default).
        std::optional<size_t> num_writer_threads = std::nullopt;
        /// Maximum number of concurrent memtable flushes allowed before backpressure
        size_t max_pending_flushes = 8;
        /// Maximum number of levels in the LSM tree.
        size_t max_num_levels = 40;
        /// Minimum number of sorted_index files in the same level needed to trigger a merge.
        /// Higher means less frequent compactions (lower write amplification) but higher read amplification.
        size_t min_merge_width = 4;
        /// Maximum number of sorted_index files in the same level allowed before a merge is forced, regardless of their size ratio.
        size_t max_merge_width = 16;
        /// Per-partition L0 SST count threshold above which writes are blocked via backpressure.
        /// The effective threshold is multiplied by the total number of partitions.
        std::optional<size_t> ssts_in_l0_block_write_threshold = 40;
        /// If true, the key-values will not be written to the WAL and will only exist in the memtable until flushed.
        /// This reduces write amplification but increases risk of data loss on crash.
        bool disable_wal = false;
        /// If true, collects statistics about flush
        bool acquire_flush_stats = false;
        /// If true, collects statistics about compaction
        bool acquire_compaction_stats = false;

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
        std::filesystem::path _base_path;       ///< Root directory for the database.
        std::filesystem::path _partitions_path; ///< Subdirectory storing partitions and sst files.

        // --- Configuration ---
        db_config _config; ///< Database configuration settings.

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
        using byte_buffer_t = std::vector<std::byte>;

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
        hedge::status put(const key_t& key, const std::span<const std::byte>& value);

        /**
         * @brief Asynchronously marks a key as deleted.
         * Finds the latest value_ptr for the key (memtable or sorted_index), marks it as deleted
         * (either in the value_table header or by adding a tombstone entry to memtable),
         * and potentially updates the memtable with the tombstone.
         * @param key The key to remove.
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure (e.g., key not found).
         */
        hedge::status remove(const key_t& key);

        /**
         * @brief Creates a scan iterator over [lower, upper) within a single partition.
         * The partition is determined from the lower bound key (or upper if lower is nullopt).
         */
        [[nodiscard]] hedge::expected<range_iterator> scan(std::optional<key_t> lower, std::optional<key_t> upper, size_t read_ahead_size = 16 * KiB);

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
         * @brief Factory function to load an existing database instance from a path.
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

        /**
         * @brief Print some statistics about background compaction regarding throughput and bytes written.
         */
        void print_compaction_stats() const;

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
