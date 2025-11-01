#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <mutex>

#include <error.hpp>
#include <logger.h>
#include <sys/types.h> // POSIX types, consider if needed or include specific headers like <fcntl.h> if used

#include "async/io_executor.h"
#include "async/task.h"
#include "async/worker.h"
#include "mem_index.h"
#include "sorted_index.h"
#include "types.h"
#include "value_table.h"

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

        /// Number of key-value pairs allowed in the memtable before it's flushed to disk.
        size_t keys_in_mem_before_flush = 2'000'000;
        /// Exponent determining the number of partitions (2^num_partition_exponent). Affects index file organization.
        size_t num_partition_exponent = 10;
        /// Ratio (rhs_size / lhs_size) threshold triggering compaction during a two-way merge. rhs is the smaller index.
        /// If the smaller index is less than this fraction of the larger one, the merge occurs.
        /// Higher target_compaction_size_ratio means less frequent compactions (lower write amplification) but higher read amplification (more data read during compaction).
        double target_compaction_size_ratio = 0.2;
        /// Amount of data read ahead from each sorted_index file during compaction merges.
        size_t compaction_read_ahead_size_bytes = 16384;
        /// Maximum time to wait for a compaction job (currently for sub-tasks within the job) before timing out.
        std::chrono::milliseconds compaction_timeout{120000};
        /// If true, compaction is automatically triggered when the memtable is flushed and merge conditions are met.
        bool auto_compaction = true;
        /// Maximum number of concurrent compaction tasks allowed. Will block execution of insertions if exceeded.
        size_t max_pending_compactions = 16;
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

        // --- Persisted State Representation ---
        using sorted_index_ptr_t = std::shared_ptr<hedge::db::sorted_index>;

        // Map from partition ID to vector of sorted_index shared pointers.
        // Each partition has multiple sorted_index files:
        // - within sorted indices belonging to the same vector, the key ranges are overlapping
        // - across different partitions, the key ranges are disjoint
        using sorted_indices_map_t = std::map<uint16_t, std::vector<sorted_index_ptr_t>>;

        std::atomic<size_t> _flush_iteration{0};           ///< Counter for naming flushed index files uniquely within partitions.
        std::mutex _sorted_index_mutex;       ///< Protects access to the `_sorted_indices` map.
        sorted_indices_map_t _sorted_indices; ///< In-memory map representing the LSM tree levels/files.

        size_t _last_table_id{0};       ///< ID counter for the next value_table file to be created.
        std::mutex _value_tables_mutex; ///< Protects access to the `_value_tables` map.

        /// Map storing shared pointers to older, non-current value_table files, keyed by their ID.
        std::unordered_map<uint32_t, std::shared_ptr<value_table>> _value_tables;

        // --- Current/Mutable State ---
        /// Shared pointer to the currently active value_table file where new values are written.
        std::shared_ptr<value_table> _current_value_table;
        /// The active in-memory index (memtable) for recent writes.
        mem_index _mem_index;

        // --- Background Workers ---
        /// Worker thread dedicated to handling index compaction jobs.
        async::worker compaction_worker;
        /// Worker thread dedicated to handling value table garbage collection jobs (TODO).
        async::worker gc_worker;

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
        async::task<expected<byte_buffer_t>> get_async(key_t key, const std::shared_ptr<async::executor_context>& executor);

        /**
         * @brief Asynchronously inserts or updates a key-value pair (by copying value).
         * Writes the value to the current value_table, then adds/updates the key->value_ptr in the memtable.
         * May trigger a memtable flush and/or compaction if thresholds are met.
         * @param key The key to insert/update.
         * @param value The value data (copied).
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure.
         */
        async::task<hedge::status> put_async(key_t key, const byte_buffer_t& value, const std::shared_ptr<async::executor_context>& executor);
        /**
         * @brief Asynchronously inserts or updates a key-value pair (by moving value).
         * Optimized version of put_async that moves the value data.
         * @param key The key to insert/update.
         * @param value The value data (moved).
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure.
         */
        async::task<hedge::status> put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor);

        /**
         * @brief Asynchronously marks a key as deleted.
         * Finds the latest value_ptr for the key (memtable or sorted_index), marks it as deleted
         * (either in the value_table header or by adding a tombstone entry to memtable),
         * and potentially updates the memtable with the tombstone.
         * @param key The key to remove.
         * @param executor The I/O executor context.
         * @return An async task resolving to a status indicating success or failure (e.g., key not found).
         */
        async::task<hedge::status> remove_async(key_t key, const std::shared_ptr<async::executor_context>& executor);
        /**
         * @brief Submits a compaction job to the background compaction worker.
         * Iteratively merges pairs of sorted_index files within partitions based on configured ratios until stable.
         * Stable means that no further merges can be performed because of the size ratio criteria between sorted indices
         * is met, or there is only one sorted index file left in the partition.
         *
         * @example Consider that for a given ID, we have this set indices with the following sizes:
         * - A: 200,000,000 entries
         * - B: 30,000,000 entries
         * - C: 5,000,000 entries
         * - D: 1,000,000 entries
         *
         * This is a STABLE state if the target_compaction_size_ratio is 0.2, because:
         * - B / A = 0.15 < 0.2 ---> merge will be skipped
         * - C / B = 0.166 < 0.2 ---> merge will be skipped
         * - D / C = 0.2 <= 0.2 ---> merge will be skipped
         *
         * But now we add a new index E with 1,000,000 entries. This is how the sizes look now:
         * - A: 200,000,000 entries
         * - B: 30,000,000 entries
         * - C: 5,000,000 entries
         * - D: 1,000,000 entries
         * - E: 1,000,000 entries
         *
         * Now the state is UNSTABLE, because:
         * E and D will be merged (size_ratio is 1.0), resulting in a new index F with 2,000,000 entries.
         * F' and C will be merged (size_ratio is 0.4), resulting in a new index F'' with 7,000,000 entries.
         * F'' and B will be merged (size_ratio is 0.23), resulting in a new index F''' with 37,000,000 entries.
         * Finally, A and F''' will NOT be merged as the size_ratio is 37/200 = 0.185 < 0.2.
         *
         * This is the final STABLE state after compaction:
         * - A: 200,000,000 entries
         * - F''': 37,000,000 entries
         *
         * If new sorted indices are added during the compaction (e.g., due to memtable flushes),
         * they will NOT be considered in the current compaction job.
         * @param ignore_ratio If true, forces merges even if size ratio criteria aren't met (e.g., for full compaction).
         * @param executor The I/O executor context used *within* the compaction job for disk I/O.
         * @return A future that resolves to the status of the submitted compaction job upon completion.
         */
        std::future<hedge::status> compact_sorted_indices(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor);

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
        static expected<std::shared_ptr<database>> load(const std::filesystem::path& base_path);

        /**
         * @brief Manually triggers a flush of the current memtable to disk if size criteria are met.
         * Useful for ensuring data persistence before shutdown or during testing.
         * @return Status indicating success or failure (e.g., i/o error).
         */
        hedge::status flush();

        /**
         * @brief Submits a garbage collection job for value tables to the background GC worker.
         * NOT EVEN TESTED YET. Will likely reimplement later with mmap.
         * TODO: Implement GC logic: iterate tables, copy live data, update indices, delete old tables.
         * @param executor The I/O executor context used *within* the GC job.
         * @return A future resolving to the status of the GC job upon completion.
         */
        std::future<hedge::status> garbage_collect_tables(const std::shared_ptr<async::executor_context>& executor);

        /**
         * @brief Calculates the average read amplification factor.
         * Represents the average number of sorted_index files checked per partition during a lookup.
         * Lower is better (1.0 is ideal after full compaction).
         * @return The average load factor across all partitions. Returns NaN if there are no partitions.
         */
        [[nodiscard]] double read_amplification_factor();

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
         * @brief Closes the current value table, moves it to the map of older tables, and creates a new active value table.
         * Called when the current value table is full.
         * @return Status indicating success or failure.
         */
        hedge::status _rotate_value_table();
        /**
         * @brief Flushes the contents of the `_mem_index` to new `sorted_index` file(s) on disk.
         * Clears the `_mem_index` afterwards. Manages partitioning and file naming.
         * @return Status indicating success or failure.
         */
        hedge::status _flush_mem_index();
        /**
         * @brief The core logic for performing index compaction, run by the `compaction_worker`.
         * Updates the main `_sorted_indices` map upon completion.
         * @param ignore_ratio If true, forces merges regardless of size ratios.
         * @param executor The I/O executor for disk operations during merging.
         * @return Status indicating the overall success or failure of the compaction job.
         */
        hedge::status _compaction_job(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor);
        /**
         * @brief The core logic for garbage collecting a single value table, run by the `gc_worker`. (Not implemented yet).
         * TODO: Implement the process of reading a table, copying live data to a new table, updating indices, and deleting the old table.
         * @param table The value table to garbage collect.
         * @param id The target ID for the new, compacted value table.
         * @param executor The I/O executor for disk operations during GC.
         * @return An async task resolving to a status indicating success or failure.
         */
        async::task<hedge::status> _garbage_collect_table(std::shared_ptr<value_table> table, size_t id, const std::shared_ptr<async::executor_context>& executor);
        /**
         * @brief Internal helper to find the most recent `value_ptr_t` and the corresponding `value_table` for a key.
         * Searches memtable, then relevant sorted_indices. Handles deleted entries.
         * @param key The key to locate.
         * @param executor The I/O executor for potential sorted_index lookups.
         * @return An async task resolving to an expected containing the pair {value_ptr, value_table_ptr} or an error (e.g., KEY_NOT_FOUND, DELETED).
         */
        async::task<expected<std::pair<value_ptr_t, std::shared_ptr<value_table>>>> _find_value_ptr_and_value_table(key_t key, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedge::db
