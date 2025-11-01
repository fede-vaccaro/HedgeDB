#pragma once

#include "mem_index.h"
#include "sorted_index.h"
#include "types.h"

namespace hedge::db
{
    /**
     * @brief Provides static utility functions for managing index files (mem_index and sorted_index).
     * @details This struct encapsulates the core logic for operations that involve
     * transforming or combining index structures, such as flushing in-memory data to disk,
     * loading index files from disk, and merging existing index files during compaction.
     * All methods are static as they operate on index objects rather than representing
     * an index itself.
     */
    struct index_ops
    {
        /**
         * @brief Merges multiple mem_index instances into a single, sorted vector of index entries in memory.
         * @details This is typically the first step when flushing memtables to disk. It aggregates
         * all key-value pointers from the provided memtables, sorts them by key, and returns
         * a single vector ready to be partitioned and written to sorted_index files.
         * The source mem_index instances are moved from and cleared during the process.
         * @param indices A vector of `mem_index` objects to merge (passed by rvalue reference).
         * In practice, this has always been a single mem_index, but the code supports multiple.
         * @return A `std::vector<index_entry_t>` containing all entries from the input indices, sorted by key.
         */
        static std::vector<index_entry_t> merge_memtables_in_mem(std::vector<mem_index>&& indices);

        /**
         * @brief Loads a `sorted_index` representation from a file on disk.
         * @details Reads the footer and meta-index from the specified file path. Optionally,
         * it can also load the entire main index data into memory immediately. If the main index
         * is not loaded, lookups will require disk access (either mmap or async reads).
         * @param path The filesystem path to the `sorted_index` file.
         * @param load_index If `true`, loads the entire main index data (`_index`) into memory upon loading.
         * If `false` (default), only the footer and meta-index are loaded.
         * @return An `expected<sorted_index>` containing the loaded `sorted_index` object on success,
         * or an error if the file cannot be opened, read, or is malformed.
         */
        static hedge::expected<sorted_index> load_sorted_index(const std::filesystem::path& path, bool load_index = false);

        /**
         * @brief Creates a new `sorted_index` file on disk from a sorted vector of index entries.
         * @details Writes the main index data, computes and writes the meta-index, and writes the
         * footer to create a complete `sorted_index` file according to the defined format.
         * The input vector `sorted_keys` is moved into the resulting `sorted_index` object if successful.
         * @param path The filesystem path where the new `sorted_index` file should be created.
         * @param sorted_keys An rvalue reference to a vector of `index_entry_t`, expected to be pre-sorted by key.
         * @param upper_bound The upper bound key prefix for the partition this index belongs to.
         * @param use_odirect If `true`, opens the file with O_DIRECT flag for direct I/O access.
         * @return An `expected<sorted_index>` containing the newly created `sorted_index` object (representing the file)
         * on success, or an error if file writing fails.
         */
        static hedge::expected<sorted_index> save_as_sorted_index(const std::filesystem::path& path, std::vector<index_entry_t>&& sorted_keys, size_t upper_bound, bool use_odirect);

        /**
         * @brief Flushes one or more `mem_index` instances to disk, creating potentially multiple `sorted_index` files based on key partitioning.
         * @details This function orchestrates the process of turning in-memory data into persistent files.
         * 1. Merges all input `mem_index` instances into a single sorted list using `merge_memtables_in_mem`.
         * 2. Iterates through the sorted list, partitioning entries based on their key prefix (`extract_prefix`).
         * 3. For each partition, calls `save_as_sorted_index` to write a new `sorted_index` file containing only the entries for that partition.
         * The `flush_iteration` is used to generate unique file names within each partition directory.
         * @param base_path The base directory where partition subdirectories (`00/`, `01/`, etc.) will be created.
         * @param indices A vector of `mem_index` objects to flush (passed by rvalue reference).
         * @param num_partition_exponent Determines the number of partitions (2^num_partition_exponent).
         * @param flush_iteration A unique identifier for this flush operation, used in filenames
         * to distinguish between multiple and independent flushes to the same partition (e.g., `ff/ff00.0`, `ff/ff00.1`).
         * @return An `expected<std::vector<sorted_index>>` containing the newly created `sorted_index` objects on success,
         * or an error if any step fails.
         */
        static hedge::expected<std::vector<sorted_index>> flush_mem_index(const std::filesystem::path& base_path,
                                                                          std::vector<mem_index>&& indices,
                                                                          size_t num_partition_exponent,
                                                                          size_t flush_iteration,
                                                                          bool use_odirect = false);

        /**
         * @brief Configuration options for the `two_way_merge_async` and `two_way_merge` operations.
         */
        struct merge_config
        {
            size_t read_ahead_size{};          ///< Number of bytes to read from each input index file at a time during the merge. (e.g., 64 * 1024 for 64KB chunks).
            size_t new_index_id{};             ///< The unique ID (iteration number) to use for the output merged index file name (e.g., ".<new_index_id>").
            std::filesystem::path base_path{}; ///< The base directory where the output file will be created (within its partition subdirectory).
            bool discard_deleted_keys{false};  ///< If `true`, entries marked with the delete flag (`value_ptr_t::is_deleted()`) will not be written to the output file.
                                               ///< Set `false` if there are more indices belonging to the same partition to be merged later, to preserve delete markers until the final merge.
                                               ///< Set `true` when this is the final merge for the partition to eliminate deleted entries from the final index.
            bool create_new_with_odirect{false};       ///< If `true`, opens the output file with O_DIRECT flag for direct I/O access.
        };

        /**
         * @brief Asynchronously merges two `sorted_index` files into a new `sorted_index` file using `io_executor` `liburing`.
         * @details Implements an external merge sort algorithm optimized for asynchronous I/O and reduced memory footprint.
         * Reads chunks from `left` and `right` files, merges them in memory (handling key deduplication
         * using `value_ptr_t::operator<` to keep the newest entry), and writes the result to a new file.
         * @param config Configuration settings for the merge operation (`merge_config`).
         * @param left The first `sorted_index` file to merge.
         * @param right The second `sorted_index` file to merge.
         * @param executor A shared pointer to the `liburing`-based executor context.
         * @return A `async::task` that resolves to an `expected<sorted_index>` containing the newly created merged `sorted_index`
         * object on success, or an error if the merge fails.
         */
        static async::task<hedge::expected<sorted_index>> two_way_merge_async(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);

        /**
         * @brief Synchronously merges two `sorted_index` files by running and waiting for `two_way_merge_async`.
         * @details Provides a blocking wrapper around the asynchronous merge operation.
         * Requires a valid `executor` to run the underlying asynchronous task.
         * @param config Configuration settings for the merge operation (`merge_config`).
         * @param left The first `sorted_index` file to merge.
         * @param right The second `sorted_index` file to merge.
         * @param executor A shared pointer to the `io_uring` executor context.
         * @return An `expected<sorted_index>` containing the newly created merged `sorted_index` object on success,
         * or an error if the merge fails or the executor is invalid.
         */
        static hedge::expected<sorted_index> two_way_merge(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedge::db