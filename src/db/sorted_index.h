#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <sys/types.h>
#include <uuid.h>

#include <error.hpp>

#include "async/io_executor.h"
#include "async/task.h"
#include "cache.h"
#include "fs/fs.hpp"
#include "tsl/robin_map.h"
#include "types.h"
#include "utils.h"

#include "perf_counter.h"

namespace hedge::db
{
    /**
     * @brief Defines the structure of the footer at the end of every sorted_index file.
     * @details The footer is the entry point for reading an index file. It contains
     * metadata about the file's layout, including offsets to the main index data
     * and the meta-index, as well as versioning information. It is always located
     * at the very end of the file for easy access.
     */
    struct sorted_index_footer
    {
        static constexpr uint32_t CURRENT_FOOTER_VERSION = 1; ///< Current version of the file format.

        char header[16] = "hedge_FOOTER";        ///< Magic string ("hedge_FOOTER") to identify the footer block.
        uint8_t version{CURRENT_FOOTER_VERSION}; ///< File format version number.
        uint64_t upper_bound{};                  ///< The upper bound (inclusive) of the key partition this index belongs to. All keys in this file are <= this value.
        uint64_t indexed_keys{};                 ///< Total number of key-value pointer entries in the main index data section.
        uint64_t meta_index_entries{};           ///< Total number of entries in the meta-index section.
        uint64_t index_start_offset{};           ///< Byte offset from the beginning of the file where the main index data begins.
        uint64_t index_end_offset{};             ///< Byte offset from the beginning of the file where the main index data ends (exclusive of any padding).
        uint64_t meta_index_start_offset{};      ///< Byte offset from the beginning of the file where the meta-index data begins.
        uint64_t meta_index_end_offset{};        ///< Byte offset from the beginning of the file where the meta-index data ends (exclusive of any padding).
        uint64_t footer_start_offset{};          ///< Byte offset from the beginning of the file where this footer structure begins.
        uint64_t epoch{};                        ///< Epoch timestamp indicating when the file was created.
    };

    /**
     * @brief An entry in the meta-index.
     * @details The meta-index is a secondary, smaller index stored within the sorted_index file.
     * Each entry stores the highest key found in a corresponding data page (block) of the main index.
     * This allows for a fast binary search over the meta-index to locate the correct page
     * for a given key without reading the entire (potentially large) main index file.
     */
    struct meta_index_entry
    {
        uuids::uuid key{}; ///< The maximum key present in the corresponding data page.
    };

    /**
     * @brief Represents an immutable, sorted key-value index file on disk.
     * @details This class manages access to a single sorted index file.
     * These files are typically created by flushing a `mem_index` or merging existing `sorted_index` files.
     * It inherits from `fs::file` to manage the underlying file descriptor.
     *
     * Key Features:
     * - **Immutability:** Once created, the file content is generally not modified (except potentially for experimental in-place updates).
     * - **Key uniqueness:** Each key appears at most once in the index.
     *                       For updating or removing an entry, it should be done in the mem_index and then flushed/merged to create a new sorted_index file.
     * - **Sorted Keys:** Entries (`index_entry_t`) within the file are sorted by key.
     * - **Meta-Index:** Contains an in-memory meta-index for efficient page lookups.
     * - **Optional In-Memory Index:** Can load the entire main index into memory (`_index`) for faster lookups if configured, or load pages on demand.
     * - **Asynchronous Lookups:** Provides `lookup_async` using `io_executor` and `liburing` for high performance.
     */
    class sorted_index : public fs::file
    {
        // Allow index_ops to access private members for operations like merging/saving.
        friend struct index_ops;

        page_aligned_buffer<index_entry_t> _index;                         ///< In-memory copy of the entire main index data (optional, loaded by load_index()). Empty if using on-demand page loading.
        page_aligned_buffer<meta_index_entry> _meta_index;                 ///< In-memory copy of the meta-index (always loaded on construction/load).
        std::optional<page_aligned_buffer<meta_index_entry>> _super_index; ///< The super index can be built to facilitate lookup over the meta index.
        sorted_index_footer _footer;                                       ///< In-memory copy of the file's footer (always loaded on construction/load).

    public:
        /**
         * @brief Constructs a sorted_index object.
         * @param fd An rvalue reference to an `fs::file` object managing the file descriptor.
         * @param index An rvalue reference to a vector containing the main index entries (can be empty).
         * @param meta_index An rvalue reference to a vector containing the meta index entries.
         * @param footer A copy of the sorted_index_footer structure.
         */
        sorted_index(fs::file fd, page_aligned_buffer<index_entry_t> index, page_aligned_buffer<meta_index_entry> meta_index, sorted_index_footer footer);

        /**
         * @brief Default constructor. Creates an invalid/uninitialized sorted_index.
         */
        sorted_index() = default;

        /**
         * @brief Move constructor.
         */
        sorted_index(sorted_index&& other) noexcept = default;

        /**
         * @brief Move assignment operator.
         */
        sorted_index& operator=(sorted_index&& other) noexcept = default;

        // Prevent copying, as this object manages a unique file resource.
        sorted_index(const sorted_index&) = delete;
        sorted_index& operator=(const sorted_index&) = delete;

        /**
         * @brief Synchronously looks up a key in the index.
         * @details This method uses memory mapping (`mmap`) for file access if the index (`_index`)
         * is not already loaded into memory. It performs two binary searches:
         * 1. Searches the in-memory `_meta_index` to find the correct data page ID.
         * 2. Searches within the identified data page (either in memory or via mmap) to find the key.
         * It will likely be deprecated in future.
         * @param key The key (`key_t`) to search for.
         * @return An `expected` containing an `std::optional<value_ptr_t>`. The optional contains the
         * `value_ptr_t` if the key is found, otherwise it's empty. Returns an error if file
         * operations fail.
         */
        [[nodiscard]] hedge::expected<std::optional<value_ptr_t>> lookup(const key_t& key) const;

        /**
         * @brief Asynchronously looks up a key in the index using `io_executor` and `liburing`.
         * @details This is the high-performance lookup path optimized for asynchronous I/O.
         * 1. Searches the in-memory `_meta_index` to find the correct data page ID.
         * 2. Submits an `io_uring` read request to load only the required data page from disk.
         * 3. Performs a binary search within the loaded page data.
         * @param key The key (`key_t`) to search for.
         * @param executor A shared pointer to the `io_uring` executor context used for the read operation.
         * @return A `async::task` that resolves to an `expected` containing an `std::optional<value_ptr_t>`, or an error.
         */
        [[nodiscard]] async::task<expected<std::optional<value_ptr_t>>> lookup_async(const key_t& key, const std::shared_ptr<shared_page_cache>& cache) const;

        /**
         * @brief Loads the entire main index data from the associated file into the in-memory `_index` vector.
         * @details Uses `mmap` to read the data efficiently. If the index is already loaded (`!_index.empty()`),
         * this function does nothing and returns `ok()`. This is useful for smaller indices that can fit
         * comfortably in RAM, providing faster lookups by avoiding disk I/O.
         * @return A `hedge::status`. Returns an error if memory mapping or file reading fails.
         * @throws std::runtime_error (meaning, it panics) if mmap fails.
         */
        hedge::status load_index_from_fs();

        /**
         * @brief Gets the upper bound of the key partition this index belongs to, as stored in the footer.
         * @return The partition identifier (a 64-bit unsigned integer).
         */
        [[nodiscard]] size_t upper_bound() const { return this->_footer.upper_bound; }

        /**
         * @brief Gets the total number of keys stored in this index file, as stored in the footer.
         * @return The number of indexed keys (a 64-bit unsigned integer).
         */
        [[nodiscard]] size_t size() const { return this->_footer.indexed_keys; }

        [[nodiscard]] size_t epoch() const { return this->_footer.epoch; }

        /**
         * @brief Prints various statistics about the index (file path, counts, sizes) to standard output.
         * @details Useful for debugging and understanding the state of the index.
         */
        void stats() const;

        /**
         * @brief Clears the in-memory `_index` vector and releases its memory.
         * @details After calling this, subsequent `lookup` calls will need to use `mmap` or `lookup_async`
         * will need to load pages from disk, until `load_index()` is called again. Useful for reducing memory footprint.
         */
        void clear_index();

    private:
        /**
         * @brief Performs a binary search for a key within a given range of `index_entry_t` (representing a data page of key/value_ptr pairs).
         * @param key The key to search for.
         * @param page_start Pointer to the beginning of the page data.
         * @param page_end Pointer to the end (one past the last element) of the page data.
         * @return An `std::optional<value_ptr_t>` containing the value pointer if the key is found, otherwise `std::nullopt`.
         */
        static std::optional<value_ptr_t> _find_in_page(const key_t& key, const index_entry_t* page_start, const index_entry_t* page_end);

        /**
         * @brief Finds the ID (index) of the data page that *might* contain the given key.
         * @details Performs a binary search on the in-memory `_meta_index`. It finds the first
         * meta-index entry whose `page_max_key` is greater than or equal to the target `key`.
         * @param key The key to search for.
         * @return An `std::optional<size_t>` containing the page ID (0-based index) if the key
         * falls within the range covered by the meta-index, otherwise `std::nullopt`.
         */
        [[nodiscard]] std::optional<size_t> _find_page_id(const key_t& key) const;

        /**
         * @brief Asynchronously loads a single data page (fixed size `PAGE_SIZE_IN_BYTES=4096 bytes`) from the index file using `io_executor` and `liburing`.
         * @param offset The byte offset within the file where the page begins.
         * @param executor A shared pointer to the `io_uring` executor context.
         * @return A `async::task` that resolves to an `expected` containing a `std::unique_ptr<uint8_t>`
         * holding the page data, or an error if the read fails. The memory is allocated page-aligned.
         */
        [[nodiscard]] async::task<hedge::status> _load_page_async(size_t offset, uint8_t* data_ptr) const;
    };

} // namespace hedge::db
