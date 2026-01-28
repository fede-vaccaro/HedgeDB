#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <mutex> // Added for _delete_mutex documentation

#include <error.hpp>
#include <shared_mutex>

#include "async/io_executor.h"
#include "async/task.h"
#include "fs/fs.hpp"
#include "types.h"

namespace hedge::db
{
    /**
     * @brief Magic bytes used to separate file entries within the value table.
     * @details It might also needed in future to inspect a value_table independently of the
     * index.
     */
    constexpr std::array<uint8_t, 16> FILE_SEPARATOR = {0x59, 0x34, 0xef, 0xdc, 0x74, 0x62, 0x11, 0xf0, 0x95, 0x44, 0x33, 0x9d, 0x15, 0x81, 0x8d, 0x1e};
    /** @brief Magic bytes used to mark the end of valid file entries, before the value_table_info. */
    constexpr std::array<uint8_t, 16> EOF_MARKER = {0xc2, 0xf6, 0x97, 0xbc, 0x74, 0x68, 0x11, 0xf0, 0x82, 0x0d, 0xdf, 0x7d, 0x66, 0x5a, 0xc0, 0x46};

    /**
     * @brief Header structure prepended to each value entry stored in the value_table.
     */
    struct file_header
    {
        std::array<uint8_t, 16> separator{FILE_SEPARATOR}; ///< Magic bytes to identify the start of a header.
        key_t key{};                                       ///< The key associated with this value (primarily for recovery/validation).
        uint32_t file_size{};                              ///< The size of the actual value data (excluding this header).
    };

    /**
     * @brief Represents a value read from the value_table, including its header and binary data.
     */
    struct output_file
    {
        file_header header{};            ///< The header read from the file.
        std::vector<uint8_t> binaries{}; ///< The actual value data.
    };

    /**
     * @brief Metadata structure stored at the end of the value_table file.
     * @details Contains information about the current state of the table, used for
     * recovery and for garbage collection heuristics.
     */
    struct value_table_info
    {
        std::size_t current_offset{}; ///< The offset where the next write will begin.

        // Garbage-collector related info (Currently tracked but GC not implemented)
        std::size_t items_count{};    ///< Total number of items (including deleted) written.
        std::size_t occupied_space{}; ///< Total bytes occupied by headers and non-deleted values.

        std::size_t deleted_count{}; ///< Number of items marked as deleted.
        std::size_t freed_space{};   ///< Total bytes occupied by headers and values of deleted items.

        uint8_t padding[8]; // NOLINT ///< Padding to ensure struct size and alignment.

        /** @brief Default equality comparison. */
        bool operator==(const value_table_info& other) const = default;
    };

    /**
     * @brief Represents a single value table file (value log) where actual key-value data is stored.
     * @details Value tables are append-only files. Each entry consists of a `file_header` followed by the value's binary data.
     * An `EOF_MARKER` and a `value_table_info` struct are stored at the very end of the file.
     * Lookups are performed by reading directly at a specific offset provided by a `value_ptr_t` (obtained from an index).
     *  Inherits from `fs::file` to manage the underlying file descriptor.
     *
     *  File Layout:
     *  [file_header_0][value_data_0][padding to page size if necessary]
     *  [file_header_1][value_data_1][padding to page size if necessary]
     *  ...
     *  [file_header_N][value_data_N][padding to page size if necessary]
     *  [EOF_MARKER]
     *  --- blank space if any ---
     *  [value_table_info]
     */
    class value_table : public fs::file
    {
        uint32_t _unique_id;                   ///< Unique identifier for this value table file.
        std::atomic_size_t _current_offset{0}; ///< Current write offset within the file.
        std::optional<fs::mmap_view> _mmap;    // mmap for write only if not direct IO

        /** @brief Default constructor (private). Use factory methods. */
        value_table() = default;
        /** @brief Private constructor used by factory methods. */
        value_table(uint32_t unique_id, size_t current_offset, fs::file file_descriptor)
            : fs::file(std::move(file_descriptor)), _unique_id(unique_id), _current_offset(current_offset)
        {
        }

    public:
        /** @brief File extension used for value table files. */
        static constexpr std::string_view TABLE_FILE_EXTENSION = ".vt";
        /** @brief Maximum size in bytes for the data portion of the table (excluding EOF marker and info). Limited by uint32_t offset in value_ptr_t. */
        static constexpr size_t TABLE_MAX_SIZE_BYTES = std::numeric_limits<uint32_t>::max();
        /** @brief Actual maximum file size including EOF marker and info struct. */
        static constexpr size_t TABLE_ACTUAL_MAX_SIZE = TABLE_MAX_SIZE_BYTES + sizeof(EOF_MARKER) + sizeof(value_table_info);
        /** @brief Maximum possible ID for a value table. */
        static constexpr size_t TABLE_MAX_ID = std::numeric_limits<uint32_t>::max();
        /** @brief Maximum practical size for a single value entry (derived constraint). */
        static constexpr size_t MAX_VALUE_SIZE = (((1UL << 17) - 1) * PAGE_SIZE_IN_BYTES) - sizeof(file_header); // TODO: Re-evaluate this limit's origin/necessity

        /// Deleted constructor and assignment.
        value_table(value_table&&) = delete;
        value_table& operator=(value_table&&) = delete;

        /// Deleted copy constructor and assignment.
        value_table(const value_table&) = delete;
        value_table& operator=(const value_table&) = delete;

        /**
         * @brief Gets the unique identifier of this value table.
         * @return The table ID.
         */
        [[nodiscard]] uint32_t id() const
        {
            return this->_unique_id;
        }

        /**
         * @brief Calculates the remaining free space in the data portion of the table.
         * @return The available space in bytes.
         */
        [[nodiscard]] size_t free_space() const
        {
            return value_table::TABLE_MAX_SIZE_BYTES - this->_current_offset;
        }

        /**
         * @brief Represents a reserved space for writing a value.
         * @see value_table::get_write_reservation
         */
        struct write_reservation
        {
            size_t offset{}; ///< The offset at which the write should occur.
        };

        [[nodiscard]] size_t current_offset() const
        {
            return this->_current_offset.load(std::memory_order_relaxed);
        }

        /**
        todo: rewrite doc
         */
        expected<size_t> allocate_pages_for_write(size_t num_bytes);

        /**
         * @brief Asynchronously writes a key-value pair at a previously reserved offset.
         * @param key The key being written.
         * @param value The value data.
         * @param reservation The `write_reservation` obtained from `get_write_reservation`.
         * @param executor The executor context for async I/O.
         * @return `async::task<expected<hedge::value_ptr_t>>` resolving to the `value_ptr_t` pointing
         * to the newly written entry, or an error if the write fails.
         */
        async::task<expected<hedge::value_ptr_t>> write_async(key_t key, const std::vector<uint8_t>& value, const write_reservation& reservation);

        /**
         * @brief Asynchronously reads a value entry (header + data) from a specific offset and size.
         * @param file_offset The starting byte offset of the `file_header`.
         * @param file_size The total size of the entry (header + data).
         * @param executor The executor context for async I/O.
         * @param skip_delete_check If true, reads the data even if the deleted flag is set (used during GC).
         * @return `async::task<expected<output_file>>` resolving to the read data (`output_file`) or an error
         * (e.g., `errc::DELETED` if the flag is set and not skipped, I/O errors, validation errors).
         */
        async::task<expected<output_file>> read_async(size_t file_offset, size_t file_size, bool skip_delete_check = false);

        /**
         * @brief Factory function to create a new, empty value table file.
         * @param base_path The directory where the file will be created.
         * @param table_id The unique ID for the new table.
         * @param preallocate If true (default), preallocates the full `TABLE_ACTUAL_MAX_SIZE` on disk using `fallocate`.
         * @return `expected<value_table>` containing the new table object or an error.
         */
        static hedge::expected<std::shared_ptr<value_table>> make_new(const std::filesystem::path& base_path, uint32_t table_id, bool preallocate = true);

        /**
         * @brief Factory function to load an existing value table file.
         * @param path The full path to the value table file.
         * @param open_mode The mode to open the file in (e.g., read-only, read-write).
         * @return `expected<value_table>` containing the loaded table object or an error.
         */
        static hedge::expected<std::shared_ptr<value_table>> load(const std::filesystem::path& path, fs::file::open_mode open_mode, bool use_direct = false);

        static hedge::expected<std::shared_ptr<value_table>> reload(value_table&& other, fs::file::open_mode open_mode, bool direct);

    private:
    };

    // Per thread write buffer
    class alignas(64) write_buffer
    {
        std::unique_ptr<uint8_t> _buffer{nullptr};
        size_t _buffer_capacity{0};
        std::atomic_size_t _write_buffer_head{0};

        std::shared_mutex _flush_mutex;
        std::shared_ptr<value_table> _reference_table{nullptr};
        size_t _file_offset{std::numeric_limits<size_t>::max()};

        std::mutex _write_lock;

    public:
        write_buffer() = default;
        write_buffer(size_t capacity)
            : _buffer(static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE_IN_BYTES, capacity))), _buffer_capacity(capacity)
        {
            assert(_buffer);
            assert(capacity % PAGE_SIZE_IN_BYTES == 0);
        }

        void set(std::shared_ptr<value_table> reference_table, size_t reserved_offset)
        {
            std::lock_guard lk(this->_flush_mutex);
            this->_reference_table = std::move(reference_table);
            this->_write_buffer_head.store(0);
            this->_file_offset = reserved_offset;
        }

        [[nodiscard]] bool is_set()
        {
            std::shared_lock lk(this->_flush_mutex);
            return this->_reference_table != nullptr;
        }

        async::task<status> flush(const std::shared_ptr<async::executor_context>& executor);
        expected<value_ptr_t> write(const key_t& key, const std::vector<uint8_t>& value);
        expected<output_file> try_read(value_ptr_t value_ptr);
    };
} // namespace hedge::db