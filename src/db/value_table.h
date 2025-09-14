#pragma once

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>

#include <error.hpp>

#include "async/io_executor.h"
#include "async/task.h"
#include "common.h"
#include "fs/fs.hpp"

namespace hedge::db
{
    // two uuids to reduce the chance of collision in case of recovery
    constexpr std::array<uint8_t, 16> FILE_SEPARATOR = {0x59, 0x34, 0xef, 0xdc, 0x74, 0x62, 0x11, 0xf0, 0x95, 0x44, 0x33, 0x9d, 0x15, 0x81, 0x8d, 0x1e};
    constexpr std::array<uint8_t, 16> EOF_MARKER = {0xc2, 0xf6, 0x97, 0xbc, 0x74, 0x68, 0x11, 0xf0, 0x82, 0x0d, 0xdf, 0x7d, 0x66, 0x5a, 0xc0, 0x46};

    struct file_header
    {
        std::array<uint8_t, 16> separator{FILE_SEPARATOR};
        key_t key{};
        size_t file_size{};
        bool deleted_flag{false};
    };

    struct output_file
    {
        file_header header{};
        std::vector<uint8_t> binaries{};
    };

    struct value_table_info
    {
        size_t current_offset{};

        // gc related info
        size_t items_count{};
        size_t occupied_space{};

        size_t deleted_count{};
        size_t freed_space{};

        uint8_t padding[8]; // NOLINT

        bool operator==(const value_table_info& other) const = default;
    };

    /**
        The value table is where the all the values are stored.

        The layout is

        output_file0
        output_file1
        output_file2
        ...
        output_fileN
        EOF marker
        value_table_info
    */
    class value_table : public fs::file
    {
        uint32_t _unique_id;
        size_t _current_offset{0};
        fs::non_owning_mmap _mmap{};
        std::unique_ptr<std::mutex> _delete_mutex{std::make_unique<std::mutex>()};

        value_table() = default;
        value_table(uint32_t unique_id, size_t current_offset, fs::file file_descriptor, fs::non_owning_mmap mmap)
            : fs::file(std::move(file_descriptor)), _unique_id(unique_id), _current_offset(current_offset), _mmap(std::move(mmap)) {}

    public:
        static constexpr std::string_view TABLE_FILE_EXTENSION = ".vt";
        static constexpr size_t TABLE_MAX_SIZE_BYTES = std::numeric_limits<uint32_t>::max(); // addressable space in a uint32_t
        static constexpr size_t TABLE_ACTUAL_MAX_SIZE = TABLE_MAX_SIZE_BYTES + sizeof(EOF_MARKER) + sizeof(value_table_info);
        static constexpr size_t TABLE_MAX_ID = std::numeric_limits<uint32_t>::max();
        static constexpr size_t MAX_FILE_SIZE = (((1UL << 17) - 1) * PAGE_SIZE_IN_BYTES) - sizeof(file_header);

        [[nodiscard]] uint32_t id() const
        {
            return this->_unique_id;
        }

        [[nodiscard]] size_t current_offset() const
        {
            return this->_current_offset;
        }

        [[nodiscard]] size_t free_space() const
        {
            return value_table::TABLE_MAX_SIZE_BYTES - this->_current_offset;
        }

        [[nodiscard]] value_table_info info() const;

        struct write_reservation
        {
            size_t offset{};
        };

        using next_offset_and_size_t = std::pair<size_t, size_t>;

        expected<write_reservation> get_write_reservation(size_t file_size);
        async::task<expected<hedge::value_ptr_t>> write_async(key_t key, const std::vector<uint8_t>& value, const write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor);

        // nb file_size includes the size of the header
        async::task<expected<output_file>> read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor, bool skip_delete_check = false);

        // this class method is needed to iterate over the table (and skip deleted entries)
        async::task<expected<std::pair<file_header, std::unique_lock<std::mutex>>>> get_first_header_async(const std::shared_ptr<async::executor_context>& executor);
        async::task<expected<std::pair<output_file, next_offset_and_size_t>>> read_file_and_next_header_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor);

        async::task<status> delete_async(key_t key, size_t offset, const std::shared_ptr<async::executor_context>& executor);

        static hedge::expected<value_table> make_new(const std::filesystem::path& base_path, uint32_t table_id, bool preallocate = true);
        static hedge::expected<value_table> load(const std::filesystem::path& path, fs::file::open_mode open_mode);

    private:
        constexpr static fs::range _page_align_for_mmap()
        {
            constexpr size_t last_page_size = TABLE_ACTUAL_MAX_SIZE % PAGE_SIZE_IN_BYTES;

            constexpr size_t mmap_begin_range = TABLE_ACTUAL_MAX_SIZE - last_page_size - PAGE_SIZE_IN_BYTES;

            constexpr size_t mmap_size = last_page_size + PAGE_SIZE_IN_BYTES;

            return fs::range{.start = mmap_begin_range, .size = mmap_size};
        }

        static value_table_info& _get_info_from_mmap(const fs::non_owning_mmap& mmap);
        [[nodiscard]] value_table_info& _info();
    };
} // namespace hedge::db