#pragma once

#include <cstdint>
#include <cstdio>
#include <error.hpp>

#include <filesystem>
#include <limits>

#include "common.h"
#include "fs.hpp"
#include "io_executor.h"
#include "task.h"

namespace hedgehog::db
{

    struct file_footer
    {
        key_t key{};
        size_t file_size{};
        uint8_t separator[16] = "FILE SEPARATOR#";
    };

    struct output_file
    {
        file_footer header{};
        std::vector<uint8_t> binaries{};
    };

    class value_table
    {
        uint32_t _unique_id;
        size_t _current_offset{0};
        fs::file_descriptor _fd;

        value_table() = default;
        value_table(uint32_t unique_id, size_t current_offset, fs::file_descriptor file_descriptor)
            : _unique_id(unique_id), _current_offset(current_offset), _fd(std::move(file_descriptor)) {}

    public:
        static constexpr std::string_view TABLE_FILE_EXTENSION = ".vt";
        static constexpr size_t TABLE_MAX_SIZE_BYTES = std::numeric_limits<uint32_t>::max();
        static constexpr size_t TABLE_MAX_ID = std::numeric_limits<uint32_t>::max();
        static constexpr size_t MAX_FILE_SIZE = (((1UL << 17) - 1) * PAGE_SIZE_IN_BYTES) - sizeof(file_footer);

        [[nodiscard]] uint32_t id() const
        {
            return this->_unique_id;
        }

        [[nodiscard]] size_t current_offset() const
        {
            return this->_current_offset;
        }

        [[nodiscard]] const fs::file_descriptor& fd() const
        {
            return this->_fd;
        }

        [[nodiscard]] size_t free_space() const
        {
            return value_table::TABLE_MAX_SIZE_BYTES - this->_current_offset;
        }

        struct write_reservation
        {
            size_t offset{};
        };

        expected<write_reservation> get_write_reservation(size_t file_size);
        async::task<expected<hedgehog::value_ptr_t>> write_async(key_t key, const std::vector<uint8_t>& value, const write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor);
        async::task<expected<output_file>> read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor);

        static hedgehog::expected<value_table> make_new(const std::filesystem::path& base_path, uint32_t table_id);
        static hedgehog::expected<value_table> load(const std::filesystem::path& path, fs::file_descriptor::open_mode open_mode);
        static hedgehog::expected<value_table> load(const std::filesystem::path& base_path, uint32_t table_id, fs::file_descriptor::open_mode open_mode);
    };
} // namespace hedgehog::db