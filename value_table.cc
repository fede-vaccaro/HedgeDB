#include <cstdint>
#include <cstring>
#include <error.hpp>
#include <filesystem>

#include "common.h"
#include "io_executor.h"
#include "mailbox_impl.h"
#include "value_table.h"

namespace hedgehog::db
{
    expected<value_table::write_reservation> value_table::get_write_reservation(size_t file_size)
    {
        if(file_size > value_table::MAX_FILE_SIZE)
            return hedgehog::error(std::format("Requested file size ({}) is larger than max allowed ({})", file_size, value_table::MAX_FILE_SIZE));

        auto file_w_header_size = file_size + sizeof(file_header);

        if(this->_current_offset + file_w_header_size > value_table::TABLE_MAX_SIZE_BYTES)
            return hedgehog::error(
                std::format("Adding this file (of size: {}) to this table will exceed maximum table size ({} > {})",
                            file_w_header_size,
                            this->_current_offset + file_w_header_size,
                            value_table::TABLE_MAX_SIZE_BYTES),
                errc::VALUE_TABLE_NOT_ENOUGH_SPACE);

        auto ret_offset = this->_current_offset;

        this->_current_offset += file_w_header_size;

        return value_table::write_reservation{ret_offset};
    }

    async::task<expected<hedgehog::value_ptr_t>> value_table::write_async(key_t key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor)
    {
        auto header = file_header{
            .separator = FILE_SEPARATOR,
            .key = key,
            .file_size = value.size(),
            .deleted_marker = false};

        // todo: the entire file should be written in one go

        std::vector<uint8_t> value_with_header;
        value_with_header.reserve(value.size() + sizeof(file_header) + sizeof(EOF_MARKER));

        value_with_header.insert(value_with_header.end(), reinterpret_cast<uint8_t*>(&header), reinterpret_cast<uint8_t*>(&header) + sizeof(file_header));
        value_with_header.insert(value_with_header.end(), value.begin(), value.end());
        value_with_header.insert(value_with_header.end(), EOF_MARKER.begin(), EOF_MARKER.end());

        auto write_value_response = co_await executor->submit_request(async::write_request{
            .fd = this->_fd.get(),
            .data = const_cast<uint8_t*>(value_with_header.data()),
            .size = value_with_header.size(),
            .offset = reservation.offset,
        });

        if(write_value_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to write value to value table (path: {}): {}", this->_fd.path().string(), strerror(-write_value_response.error_code)));

        if(write_value_response.bytes_written != value_with_header.size())
            co_return hedgehog::error(std::format("Failed to write value to value table (path: {}): expected {}, got {}", this->_fd.path().string(), value_with_header.size(), write_value_response.bytes_written));

        co_return hedgehog::value_ptr_t{
            .offset = reservation.offset,
            .size = static_cast<uint32_t>(value.size() + sizeof(file_header)),
            .table_id = this->_unique_id,
        };
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor)
    {
        if(file_offset + file_size > this->_current_offset)
            co_return hedgehog::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                                  file_offset,
                                                  file_size,
                                                  this->_current_offset));

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = file_offset,
            .size = file_size,
        });

        if(read_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-read_response.error_code)));

        if(read_response.bytes_read != file_size)
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  file_size,
                                                  read_response.bytes_read));

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(header.deleted_marker)
            co_return hedgehog::error(std::format("File with key '{}' is marked as deleted in value table (path: {}) at offset {}",
                                                  uuids::to_string(header.key),
                                                  this->_fd.path().string(),
                                                  file_offset),
                                      hedgehog::errc::DELETED);

        if(header.separator != FILE_SEPARATOR)
            co_return hedgehog::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                                  this->_fd.path().string(),
                                                  file_offset));

        if(header.file_size != file_size - sizeof(file_header))
            co_return hedgehog::error(std::format("Invalid file size in value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  header.file_size,
                                                  file_size - sizeof(file_header)));

        read_response.data.erase(read_response.data.begin(), read_response.data.begin() + sizeof(file_header));

        co_return output_file{
            .header = header,
            .binaries = std::move(read_response.data),
        };
    }

    hedgehog::expected<value_table> value_table::make_new(const std::filesystem::path& base_path, uint32_t id)
    {
        auto file_path = base_path / std::to_string(id);
        file_path = with_extension(file_path, TABLE_FILE_EXTENSION);

        if(std::filesystem::exists(file_path))
            return hedgehog::error("File already exists: " + file_path.string());

        auto file_desc = fs::file_descriptor::from_path(file_path, fs::file_descriptor::open_mode::read_write_new, false, value_table::TABLE_MAX_SIZE_BYTES);

        if(!file_desc)
            return hedgehog::error("Failed to create file descriptor: " + file_desc.error().to_string());

        return value_table{
            id,
            0,
            std::move(file_desc.value()),
        };
    }

    hedgehog::expected<value_table> value_table::load(const std::filesystem::path& path, fs::file_descriptor::open_mode open_mode, std::optional<size_t> offset)
    {
        if(!std::filesystem::exists(path))
            return hedgehog::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file_descriptor::from_path(path, open_mode, false);

        if(!file_descriptor)
            return hedgehog::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        if(offset.has_value() && offset.value() >= value_table::TABLE_MAX_SIZE_BYTES)
            return hedgehog::error("Invalid offset: " + std::to_string(*offset));

        return value_table{
            static_cast<uint32_t>(table_id),
            offset.value_or(file_descriptor.value().file_size()),
            std::move(file_descriptor.value())};
    }

    async::task<status> value_table::delete_async(key_t key, size_t offset, const std::shared_ptr<async::executor_context>& executor)
    {
        if(offset + sizeof(file_header) > this->_current_offset)
            co_return hedgehog::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                                  offset,
                                                  sizeof(file_header),
                                                  this->_current_offset));

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = offset,
            .size = sizeof(file_header),
        });

        if(read_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to read file header from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-read_response.error_code)));

        if(read_response.bytes_read != sizeof(file_header))
            co_return hedgehog::error(std::format("Failed to read file header from value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  sizeof(file_header),
                                                  read_response.bytes_read));

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(header.separator != FILE_SEPARATOR)
            co_return hedgehog::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                                  this->_fd.path().string(),
                                                  offset));

        if(header.key != key)
            co_return hedgehog::error(std::format("Key mismatch on delete: expected {}, got {}", uuids::to_string(key), uuids::to_string(header.key)));

        if(header.deleted_marker)
            co_return hedgehog::ok();

        header.deleted_marker = true;

        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->_fd.get(),
            .data = reinterpret_cast<uint8_t*>(&header),
            .size = sizeof(file_header),
            .offset = offset,
        });

        if(write_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to write delete marker to value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-write_response.error_code)));

        if(write_response.bytes_written != sizeof(file_header))
            co_return hedgehog::error(std::format("Failed to write delete marker to value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  sizeof(file_header),
                                                  write_response.bytes_written));

        co_return hedgehog::ok();
    }

} // namespace hedgehog::db