#include <cstdint>
#include <cstring>
#include <error.hpp>
#include <filesystem>
#include <limits>

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
        {
            return hedgehog::error(
                std::format("Adding this file (of size: {}) to this table will exceed maximum table size ({} > {})",
                            file_w_header_size,
                            this->_current_offset + file_w_header_size,
                            value_table::TABLE_MAX_SIZE_BYTES),
                errc::VALUE_TABLE_NOT_ENOUGH_SPACE);
        }

        auto ret_offset = this->_current_offset;

        this->_current_offset += file_w_header_size;

        // update infos
        auto& info = this->_info();
        info.current_offset = this->_current_offset;

        return value_table::write_reservation{ret_offset};
    }

    async::task<expected<hedgehog::value_ptr_t>> value_table::write_async(key_t key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor)
    {
        auto header = file_header{
            .separator = FILE_SEPARATOR,
            .key = key,
            .file_size = value.size(),
            .deleted_flag = false};

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

        // update infos
        auto& info = this->_info();

        info.items_count++;
        info.occupied_space += value.size() + sizeof(file_header);

        co_return hedgehog::value_ptr_t(
            reservation.offset,
            static_cast<uint32_t>(value.size() + sizeof(file_header)),
            this->_unique_id
        );
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor, bool skip_delete_check)
    {
        if(file_offset + file_size > this->_current_offset)
        {
            co_return hedgehog::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                                  file_offset,
                                                  file_size,
                                                  this->_current_offset));
        }

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = file_offset,
            .size = file_size,
        });

        if(read_response.error_code != 0)
        {
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-read_response.error_code)));
        }

        if(read_response.bytes_read != file_size)
        {
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  file_size,
                                                  read_response.bytes_read));
        }

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(!skip_delete_check && header.deleted_flag)
        {
            co_return hedgehog::error(std::format("File with key '{}' is marked as deleted in value table (path: {}) at offset {}",
                                                  uuids::to_string(header.key),
                                                  this->_fd.path().string(),
                                                  file_offset),
                                      hedgehog::errc::DELETED);
        }

        if(header.separator != FILE_SEPARATOR)
        {
            co_return hedgehog::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                                  this->_fd.path().string(),
                                                  file_offset));
        }

        if(header.file_size != file_size - sizeof(file_header))
        {
            co_return hedgehog::error(std::format("Invalid file size in value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  header.file_size,
                                                  file_size - sizeof(file_header)));
        }

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

        auto file_desc = fs::file_descriptor::from_path(file_path,
                                                        fs::file_descriptor::open_mode::read_write_new,
                                                        false,
                                                        value_table::TABLE_ACTUAL_MAX_SIZE);

        if(!file_desc)
            return hedgehog::error("Failed to create file descriptor: " + file_desc.error().to_string());

        auto tmp_mmap = fs::tmp_mmap::from_fd_wrapper(
            file_desc.value(),
            value_table::_page_align_for_mmap());

        if(!tmp_mmap.has_value())
            return hedgehog::error("Failed to create temporary mmap: " + tmp_mmap.error().to_string());

        return value_table{
            id,
            0,
            std::move(file_desc.value()),
            std::move(tmp_mmap.value())};
    }

    hedgehog::expected<value_table> value_table::load(const std::filesystem::path& path, fs::file_descriptor::open_mode open_mode)
    {
        if(!std::filesystem::exists(path))
            return hedgehog::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file_descriptor::from_path(path, open_mode, false);

        if(!file_descriptor)
            return hedgehog::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        auto tmp_mmap = fs::tmp_mmap::from_fd_wrapper(
            file_descriptor.value(),
            value_table::_page_align_for_mmap());

        auto info = value_table::_get_info_from_mmap(tmp_mmap.value());

        return value_table{
            static_cast<uint32_t>(table_id),
            info.current_offset,
            std::move(file_descriptor.value()),
            std::move(tmp_mmap.value())};
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
        {
            co_return hedgehog::error(std::format("Failed to read file header from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-read_response.error_code)));
        }

        if(read_response.bytes_read != sizeof(file_header))
        {
            co_return hedgehog::error(std::format("Failed to read file header from value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  sizeof(file_header),
                                                  read_response.bytes_read));
        }

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(header.separator != FILE_SEPARATOR)
        {
            co_return hedgehog::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                                  this->_fd.path().string(),
                                                  offset));
        }

        if(header.key != key)
        {
            co_return hedgehog::error(
                std::format("Key mismatch on delete: expected {}, got {}",
                            uuids::to_string(key),
                            uuids::to_string(header.key)));
        }

        if(header.deleted_flag)
            co_return hedgehog::ok();

        header.deleted_flag = true;

        // update infos
        auto& info = this->_info();

        info.deleted_count++;
        info.freed_space += sizeof(file_header) + header.file_size;

        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->_fd.get(),
            .data = reinterpret_cast<uint8_t*>(&header),
            .size = sizeof(file_header),
            .offset = offset,
        });

        if(write_response.error_code != 0)
        {
            co_return hedgehog::error(std::format("Failed to write delete marker to value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-write_response.error_code)));
        }

        if(write_response.bytes_written != sizeof(file_header))
        {
            co_return hedgehog::error(std::format("Failed to write delete marker to value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  sizeof(file_header),
                                                  write_response.bytes_written));
        }
        co_return hedgehog::ok();
    }

    value_table_info& value_table::_info()
    {
        return value_table::_get_info_from_mmap(this->_mmap);
    }

    value_table_info value_table::info() const
    {
        return value_table::_get_info_from_mmap(this->_mmap);
    }

    value_table_info& value_table::_get_info_from_mmap(const fs::tmp_mmap& mmap)
    {
        auto* ptr_start = (uint8_t*)mmap.get_ptr();
        auto* ptr_end = ptr_start + mmap.size();

        auto* info_ptr = reinterpret_cast<value_table_info*>(ptr_end - sizeof(value_table_info));
        return *info_ptr;
    }

    async::task<expected<std::pair<output_file, value_table::next_offset_and_size_t>>> value_table::read_file_and_next_header_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor)
    {
        auto read_result = co_await this->read_async(file_offset, file_size, executor, true);

        if(!read_result.has_value())
            co_return read_result.error();

        auto next_header_offset = file_offset + sizeof(file_header) + read_result.value().header.file_size;

        auto get_next_header = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = next_header_offset,
            .size = sizeof(file_header),
        });

        if(get_next_header.error_code != 0)
        {
            co_return hedgehog::error(std::format("Failed to read next file header from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-get_next_header.error_code)));
        }

        if(get_next_header.bytes_read != sizeof(file_header))
        {
            co_return hedgehog::error(std::format("Failed to read next file header from value table (path: {}): expected {}, got {}",
                                                  this->_fd.path().string(),
                                                  sizeof(file_header),
                                                  get_next_header.bytes_read));
        }

        auto next_header = file_header{};
        std::copy(get_next_header.data.data(), get_next_header.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&next_header));

        if(next_header.separator == EOF_MARKER)
        {
            co_return std::pair(
                std::move(read_result.value()),
                value_table::next_offset_and_size_t{std::numeric_limits<size_t>::max(), 0});
        }

        if(next_header.separator != FILE_SEPARATOR)
        {
            co_return hedgehog::error(std::format("Invalid next file header separator in value table (path: {}) at offset {}",
                                                  this->_fd.path().string(),
                                                  next_header_offset));
        }

        co_return std::pair(
            std::move(read_result.value()),
            value_table::next_offset_and_size_t{
                next_header_offset,
                next_header.file_size + sizeof(file_header)});
    }

    async::task<expected<file_header>> value_table::get_first_header_async(const std::shared_ptr<async::executor_context>& executor)
    {
        auto get_first_header = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = 0,
            .size = sizeof(file_header),
        });

        if(get_first_header.error_code != 0)
        {
            co_return hedgehog::error(std::format("Failed to read next file header from value table (path: {}): {}",
                                                  this->_fd.path().string(),
                                                  strerror(-get_first_header.error_code)));
        }

        file_header header;
        std::copy(get_first_header.data.data(),
                  get_first_header.data.data() + sizeof(file_header),
                  reinterpret_cast<uint8_t*>(&get_first_header));

        co_return header;
    }

} // namespace hedgehog::db