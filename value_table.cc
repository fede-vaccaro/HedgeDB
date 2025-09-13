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

        auto file_size_w_header = file_size + sizeof(file_footer);

        if(this->_current_offset + file_size_w_header > value_table::TABLE_MAX_SIZE_BYTES)
            return hedgehog::error(
                std::format("Adding this file (of size: {}) to this table will exceed maximum table size ({} > {})",
                            file_size_w_header,
                            this->_current_offset + file_size_w_header,
                            value_table::TABLE_MAX_SIZE_BYTES),
                errc::VALUE_TABLE_NOT_ENOUGH_SPACE);

        auto ret_offset = this->_current_offset;

        this->_current_offset += file_size_w_header;

        return value_table::write_reservation{ret_offset};
    }

    async::task<expected<hedgehog::value_ptr_t>> value_table::write_async(key_t key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor)
    {
        auto header = file_footer{
            .key = key,
            .file_size = value.size()};

        auto write_value_response = co_await executor->submit_request(async::write_request{
            .fd = this->_fd.get(),
            .data = const_cast<uint8_t*>(value.data()), // eeww
            .size = value.size(),
            .offset = reservation.offset,
        });

        if(write_value_response.bytes_written != value.size())
            co_return hedgehog::error("Failed to write file value to value table: " + this->_fd.path().string());

        auto write_footer_response = co_await executor->submit_request(async::write_request{
            .fd = this->_fd.get(),
            .data = reinterpret_cast<uint8_t*>(&header),
            .size = sizeof(file_footer),
            .offset = reservation.offset + value.size(),
        });

        if(write_footer_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to write file footer to value table (path: {}): {}", this->_fd.path().string(), strerror(-write_footer_response.error_code)));

        if(write_footer_response.bytes_written != sizeof(file_footer))
            co_return hedgehog::error(std::format("Failed to write file footer to value table (path: {}): Footer bytes written does not match expected size", this->_fd.path().string()));

        co_return hedgehog::value_ptr_t{
            .offset = reservation.offset,
            .size = static_cast<uint32_t>(value.size() + sizeof(file_footer)),
            .table_id = this->_unique_id,
        };
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor)
    {
        if(file_offset + file_size > this->_current_offset)
            co_return hedgehog::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})", file_offset, file_size, this->_current_offset));

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->_fd.get(),
            .offset = file_offset,
            .size = file_size,
        });

        if(read_response.error_code != 0)
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): {}", this->_fd.path().string(), strerror(-read_response.error_code)));

        if(read_response.bytes_read != file_size)
            co_return hedgehog::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}", this->_fd.path().string(), file_size, read_response.bytes_read));

        file_footer footer;
        std::memcpy(&footer, read_response.data.data() + read_response.data.size() - sizeof(file_footer), sizeof(file_footer));

        auto separator_view = std::string_view(reinterpret_cast<const char*>(footer.separator), sizeof(footer.separator) - 1);

        if(separator_view != "FILE SEPARATOR#")
            co_return hedgehog::error(std::format("Invalid file footer separator in value table (path: {}): expected 'FILE SEPARATOR#', got '{}'", this->_fd.path().string(), separator_view));

        if(footer.file_size != file_size - sizeof(file_footer))
            co_return hedgehog::error(std::format("Invalid file size in value table (path: {}): expected {}, got {}", this->_fd.path().string(), footer.file_size, file_size - sizeof(file_footer)));

        read_response.data.resize(file_size - sizeof(file_footer));

        co_return output_file{
            .header = footer,
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

    hedgehog::expected<value_table> value_table::load(const std::filesystem::path& path, fs::file_descriptor::open_mode open_mode)
    {
        if(!std::filesystem::exists(path))
            return hedgehog::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file_descriptor::from_path(path, open_mode, false);

        if(!file_descriptor)
            return hedgehog::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        return value_table{
            static_cast<uint32_t>(table_id),
            file_descriptor.value().file_size(),
            std::move(file_descriptor.value())};
    }

    hedgehog::expected<value_table> value_table::load(const std::filesystem::path& base_path, uint32_t table_id, fs::file_descriptor::open_mode open_mode)
    {
        auto file_path = base_path / std::to_string(table_id);
        file_path = with_extension(file_path, TABLE_FILE_EXTENSION);
        return value_table::load(file_path, open_mode);
    }

} // namespace hedgehog::db