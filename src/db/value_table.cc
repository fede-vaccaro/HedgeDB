#include <atomic>
#include <bits/types/struct_iovec.h>
#include <cstdint>
#include <cstring>
#include <error.hpp>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"

#include "db/value_table.h"
#include "fs/fs.hpp"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    expected<size_t> value_table::allocate_pages_for_write(size_t num_bytes)
    {
        if(num_bytes % PAGE_SIZE_IN_BYTES != 0)
            return hedge::error("File size must be multiple of page size (" + std::to_string(PAGE_SIZE_IN_BYTES) + ")");

        if(num_bytes > value_table::MAX_FILE_SIZE)
            return hedge::error(std::format("Requested file size ({}) is larger than max allowed ({})", num_bytes, value_table::MAX_FILE_SIZE));

        size_t offset = this->_current_offset.load(std::memory_order::relaxed);
        size_t next_offset;

        do
        {
            next_offset = offset + num_bytes;
            if(next_offset > value_table::TABLE_MAX_SIZE_BYTES)
            {
                return hedge::error(
                    std::format("Adding this file (of size: {}) to this table will exceed maximum table size ({} > {})",
                                num_bytes,
                                offset + num_bytes,
                                value_table::TABLE_MAX_SIZE_BYTES),
                    errc::VALUE_TABLE_NOT_ENOUGH_SPACE); // This error code signals that the caller needs to create a new table.
            }
        } while(!this->_current_offset.compare_exchange_strong(offset, next_offset));

        return offset;
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::reload(value_table&& other, fs::file::open_mode open_mode, bool use_direct)
    {
        auto new_file = fs::file::from_path(
            other.path(),
            open_mode,
            use_direct);

        if(!new_file)
            return hedge::error("Failed to reopen file descriptor: " + new_file.error().to_string());

        return std::shared_ptr<value_table>(new value_table{
            other._unique_id,
            other._current_offset.load(std::memory_order::relaxed),
            std::move(new_file.value()),
        });
    }

    async::task<expected<hedge::value_ptr_t>> value_table::write_async(key_t key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor)
    {
        auto header = file_header{
            .separator = FILE_SEPARATOR,
            .key = key,
            .file_size = static_cast<uint32_t>(value.size()),
        };

        // TODO: Optimize by writing header and value in separate io_uring requests if beneficial,
        // or ensure the current single write is efficient enough.
        // TODO: test with mmapped buffers.

        if(this->_mmap.has_value())
        {
            auto* ptr = reinterpret_cast<uint8_t*>(this->_mmap->get_ptr());
            std::memcpy(ptr + reservation.offset, &header, sizeof(file_header));
            std::memcpy(ptr + reservation.offset + sizeof(file_header), value.data(), value.size());

            msync(ptr + reservation.offset, value.size() + sizeof(file_header), MS_ASYNC);

            co_return hedge::value_ptr_t(
                reservation.offset,
                static_cast<uint32_t>(value.size() + sizeof(file_header)),
                this->_unique_id);
        }

        // value_with_header.insert(value_with_header.end(), EOF_MARKER.begin(), EOF_MARKER.end());

        std::array<iovec, 2> iovecs = {
            iovec{
                .iov_base = reinterpret_cast<void*>(&header),
                .iov_len = sizeof(file_header)},
            iovec{
                .iov_base = const_cast<uint8_t*>(value.data()),
                .iov_len = value.size()}};

        auto write_value_response = co_await executor->submit_request(async::writev_request{
            .fd = this->fd(),
            .iovecs = iovecs.data(),
            .iovecs_count = iovecs.size(),
            .offset = reservation.offset,
        });

        if(write_value_response.error_code != 0)
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): {}", this->path().string(), strerror(-write_value_response.error_code)));

        if(write_value_response.bytes_written != sizeof(file_header) + value.size())
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): expected {}, got {}", this->path().string(), sizeof(file_header) + value.size(), write_value_response.bytes_written));

        co_return hedge::value_ptr_t(
            reservation.offset,
            static_cast<uint32_t>(sizeof(file_header) + value.size()),
            this->_unique_id);
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor, bool)
    {
        if(file_offset + file_size > this->_current_offset)
        {
            co_return hedge::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                               file_offset,
                                               file_size,
                                               this->_current_offset.load(std::memory_order_relaxed)));
        }

        std::vector<uint8_t> value_data;
        file_header header;

        if(this->has_direct_access())
        {
            // normalize offset and size to read full pages

            size_t page_aligned_offset = file_offset;
            size_t remainder = file_offset % 4096;

            if(remainder != 0)
                page_aligned_offset = file_offset - remainder;

            size_t page_aligned_file_size = file_size + remainder;
            size_t page_aligned_size = hedge::ceil(page_aligned_file_size, PAGE_SIZE_IN_BYTES) * PAGE_SIZE_IN_BYTES;

            auto* data_ptr = static_cast<uint8_t*>(aligned_alloc(page_aligned_size, PAGE_SIZE_IN_BYTES));
            if(data_ptr == nullptr)
            {
                co_return hedge::error(std::format("Failed to allocate memory for reading file from value table (path: {}): requested size {}",
                                                   this->path().string(),
                                                   page_aligned_size));
            }

            auto data = std::unique_ptr<uint8_t>(data_ptr);

            auto read_response = co_await executor->submit_request(async::read_request{
                .fd = this->fd(),
                .data = data.get(),
                .offset = page_aligned_offset,
                .size = page_aligned_size,
            });

            if(read_response.error_code != 0)
            {
                co_return hedge::error(std::format("Failed to read file from value table (path: {}): {}",
                                                   this->path().string(),
                                                   strerror(-read_response.error_code)));
            }

            if(read_response.bytes_read != page_aligned_size)
            {
                co_return hedge::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}",
                                                   this->path().string(),
                                                   file_size,
                                                   read_response.bytes_read));
            }

            std::copy(data_ptr + remainder, data_ptr + remainder + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));
            value_data.assign(data_ptr + remainder + sizeof(file_header), data_ptr + page_aligned_file_size);
        }
        else
        {
            size_t actual_value_size = file_size - sizeof(file_header);
            value_data = std::vector<uint8_t>(actual_value_size);

            std::array<iovec, 2> iovecs = {
                iovec{
                    .iov_base = static_cast<void*>(&header),
                    .iov_len = sizeof(file_header)},
                iovec{
                    .iov_base = static_cast<void*>(value_data.data()),
                    .iov_len = actual_value_size}};

            auto read_response = co_await executor->submit_request(async::unaligned_readv_request{
                .fd = this->fd(),
                .iovecs = iovecs.data(),
                .iovecs_count = iovecs.size(),
                .offset = file_offset,
            });

            if(read_response.error_code != 0)
            {
                co_return hedge::error(std::format("Failed to read file from value table (path: {}): {}",
                                                   this->path().string(),
                                                   strerror(-read_response.error_code)));
            }

            if(read_response.bytes_read != file_size)
            {
                co_return hedge::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}",
                                                   this->path().string(),
                                                   file_size,
                                                   read_response.bytes_read));
            }
        }

        if(header.separator != FILE_SEPARATOR)
        {
            co_return hedge::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                               this->path().string(),
                                               file_offset));
        }

        if(header.file_size != file_size - sizeof(file_header))
        {
            co_return hedge::error(std::format("Invalid file size in value table (path: {}): expected {}, got {}",
                                               this->path().string(),
                                               header.file_size,
                                               file_size - sizeof(file_header)));
        }

        // Remove the header bytes from the data vector

        co_return output_file{
            .header = header,
            .binaries = std::move(value_data),
        };
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::make_new(const std::filesystem::path& base_path, uint32_t id, bool preallocate)
    {
        auto file_path = base_path / std::to_string(id);
        file_path = with_extension(file_path, TABLE_FILE_EXTENSION);

        if(std::filesystem::exists(file_path))
            return hedge::error("File already exists: " + file_path.string());

        auto file_desc = fs::file::from_path(file_path,
                                             fs::file::open_mode::read_write_new,
                                             true,
                                             preallocate ? std::optional{value_table::TABLE_ACTUAL_MAX_SIZE} : std::nullopt);

        if(!file_desc)
            return hedge::error("Failed to create file descriptor: " + file_desc.error().to_string());

        if(file_desc.value().has_direct_access())
            posix_fadvise(file_desc.value().fd(), 0, 0, POSIX_FADV_NOREUSE);

        auto vt = std::shared_ptr<value_table>(new value_table{
            id,
            0,
            std::move(file_desc.value())});

        auto maybe_mmap_view = fs::mmap_view::from_file(*vt);
        // if(!maybe_mmap_view)
        // return hedge::error("Failed to mmap value table file: " + maybe_mmap_view.error().to_string());

        // vt->_mmap = std::move(maybe_mmap_view.value());
        return vt;
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::load(const std::filesystem::path& path, fs::file::open_mode open_mode, bool use_direct)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file::from_path(path, open_mode, use_direct);

        if(!file_descriptor)
            return hedge::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        return std::shared_ptr<value_table>(new value_table{
            static_cast<uint32_t>(table_id),
            0,
            std::move(file_descriptor.value())});
    }

    async::task<status> write_buffer::flush(const std::shared_ptr<async::executor_context>&)
    {
        // TODO: Test write with io_uring
        // Although, the other writes should be blocked if a fiber is flushing

        int res = pwrite(
            this->_reference_table->fd(),
            this->_buffer.get(),
            this->_buffer_capacity,
            this->_file_offset);

        if(res < 0)
            co_return hedge::error(std::format("Failed to flush write buffer to value table (path: {}): {}",
                                               this->_reference_table->path().string(),
                                               strerror(-res)));

        if(res != static_cast<int>(this->_buffer_capacity))
            co_return hedge::error(std::format("Failed to flush write buffer to value table (path: {}): expected {}, got {}",
                                               this->_reference_table->path().string(),
                                               this->_buffer_capacity,
                                               res));

        std::lock_guard lk(this->_flush_mutex);
        this->_file_offset = std::numeric_limits<size_t>::max();
        this->_reference_table = nullptr;

        co_return hedge::ok();
    }

    expected<value_ptr_t> write_buffer::write(const key_t& key, const std::vector<uint8_t>& value)
    {
        size_t total_size = sizeof(file_header) + value.size();

        // TODO: If the data spans across two pages (but can fit in one), add some padding
        auto cur_offset = this->_write_buffer_head.load(std::memory_order::relaxed);

        size_t padding = 0;
        size_t next_offset = cur_offset + sizeof(file_header) + value.size();
        size_t offset_page = cur_offset / PAGE_SIZE_IN_BYTES;
        size_t next_offset_page = next_offset / PAGE_SIZE_IN_BYTES;

        size_t max_needed_pages_for_file = hedge::ceil(sizeof(file_header) + value.size(), PAGE_SIZE_IN_BYTES);
        if(next_offset_page - offset_page >= max_needed_pages_for_file)
        {
            // Align the current offset (where the file is being written) to the following page
            padding = PAGE_SIZE_IN_BYTES - (cur_offset % PAGE_SIZE_IN_BYTES);
            next_offset += padding;
        }

        if(cur_offset + padding + total_size > this->_buffer_capacity)
            return hedge::error("buffer full", hedge::errc::BUFFER_FULL); // Tells that flush is needed

        auto* header_ptr = reinterpret_cast<file_header*>(this->_buffer.get() + cur_offset + padding);
        header_ptr->separator = FILE_SEPARATOR;
        header_ptr->key = key;
        header_ptr->file_size = static_cast<uint32_t>(value.size());

        std::copy(value.data(), value.data() + value.size(), this->_buffer.get() + cur_offset + padding + sizeof(file_header));

        size_t write_offset = this->_file_offset + cur_offset + padding;
        this->_write_buffer_head.fetch_add(padding + total_size, std::memory_order::release);

        return value_ptr_t(
            write_offset,
            static_cast<uint32_t>(total_size),
            this->_reference_table->id());
    }

    expected<output_file> write_buffer::try_read(value_ptr_t value_ptr)
    {
        std::shared_lock lk(this->_flush_mutex);

        if(this->_reference_table == nullptr || this->_reference_table->id() != value_ptr.table_id())
            return hedge::error("wrong_id", errc::KEY_NOT_FOUND);

        if(value_ptr.offset() < this->_file_offset || value_ptr.offset() + value_ptr.size() > this->_file_offset + this->_write_buffer_head.load(std::memory_order::acquire))
            return hedge::error("oob", errc::KEY_NOT_FOUND); // out of bounds

        size_t rescaled_offset = value_ptr.offset() - this->_file_offset;

        file_header header;
        std::copy(this->_buffer.get() + rescaled_offset,
                  this->_buffer.get() + rescaled_offset + sizeof(file_header),
                  reinterpret_cast<uint8_t*>(&header));

        if(header.file_size + sizeof(file_header) != value_ptr.size())
            return hedge::error("File size in header does not match value pointer size");

        if(header.separator != FILE_SEPARATOR)
            return hedge::error("Invalid file header separator in write buffer");

        std::vector<uint8_t> value_data(header.file_size);
        std::copy(this->_buffer.get() + rescaled_offset + sizeof(file_header),
                  this->_buffer.get() + rescaled_offset + sizeof(file_header) + header.file_size,
                  value_data.data());

        return output_file{.header = header, .binaries = std::move(value_data)};
    }

} // namespace hedge::db