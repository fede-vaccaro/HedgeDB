#include <atomic>
#include <bits/types/struct_iovec.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <error.hpp>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <sys/mman.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"

#include "db/value_table.h"
#include "fs/fs.hpp"
#include "key.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    expected<size_t> value_table::allocate_pages_for_write(size_t num_bytes)
    {
        if(num_bytes % PAGE_SIZE_IN_BYTES != 0)
            return hedge::error("File size must be multiple of page size (" + std::to_string(PAGE_SIZE_IN_BYTES) + ")");

        if(num_bytes > value_table::MAX_VALUE_SIZE)
            return hedge::error(std::format("Requested file size ({}) is larger than max allowed ({})", num_bytes, value_table::MAX_VALUE_SIZE));

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

    async::task<expected<hedge::value_ptr_t>> value_table::write_async(const key_t& key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation)
    {
        const size_t total_size = sizeof(file_header) + key.size() + value.size();

        if(this->has_direct_access())
        {
            const size_t aligned_size = hedge::ceil_page_align(total_size);
            auto buffer = hedge::make_aligned_buffer(aligned_size);
            if(!buffer)
                co_return hedge::error("Failed to allocate aligned buffer");

            // zero out padding
            std::memset(buffer.get(), 0, aligned_size);

            // construct header
            auto header = file_header{
                .separator = FILE_SEPARATOR,
                .key_size = static_cast<uint16_t>(key.size()),
                .value_size = static_cast<uint32_t>(value.size()),
            };

            uint8_t* ptr = buffer.get();
            std::memcpy(ptr, &header, sizeof(header));
            ptr += sizeof(header);
            std::memcpy(ptr, key.data(), key.size());
            ptr += key.size();
            std::memcpy(ptr, value.data(), value.size());

            // use writev with single iovec for aligned buffer
            iovec iov{
                .iov_base = buffer.get(),
                .iov_len = aligned_size};

            auto write_res = co_await async::this_thread_executor()->submit_request(async::writev_request{
                .fd = this->fd(),
                .iovecs = &iov,
                .iovecs_count = 1,
                .offset = reservation.offset,
            });

            if(write_res.error_code != 0)
                co_return hedge::error(std::format("Failed to write value to value table (path: {}): {}", this->path().string(), strerror(-write_res.error_code)));

            if(write_res.bytes_written != aligned_size)
                co_return hedge::error(std::format("Failed to write value to value table (path: {}): expected {}, got {}", this->path().string(), aligned_size, write_res.bytes_written));

            co_return hedge::value_ptr_t(
                reservation.offset,
                static_cast<uint32_t>(total_size),
                this->_unique_id);
        }

        auto header = file_header{
            .separator = FILE_SEPARATOR,
            .key_size = static_cast<uint16_t>(key.size()),
            .value_size = static_cast<uint32_t>(value.size()),
        };

        std::array<iovec, 3> iovecs = {
            iovec{
                .iov_base = static_cast<void*>(&header),
                .iov_len = sizeof(file_header)},
            iovec{
                .iov_base = const_cast<uint8_t*>(key.data()),
                .iov_len = key.size()},
            iovec{
                .iov_base = const_cast<uint8_t*>(value.data()),
                .iov_len = value.size()}};

        auto write_value_response = co_await async::this_thread_executor()->submit_request(async::writev_request{
            .fd = this->fd(),
            .iovecs = iovecs.data(),
            .iovecs_count = iovecs.size(),
            .offset = reservation.offset,
        });

        if(write_value_response.error_code != 0)
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): {}", this->path().string(), strerror(-write_value_response.error_code)));

        if(write_value_response.bytes_written != total_size)
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): expected {}, got {}", this->path().string(), total_size, write_value_response.bytes_written));

        co_return hedge::value_ptr_t(
            reservation.offset,
            static_cast<uint32_t>(total_size),
            this->_unique_id);
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t total_file_size, bool /* skip_delete_check */)
    {
        if(file_offset + total_file_size > this->_current_offset)
        {
            co_return hedge::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                               file_offset,
                                               total_file_size,
                                               this->_current_offset.load(std::memory_order_relaxed)));
        }

        file_header header;
        key_t key;
        std::vector<uint8_t> key_value_buffer;

        if(this->has_direct_access())
        {
            // W.r.t. the entire file
            const size_t page_offset = hedge::floor_page_align(file_offset);

            // Key-value w.r.t the loaded page(s)
            const size_t in_page_offset = file_offset % PAGE_SIZE_IN_BYTES;

            // N.B. Padding is applied on write phase to always have page-aligned data
            const size_t page_aligned_size = hedge::ceil_page_align(total_file_size + in_page_offset);

            auto* data_ptr = static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE_IN_BYTES, page_aligned_size));
            if(data_ptr == nullptr)
            {
                co_return hedge::error(std::format(
                    "Failed to allocate memory for reading file from value table (path: {}): requested size {}",
                    this->path().string(),
                    page_aligned_size));
            }

            auto data = buffer_t(data_ptr, std::free);

            auto read_response = co_await async::this_thread_executor()->submit_request(async::read_request{
                .fd = this->fd(),
                .data = data.get(),
                .off = page_offset,
                .len = page_aligned_size,
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
                                                   total_file_size,
                                                   read_response.bytes_read));
            }

            // Todo: Reader class for consuming bytes from a buffer
            // Shift offset to skip the data loaded just for alignment purposes
            data_ptr += in_page_offset;

            auto read_from_buf = [&data_ptr](auto* dst, size_t n)
            {
                std::memcpy(static_cast<void*>(dst), data_ptr, n);
                data_ptr += n;
            };

            read_from_buf(reinterpret_cast<uint8_t*>(&header), sizeof(header));

            if(total_file_size != sizeof(header) + header.key_size + header.value_size)
            {
                co_return hedge::error(std::format("Total file size ({}) does not match expected size from header ({}) for value table (path: {})",
                                                   total_file_size,
                                                   sizeof(header) + header.key_size + header.value_size,
                                                   this->path().string()));
            }

            key = key_t::make_with_length(header.key_size);
            read_from_buf(key.data(), key.size());

            // Read actual value
            key_value_buffer.assign(data_ptr, data_ptr + header.value_size);
        }
        else
        {
            const size_t key_value_size = total_file_size - sizeof(header);
            key_value_buffer = std::vector<uint8_t>(key_value_size);

            std::array<iovec, 2> iovecs = {
                iovec{
                    .iov_base = static_cast<void*>(&header),
                    .iov_len = sizeof(file_header)},
                iovec{
                    .iov_base = static_cast<void*>(key_value_buffer.data()),
                    .iov_len = key_value_size}};

            auto read_response = co_await async::this_thread_executor()->submit_request(async::unaligned_readv_request{
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

            if(read_response.bytes_read != total_file_size)
            {
                co_return hedge::error(std::format("Failed to read file from value table (path: {}): expected {}, got {}",
                                                   this->path().string(),
                                                   total_file_size,
                                                   read_response.bytes_read));
            }

            if(total_file_size != sizeof(header) + header.key_size + header.value_size)
            {
                co_return hedge::error(std::format("Total file size ({}) does not match expected size from header ({}) for value table (path: {})",
                                                   total_file_size,
                                                   sizeof(header) + header.key_size + header.value_size,
                                                   this->path().string()));
            }

            auto key_begin_it = key_value_buffer.begin();
            auto key_end_it = key_begin_it + header.key_size;

            key = key_t(std::span(key_begin_it, key_end_it));
            key_value_buffer.erase(key_begin_it, key_end_it);
        }

        if(header.separator != FILE_SEPARATOR)
        {
            co_return hedge::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                               this->path().string(),
                                               file_offset));
        }

        // Remove the header bytes from the data vector
        co_return output_file{
            .key = std::move(key),
            .binaries = std::move(key_value_buffer),
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
            posix_fadvise(file_desc.value().fd(), 0, 0, POSIX_FADV_DONTNEED);

        auto vt = std::shared_ptr<value_table>(new value_table{
            id,
            0,
            std::move(file_desc.value())});

        // auto maybe_mmap_view = fs::mmap_view::from_file(*vt);
        // if(!maybe_mmap_view)
        // return hedge::error("Failed to mmap value table file: " + maybe_mmap_view.error().to_string());

        // vt->_mmap = std::move(maybe_mmap_view.value());
        return vt;
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::load(const std::filesystem::path& path, fs::file::open_mode open_mode, bool use_direct)
    {
        auto file_desc = fs::file::from_path(path, open_mode, use_direct);
        if(!file_desc)
            return hedge::error("Failed to open value table '" + path.string() + "': " + file_desc.error().to_string());

        uint32_t id = static_cast<uint32_t>(std::stoul(path.stem().string()));

        // Use TABLE_MAX_SIZE_BYTES as the write cursor so every valid read offset passes the bounds check.
        return std::shared_ptr<value_table>(new value_table{id, TABLE_MAX_SIZE_BYTES, std::move(file_desc.value())});
    }

    async::task<status> thread_write_buffer::flush(const std::shared_ptr<async::executor_context>& /* executor */)
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

    expected<value_ptr_t> thread_write_buffer::write(const key_t& key, const std::vector<uint8_t>& value)
    {
        const size_t total_file_size = sizeof(file_header) + key.size() + value.size();
        size_t alignment_padding = 0;

        auto curr_relative_offset = this->_write_buffer_head.load(std::memory_order::relaxed);

        size_t next_offset = curr_relative_offset + total_file_size;
        size_t offset_page = curr_relative_offset / PAGE_SIZE_IN_BYTES;
        size_t next_offset_page = next_offset / PAGE_SIZE_IN_BYTES;
        size_t max_needed_pages_for_file = hedge::ceil(total_file_size, PAGE_SIZE_IN_BYTES);

        bool spans_across_more_page_than_needed = (next_offset_page - offset_page >= max_needed_pages_for_file);
        if(spans_across_more_page_than_needed)
        {
            // Align the current offset (where the file is being written) to the following page
            alignment_padding = hedge::ceil_page_align(curr_relative_offset) - curr_relative_offset;
        }

        if(curr_relative_offset + alignment_padding + total_file_size > this->_buffer_capacity)
            return hedge::error("buffer full", hedge::errc::BUFFER_FULL); // Tells that flush is needed

        uint8_t* buffer_ptr = this->_buffer.get() + curr_relative_offset + alignment_padding;

        auto write_to_buf = [&buffer_ptr](const auto* src, size_t n)
        {
            std::memcpy(buffer_ptr, reinterpret_cast<const uint8_t*>(src), n);
            buffer_ptr += n;
        };

        file_header header{
            .separator = FILE_SEPARATOR,
            .key_size = static_cast<uint16_t>(key.size()),
            .value_size = static_cast<uint32_t>(value.size()),
        };

        write_to_buf(&header, sizeof(header));
        write_to_buf(key.data(), key.size());
        write_to_buf(value.data(), value.size());

        size_t global_file_offset = this->_file_offset + curr_relative_offset + alignment_padding;
        this->_write_buffer_head.fetch_add(alignment_padding + total_file_size, std::memory_order::release);

        return value_ptr_t(
            global_file_offset,
            static_cast<uint32_t>(total_file_size),
            this->_reference_table->id());
    }

    expected<output_file> thread_write_buffer::try_read(value_ptr_t value_ptr)
    {
        std::shared_lock lk(this->_flush_mutex);

        if(this->_reference_table == nullptr ||
           this->_reference_table->id() != value_ptr.table_id())
            return hedge::error("wrong_id", errc::KEY_NOT_FOUND);

        if(value_ptr.offset() < this->_file_offset ||
           value_ptr.offset() + value_ptr.size() > this->_file_offset + this->_write_buffer_head.load(std::memory_order::acquire))
            return hedge::error("oob", errc::KEY_NOT_FOUND); // out of bounds

        size_t rescaled_offset = value_ptr.offset() - this->_file_offset;

        uint8_t* data_ptr = this->_buffer.get() + rescaled_offset;

        auto read_from_buf = [&data_ptr](auto* dst, size_t n)
        {
            std::memcpy(static_cast<void*>(dst), data_ptr, n);
            data_ptr += n;
        };

        file_header header;
        read_from_buf(&header, sizeof(file_header));

        auto key = key_t::make_with_length(header.key_size);
        read_from_buf(key.data(), key.size());

        if(header.separator != FILE_SEPARATOR)
            return hedge::error("Invalid file header separator in write buffer");

        if(value_ptr.size() != sizeof(file_header) + header.key_size + header.value_size)
            return hedge::error("File size in header does not match value pointer size");

        std::vector<uint8_t> value_data(data_ptr,
                                        data_ptr + header.value_size);

        return output_file{.key = std::move(key), .binaries = std::move(value_data)};
    }

} // namespace hedge::db
