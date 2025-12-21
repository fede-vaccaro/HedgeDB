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

    expected<value_table::write_reservation> value_table::get_write_reservation(size_t file_size)
    {
        if(file_size > value_table::MAX_FILE_SIZE)
            return hedge::error(std::format("Requested file size ({}) is larger than max allowed ({})", file_size, value_table::MAX_FILE_SIZE));

        auto file_w_header_size = file_size + sizeof(file_header);

        size_t offset = this->_current_offset.load(std::memory_order::relaxed);

        auto max_needed_pages_for_file = hedge::ceil(file_w_header_size, PAGE_SIZE_IN_BYTES);

        size_t padding = 0;
        size_t next_offset;

        do
        {
            // Check if padding is needed
            next_offset = offset + file_w_header_size;
            size_t offset_page = offset / PAGE_SIZE_IN_BYTES;
            size_t next_offset_page = next_offset / PAGE_SIZE_IN_BYTES;
            padding = 0;

            if(next_offset_page - offset_page >= max_needed_pages_for_file)
            {
                // Align the current offset (where the file is being written) to the following page
                padding = PAGE_SIZE_IN_BYTES - (offset % PAGE_SIZE_IN_BYTES);
                next_offset += padding;
            }

            /*
                Example of padding computing:
                file_size_w_header = 1200;
                max_needed_pages_for_file = 1;
                offset = 3600

                next_offset = 4800
                next_offset_page = 1
                offset_page = 0
                padding = 0

                if(1 - 0 >= 1)
                {
                    padding = 4096 - (3600) = 496
                    next_offset = 4800 + 496 = 5296
                }

                ...

                reservation.offset = 3600 + 496 = 4096 (right at the beginn of page 2)
            */

            if(next_offset > value_table::TABLE_MAX_SIZE_BYTES)
            {
                return hedge::error(
                    std::format("Adding this file (of size: {}) to this table will exceed maximum table size ({} > {})",
                                file_w_header_size,
                                offset + file_w_header_size,
                                value_table::TABLE_MAX_SIZE_BYTES),
                    errc::VALUE_TABLE_NOT_ENOUGH_SPACE); // This error code signals that the caller needs to create a new table.
            }
        } while(!this->_current_offset.compare_exchange_strong(offset, next_offset));

        // Update the table's metadata (persisted via mmap) with the new offset.
        // TODO 1: check if it is actually necessary to store the current offset within the infos
        // It might be cached and - as a fallback mechanism - inferred from EOF marker position on load.
        // auto& info = this->_info();
        // std::atomic_ref<size_t>(info.current_offset).fetch_add(file_w_header_size);

        return value_table::write_reservation{offset + padding};
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::reload(value_table&& other, fs::file::open_mode open_mode, bool use_direct)
    {
        fsync(other.fd());

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
            size_t actual_file_size = file_size - sizeof(file_header);
            value_data = std::vector<uint8_t>(actual_file_size);

            std::array<iovec, 2> iovecs = {
                iovec{
                    .iov_base = reinterpret_cast<uint8_t*>(&header),
                    .iov_len = sizeof(file_header)},
                iovec{
                    .iov_base = value_data.data(),
                    .iov_len = actual_file_size}};

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
                                             false, // Avoid O_DIRECT for value_tables due to non-aligned data being written.
                                             preallocate ? std::optional{value_table::TABLE_ACTUAL_MAX_SIZE} : std::nullopt);

        if(!file_desc)
            return hedge::error("Failed to create file descriptor: " + file_desc.error().to_string());

        posix_fadvise(file_desc.value().fd(), 0, 0, POSIX_FADV_RANDOM);

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

    hedge::expected<std::shared_ptr<value_table>> value_table::load(const std::filesystem::path& path, fs::file::open_mode open_mode)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file::from_path(path, open_mode, false); // Assuming no O_DIRECT for now.

        if(!file_descriptor)
            return hedge::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        return std::shared_ptr<value_table>(new value_table{
            static_cast<uint32_t>(table_id),
            0,
            std::move(file_descriptor.value())});
    }

    async::task<status> value_table::delete_async(key_t key, size_t offset, const std::shared_ptr<async::executor_context>& executor)
    {
        // TODO: fix table locking to avoid a race condition between GC and delete_async
        // Explanation:
        // It might happen the following situation:
        // The Value table GC will operate by iterating over the entire table and rebuilding it
        // in a new file, but skipping deleted entries.
        // If a delete_async is called while the GC is running, we might delete a file that has already
        // been copied to the new table, leading to inconsistencies.
        //
        // NB: keeping the deletion flag here as well will avoid heavy batch of lookups over the index
        // to check if a file is deleted or not.
        // But at this point, we might incur to the issue of being unable to delete items while GC is running.
        // A possible fall-back solution could be to buffer/queue the delete requests and apply them after GC is done.
        // This, however, requires WAL. Which is not available yet.
        // Last but not least, locking in a coroutine might easily lead to deadlocks

        // std::unique_lock<std::mutex> try_lock(*this->_delete_mutex, std::try_to_lock);
        // if(!try_lock.owns_lock())
        // co_return hedge::error("Cannot delete object, garbage collection is running and the table is locked", errc::BUSY);

        if(offset + sizeof(file_header) > this->_current_offset)
            co_return hedge::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                               offset,
                                               sizeof(file_header),
                                               this->_current_offset.load(std::memory_order_relaxed)));

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->fd(),
            .offset = offset,
            .size = sizeof(file_header),
        });

        if(read_response.error_code != 0)
        {
            co_return hedge::error(std::format("Failed to read file header from value table (path: {}): {}",
                                               this->path().string(),
                                               strerror(-read_response.error_code)));
        }

        if(read_response.bytes_read != sizeof(file_header))
        {
            co_return hedge::error(std::format("Failed to read file header from value table (path: {}): expected {}, got {}",
                                               this->path().string(),
                                               sizeof(file_header),
                                               read_response.bytes_read));
        }

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(header.separator != FILE_SEPARATOR)
        {
            co_return hedge::error(std::format("Invalid file header separator in value table (path: {}) at offset {}",
                                               this->path().string(),
                                               offset));
        }

        if(header.key != key)
        {
            co_return hedge::error(
                std::format("Key mismatch on delete: expected {}, got {}",
                            uuids::to_string(key),
                            uuids::to_string(header.key)));
        }

        // if(header.deleted_flag)
        //     co_return hedge::ok();

        // header.deleted_flag = true;

        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->fd(),
            .data = reinterpret_cast<uint8_t*>(&header),
            .size = sizeof(file_header),
            .offset = offset,
        });

        if(write_response.error_code != 0)
        {
            co_return hedge::error(std::format("Failed to write delete marker to value table (path: {}): {}",
                                               this->path().string(),
                                               strerror(-write_response.error_code)));
        }

        if(write_response.bytes_written != sizeof(file_header))
        {
            co_return hedge::error(std::format("Failed to write delete marker to value table (path: {}): expected {}, got {}",
                                               this->path().string(),
                                               sizeof(file_header),
                                               write_response.bytes_written));
        }

        // Deletion successful.
        co_return hedge::ok();
    }

} // namespace hedge::db