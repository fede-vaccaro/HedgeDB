#include <atomic>
#include <cstdint>
#include <cstring>
#include <error.hpp>
#include <filesystem>

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

    async::task<expected<hedge::value_ptr_t>> value_table::write_async(key_t key, const std::vector<uint8_t>& value, const value_table::write_reservation& reservation, const std::shared_ptr<async::executor_context>& executor)
    {
        auto header = file_header{
            .separator = FILE_SEPARATOR,
            .key = key,
            .file_size = value.size(),
            .deleted_flag = false};

        // TODO: Optimize by writing header and value in separate io_uring requests if beneficial,
        // or ensure the current single write is efficient enough.

        std::vector<uint8_t> value_with_header;
        value_with_header.reserve(value.size() + sizeof(file_header));

        value_with_header.insert(value_with_header.end(), reinterpret_cast<uint8_t*>(&header), reinterpret_cast<uint8_t*>(&header) + sizeof(file_header));
        value_with_header.insert(value_with_header.end(), value.begin(), value.end());
        // value_with_header.insert(value_with_header.end(), EOF_MARKER.begin(), EOF_MARKER.end());

        auto write_value_response = co_await executor->submit_request(async::write_request{
            .fd = this->fd(),
            .data = const_cast<uint8_t*>(value_with_header.data()),
            .size = value_with_header.size(),
            .offset = reservation.offset,
        });

        if(write_value_response.error_code != 0)
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): {}", this->path().string(), strerror(-write_value_response.error_code)));

        if(write_value_response.bytes_written != value_with_header.size())
            co_return hedge::error(std::format("Failed to write value to value table (path: {}): expected {}, got {}", this->path().string(), value_with_header.size(), write_value_response.bytes_written));

        // Update the table's persistent metadata (item count, occupied space).
        {
            std::lock_guard lk(this->_info_mutex);
            auto& info = this->_info();
            info.items_count++;
            info.occupied_space += value.size() + sizeof(file_header);
        }

        // this value_ptr will flow first to the mem_index and then - on flush - to the sorted_index.
        co_return hedge::value_ptr_t(
            reservation.offset,
            static_cast<uint32_t>(value.size() + sizeof(file_header)),
            this->_unique_id);
    }

    async::task<expected<output_file>> value_table::read_async(size_t file_offset, size_t file_size, const std::shared_ptr<async::executor_context>& executor, bool skip_delete_check)
    {
        if(file_offset + file_size > this->_current_offset)
        {
            co_return hedge::error(std::format("Requested file offset ({}) + size ({}) exceeds current table offset ({})",
                                               file_offset,
                                               file_size,
                                               this->_current_offset.load(std::memory_order_relaxed)));
        }

        auto read_response = co_await executor->submit_request(async::unaligned_read_request{
            .fd = this->fd(),
            .offset = file_offset,
            .size = file_size,
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

        file_header header;
        std::copy(read_response.data.data(), read_response.data.data() + sizeof(file_header), reinterpret_cast<uint8_t*>(&header));

        if(!skip_delete_check && header.deleted_flag)
        {
            co_return hedge::error(std::format("File with key '{}' is marked as deleted in value table (path: {}) at offset {}",
                                               uuids::to_string(header.key),
                                               this->path().string(),
                                               file_offset),
                                   hedge::errc::DELETED);
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
        read_response.data.erase(read_response.data.begin(), read_response.data.begin() + sizeof(file_header));

        co_return output_file{
            .header = header,
            .binaries = std::move(read_response.data),
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

        auto tmp_mmap = fs::mmap_view::from_file(
            file_desc.value(),
            value_table::_page_align_for_mmap()); // Map only the region needed for info.

        if(!tmp_mmap)
            return hedge::error("Failed to create temporary mmap: " + tmp_mmap.error().to_string());

        // Initialize the info struct in the mapped memory to zeros.
        // This ensures clean initial state, especially if not preallocating (where OS might not zero).
        value_table_info initial_info = {};
        // memcpy is safer than direct assignment via reference for potentially uninitialized memory.
        std::memcpy(&value_table::_get_info_from_mmap(tmp_mmap.value()), &initial_info, sizeof(value_table_info));

        return std::shared_ptr<value_table>(new value_table{
            id,
            0,
            std::move(file_desc.value()),
            std::move(tmp_mmap.value())});
    }

    hedge::expected<std::shared_ptr<value_table>> value_table::load(const std::filesystem::path& path, fs::file::open_mode open_mode)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("File does not exist: " + path.string());

        auto file_descriptor = fs::file::from_path(path, open_mode, false); // Assuming no O_DIRECT for now.

        if(!file_descriptor)
            return hedge::error("Failed to open file descriptor: " + file_descriptor.error().to_string());

        auto table_id = std::stoul(path.stem().string());

        auto non_owning_mmap = fs::mmap_view::from_file(
            file_descriptor.value(),
            value_table::_page_align_for_mmap());

        if(!non_owning_mmap)
            return hedge::error("Failed to create mmap for loading info: " + non_owning_mmap.error().to_string());

        // Read the current state (offset, counts, etc.) from the info struct via the mmap.
        // Make a copy to avoid potential issues if the mmap becomes invalid later,
        // although the non_owning_mmap should live as long as the value_table.
        value_table_info info = value_table::_get_info_from_mmap(non_owning_mmap.value());

        return std::shared_ptr<value_table>(new value_table{
            static_cast<uint32_t>(table_id),
            info.current_offset,
            std::move(file_descriptor.value()),
            std::move(non_owning_mmap.value())});
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

        if(header.deleted_flag)
            co_return hedge::ok();

        header.deleted_flag = true;

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

        // Update the table's persistent metadata (delete count, freed space).
        std::lock_guard lk(this->_info_mutex);
        auto& info = this->_info();
        info.deleted_count++;
        info.freed_space += sizeof(file_header) + header.file_size;
        // Note: The modification to `info` directly changes the mapped memory.
        // Fsync might be needed later if immediate persistence of metadata is required.

        // Deletion successful.
        co_return hedge::ok();
    }

    value_table_info& value_table::_info()
    {
        return value_table::_get_info_from_mmap(this->_mmap);
    }

    value_table_info value_table::info() const
    {
        // Note: This reads directly from the mmap, which might not be instantly updated
        // if writes are buffered by the OS. Fsync might be needed for strong consistency guarantees,
        // but for typical stats reading, this is usually acceptable.
        // Returns a copy, ensuring the caller doesn't accidentally modify the mapped memory.
        return value_table::_get_info_from_mmap(this->_mmap);
    }

    // Static helper to get a reference to the info struct within a given mmap region.
    value_table_info& value_table::_get_info_from_mmap(const fs::mmap_view& mmap)
    {
        // Calculate the starting address of the info struct within the mapped region.
        // It's located at the very end of the mapped range.
        auto* ptr_start = static_cast<uint8_t*>(mmap.get_ptr());
        auto* ptr_end = ptr_start + mmap.size();
        // The info struct is positioned just before the end pointer.
        auto* info_ptr = reinterpret_cast<value_table_info*>(ptr_end - sizeof(value_table_info));
        return *info_ptr;
    }

} // namespace hedge::db