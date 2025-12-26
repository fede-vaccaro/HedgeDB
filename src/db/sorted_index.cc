#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <unistd.h>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"
#include "async/task.h"
#include "fs/fs.hpp"
#include "page_cache.h"
#include "sorted_index.h"
#include "types.h"

namespace hedge::db
{

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key) const
    {
        // Define a custom comparator for lower_bound. We are looking for the first
        // element 'a' for which the predicate `a.page_max_id < key` is false.
        // This means `a.page_max_id >= key`.
        auto comparator = [](const meta_index_entry& a, const key_t& b)
        {
            return a.page_max_id < b;
        };

        // Perform the binary search on the meta-index.
        auto it = std::lower_bound(this->_meta_index.begin(), this->_meta_index.end(), key, comparator);

        // If lower_bound returns the end iterator, it means the key is greater than
        // the maximum key of all pages in this index file.
        if(it == this->_meta_index.end())
        {
            // Debugging output (commented out)
            // for (const auto& entry : this->_meta_index) { std::cout << "Meta index entry: " << entry.page_max_id << std::endl; }
            // std::cout << "Meta index size: " << this->_meta_index.size() << std::endl;
            // std::cout << "Key not found in meta index: " << key << std::endl;
            return std::nullopt;
        }

        // Otherwise, the distance from the beginning to the iterator gives the page ID.
        return std::distance(this->_meta_index.begin(), it);
    }

    hedge::expected<std::optional<value_ptr_t>> sorted_index::lookup(const key_t& key) const
    {
        // Step 1: Use the meta-index to find the potential page ID.
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id.has_value())
            return std::nullopt; // Key is not within the range of this index.

        auto page_id = maybe_page_id.value();
        // Debugging output (commented out)
        // std::cout << "Found page ID: " << page_id << " for key: " << key << std::endl;

        const index_entry_t* page_start_ptr{nullptr};
        fs::mmap_view mmap; // RAII object to manage the memory map.

        // Step 2: Get a pointer to the start of the relevant page's data.
        // Prefer the in-memory index (_index) if it has been loaded.
        if(!this->_index.empty())
        {
            page_start_ptr = this->_index.data();
        }
        else
        {
            // If the index isn't in memory, create a memory map of the file.
            // Using non_owning_mmap as the `sorted_index` already owns the fd via inheritance.
            OUTCOME_TRY(mmap, fs::mmap_view::from_file(*this));
            page_start_ptr = reinterpret_cast<index_entry_t*>(mmap.get_ptr());
        }

        // Calculate the pointer to the beginning of the target page within the mapped region or vector.
        page_start_ptr += page_id * INDEX_PAGE_NUM_ENTRIES;

        // Step 3: Calculate the pointer to the end of the target page.
        // This is typically the start + number of entries per page.
        const index_entry_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        // However, the last page might be smaller than a full page. Adjust the end pointer if necessary.
        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        // Step 4: Perform a binary search within the identified page boundaries.
        return sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    async::task<expected<std::optional<value_ptr_t>>> sorted_index::lookup_async(const key_t& key, const std::shared_ptr<async::executor_context>& executor, const std::shared_ptr<page_cache>& cache) const
    {
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return std::nullopt;

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        std::optional<page_cache::page_guard> opt_page_guard;
        auto page_tag = to_page_tag(static_cast<uint32_t>(this->fd()), page_start_offset);
        std::unique_ptr<uint8_t> data; // might be needed for holding a temporary page memory allocation
        uint8_t* page_ptr = nullptr;
        bool should_read_from_fs = (cache == nullptr);

        if(!should_read_from_fs)
        {
            auto maybe_page_guard = cache->lookup(page_tag);

            if(maybe_page_guard.has_error() && maybe_page_guard.error().code() == errc::BUSY)
            {
                should_read_from_fs = true;
                // std::cout << "read lock busy!!!!\n";
            }
            else if(maybe_page_guard.value().has_value())
            {
                opt_page_guard = std::move(maybe_page_guard.value().value());
                page_ptr = opt_page_guard->page_data;
                // std::cout << "read lock ok!!!!\n";
            }
        }

        if(!should_read_from_fs && !opt_page_guard.has_value())
        {
            should_read_from_fs = true;

            auto maybe_write_slot = cache->get_write_slot(page_tag);
            if(maybe_write_slot.has_value())
            {
                opt_page_guard = std::move(maybe_write_slot.value());
                page_ptr = opt_page_guard->page_data;
                // std::cout << "write lock ok!\n";
            }
            else
            {
                // should_read_from_fs = true;
                // std::cout << "write lock busy!!!\n";
            }
        }

        if(should_read_from_fs)
        {
            if(!opt_page_guard.has_value())
            {
                auto* page_mem_ptr = static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES));
                if(page_mem_ptr == nullptr)
                {
                    auto err_msg = std::format(
                        "Failed to allocate memory for loading page at offset {} from file {}",
                        page_start_offset,
                        this->path().string());
                    co_return hedge::error(err_msg);
                }

                data = std::unique_ptr<uint8_t>(page_mem_ptr);
                page_ptr = page_mem_ptr;
            }

            assert(page_ptr != nullptr);
            auto maybe_page_ptr = co_await this->_load_page_async(page_start_offset, page_ptr, executor);
            if(!maybe_page_ptr)
                co_return maybe_page_ptr.error();
        }

        auto* page_start_ptr = reinterpret_cast<index_entry_t*>(page_ptr);
        index_entry_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        // Adjust the end pointer if this is the last page, which is potentially partially-filled.
        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t entries_on_last_page = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(entries_on_last_page != 0)
                page_end_ptr = page_start_ptr + entries_on_last_page;
        }

        hedge::expected<std::optional<value_ptr_t>> res = sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);

        co_return res;
    }

    async::task<hedge::status> sorted_index::_load_page_async(size_t offset, uint8_t* data_ptr, const std::shared_ptr<async::executor_context>& executor) const
    {
        auto response = co_await executor->submit_request(
            async::read_request{
                .fd = this->fd(),
                .data = data_ptr,
                .offset = offset,
                .size = PAGE_SIZE_IN_BYTES});

        if(response.error_code != 0)
        {
            auto err_msg = std::format(
                "An error occurred while reading page at offset {} from file {}:  {}",
                offset,
                this->path().string(),
                strerror(-response.error_code));
            co_return hedge::error(err_msg);
        }

        if(response.bytes_read != PAGE_SIZE_IN_BYTES)
        {
            auto err_msg = std::format(
                "Read {} bytes instead of {} from file {} at offset {}",
                response.bytes_read,
                PAGE_SIZE_IN_BYTES,
                this->path().string(),
                offset);
            co_return hedge::error(err_msg);
        }

        co_return hedge::ok();
    }

    std::optional<value_ptr_t> sorted_index::_find_in_page(const key_t& key, const index_entry_t* start, const index_entry_t* end)
    {
        const auto* it = std::lower_bound(start, end, index_entry_t{.key = key, .value_ptr = {}}); // Create a dummy entry for comparison

        if(it != end && it->key == key)
            return it->value_ptr; // Key found, return the associated value pointer.

        // Debugging output (commented out)
        // for(auto* dbg_it = start; dbg_it != end; ++dbg_it) { std::cout << "Index entry key: " << dbg_it->key << std::endl; }

        return std::nullopt; // Key not found in this page range.
    }

    async::task<hedge::status> sorted_index::_update_in_page(const index_entry_t& entry, size_t page_id, const index_entry_t* start, const index_entry_t* end, const std::shared_ptr<async::executor_context>& executor)
    {
        // First, verify the key exists at the expected location within the provided page range.
        // Use lower_bound to find where the key *should* be.
        const auto* it = std::lower_bound(start, end, index_entry_t{.key = entry.key, .value_ptr = {}});

        // If the iterator is at the end or the key doesn't match, the entry isn't where we expected it.
        if(it == end || it->key != entry.key)
            co_return hedge::error("Key not found in page for update", errc::KEY_NOT_FOUND);

        // Calculate the exact byte offset within the file for this entry.
        // Offset = start_of_page + offset_within_page
        size_t entry_offset_in_page_bytes = std::distance(start, it) * sizeof(index_entry_t);
        size_t total_file_offset = (this->_footer.index_start_offset + PAGE_SIZE_IN_BYTES * page_id) + entry_offset_in_page_bytes;

        // Get a pointer to the raw byte data of the new entry to write.
        // Need const_cast as io_uring write takes a non-const void*.
        const auto* entry_byte_ptr = reinterpret_cast<const uint8_t*>(&entry);

        // Submit the asynchronous write request.
        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->fd(),                             // Use the file descriptor of this sorted_index
            .data = const_cast<uint8_t*>(entry_byte_ptr), // Pointer to the data to write
            .size = sizeof(index_entry_t),                // Size of the data to write
            .offset = total_file_offset,                  // Exact byte offset in the file
        });

        // Check for io_uring errors.
        if(write_response.error_code != 0)
            co_return hedge::error("An error occurred while updating an index entry: " + std::string(strerror(-write_response.error_code)));

        // TODO: Consider adding fsync logic here or at a higher level if durability is required immediately after update.

        co_return hedge::ok(); // Write submitted successfully (completion doesn't guarantee durability yet).
    }

    void sorted_index::clear_index()
    {
        this->_index = std::vector<index_entry_t>{};
    }

    hedge::status sorted_index::load_index()
    {
        if(!this->_index.empty())
            return hedge::ok();

        this->_index.resize(this->_footer.indexed_keys);

        int res = pread(this->fd(), this->_index.data(), sizeof(index_entry_t) * this->_index.size(), 0);

        if(res < 0)
            return hedge::error(std::format("Failed to load index from file {}: {}", this->path().string(), strerror(errno)));

        if(static_cast<size_t>(res) != sizeof(index_entry_t) * this->_index.size())
            return hedge::error(std::format("Incomplete read while loading index from file {}: read {} bytes, expected {} bytes",
                                            this->path().string(),
                                            res,
                                            sizeof(index_entry_t) * this->_index.size()));

        // The mmap object goes out of scope here, unmapping the file. The data is now in the vector.
        return hedge::ok();
    }

    sorted_index::sorted_index(fs::file fd, std::vector<index_entry_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer)
        : fs::file(std::move(fd)),            // Initialize the base class with the file descriptor
          _index(std::move(index)),           // Move the provided index data
          _meta_index(std::move(meta_index)), // Move the provided meta-index data
          _footer(footer)                     // Copy the provided footer data
    {
    }

} // namespace hedge::db