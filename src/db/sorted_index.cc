#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <linux/perf_event.h>
#include <optional>
#include <stdexcept>
#include <unistd.h>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"
#include "async/task.h"
#include "cache.h"
#include "fs/fs.hpp"
#include "sorted_index.h"
#include "types.h"
#include "utils.h"

#include <perf_counter.h>

namespace hedge::db
{

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key) const
    {
        auto meta_index_range_begin = this->_meta_index.begin();
        auto meta_index_range_end = this->_meta_index.end();

        auto comparator = [](const meta_index_entry& a, const key_t& b)
        {
            return a.key < b;
        };

        if(this->_super_index.has_value())
        {
            auto it = std::lower_bound(this->_super_index->begin(), this->_super_index->end(), key, comparator);
            if(it == this->_super_index->end())
                return std::nullopt;

            constexpr size_t REF_PAGE_SIZE = 4096;
            constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(meta_index_entry);

            size_t meta_index_page_id = std::distance(this->_super_index->begin(), it);

            meta_index_range_begin = this->_meta_index.begin() + (meta_index_page_id * KEYS_PER_META_INDEX_PAGE);
            meta_index_range_end = meta_index_range_begin + KEYS_PER_META_INDEX_PAGE;

            if(bool is_last_page = meta_index_page_id == this->_super_index->size() - 1; is_last_page)
            {
                size_t last_page_size = this->_meta_index.size() % KEYS_PER_META_INDEX_PAGE;
                if(last_page_size != 0)
                    meta_index_range_end = meta_index_range_begin + last_page_size;
            }

            const auto* prefetch_ptr = reinterpret_cast<const uint8_t*>(meta_index_range_begin);
            const auto* prefetch_end_ptr = reinterpret_cast<const uint8_t*>(meta_index_range_end);

            // Prefetch meta index page entries
            for(const uint8_t* ptr = prefetch_ptr; ptr < prefetch_end_ptr; ptr += 64)
                __builtin_prefetch(ptr, 0, 1);
        }

        // Perform the binary search on the meta-index.
        auto it = std::lower_bound(meta_index_range_begin, meta_index_range_end, key, comparator);

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

    async::task<expected<std::optional<value_ptr_t>>> sorted_index::lookup_async(const key_t& key, const std::shared_ptr<shared_page_cache>& cache) const
    {
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return std::nullopt;

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        // start_counter(fd);
        std::optional<page_cache::read_page_guard> opt_page_guard;
        std::optional<page_cache::write_page_guard> opt_write_guard;

        auto page_tag = to_page_tag(this->id(), page_start_offset);

        std::unique_ptr<uint8_t> data; // might be needed for holding a temporary memory allocation
        uint8_t* page_ptr = nullptr;
        bool should_read_from_fs = (cache == nullptr);

        if(!should_read_from_fs)
        {
            prof::get<"lookup">().start();
            auto maybe_page_guard = cache->lookup(page_tag);

            if(maybe_page_guard.has_value())
            {
                opt_page_guard = std::move(co_await maybe_page_guard.value());
                page_ptr = opt_page_guard->data + opt_page_guard->idx;
                prof::get<"cache_hits">().add(1);
                prof::get<"lookup">().stop(false);

                // std::cout << "cache hit for fd " << this->fd() << " and file " << this->path() << " page offset " << page_start_offset << "\n";

                // print meta index
                // size_t count = 0;
                // for(const auto& entry : this->_meta_index)
                // std::cout << "Meta index entry key: " << count++ << " " << entry.key << "\n";
            }
            else
            {
                prof::get<"lookup">().stop(true);
            }
        }

        if(!should_read_from_fs && !opt_page_guard.has_value())
        {
            should_read_from_fs = true;

            prof::get<"get_slot">().start();
            auto maybe_write_slot = cache->get_write_slot(page_tag);

            opt_write_guard = std::move(maybe_write_slot);
            page_ptr = opt_write_guard->data + opt_write_guard->idx;

            prof::get<"get_slot">().stop();

            prof::get<"cache_hits">().add(0);
        }

        if(should_read_from_fs)
        {
            if(!opt_page_guard.has_value() && !opt_write_guard.has_value())
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
            auto status = co_await this->_load_page_async(page_start_offset, page_ptr);
            if(!status)
                co_return status.error();
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

        prof::get<"find_in_page">().start();
        hedge::expected<std::optional<value_ptr_t>> res = sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
        prof::get<"find_in_page">().stop(should_read_from_fs);

        co_return res;
    }

    async::task<hedge::status> sorted_index::_load_page_async(size_t offset, uint8_t* data_ptr) const
    {
        auto response = co_await async::this_thread_executor()->submit_request(
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
        // for(auto* dbg_it = start; dbg_it != end; ++dbg_it)
        // {
        //     std::cout << "Index entry key: " << dbg_it->key << std::endl;
        // }

        return std::nullopt; // Key not found in this page range.
    }

    void sorted_index::clear_index()
    {
        this->_index = page_aligned_buffer<index_entry_t>{};
    }

    hedge::status sorted_index::load_index_from_fs()
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

    sorted_index::sorted_index(fs::file fd, page_aligned_buffer<index_entry_t> index, page_aligned_buffer<meta_index_entry> meta_index, sorted_index_footer footer)
        : fs::file(std::move(fd)),            // Initialize the base class with the file descriptor
          _index(std::move(index)),           // Move the provided index data
          _meta_index(std::move(meta_index)), // Move the provided meta-index data
          _footer(footer)                     // Copy the provided footer data
    {
    }

} // namespace hedge::db