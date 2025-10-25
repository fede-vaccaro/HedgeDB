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
#include "sorted_index.h"
#include "types.h"

namespace hedge::db
{

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key) const
    {
        auto comparator = [](const meta_index_entry& a, const key_t& b)
        {
            return a.page_max_id < b;
        };

        auto it = std::lower_bound(this->_meta_index.begin(), this->_meta_index.end(), key, comparator);

        if(it == this->_meta_index.end())
        {

            // for (const auto& entry : this->_meta_index)
            // {
            //     std::cout << "Meta index entry: " << entry.page_max_id << std::endl;
            //     std::cout << "Meta index size: " << this->_meta_index.size() << std::endl;
            // }

            // std::cout << "Key not found in meta index: " << key << std::endl;
            return std::nullopt;
        }
        return std::distance(this->_meta_index.begin(), it);
    }

    hedge::expected<std::optional<value_ptr_t>> sorted_index::lookup(const key_t& key) const
    {
        auto maybe_page_id = this->_find_page_id(key);

        if(!maybe_page_id.has_value())
            return std::nullopt;

        auto page_id = maybe_page_id.value();

        // std::cout << "Found page ID: " << page_id << " for key: " << key << std::endl;

        const index_entry_t* page_start_ptr{};

        fs::non_owning_mmap mmap;

        if(!this->_index.empty())
        {
            page_start_ptr = this->_index.data();
        }
        else
        {
            OUTCOME_TRY(mmap, fs::non_owning_mmap::from_fd_wrapper(*this));

            page_start_ptr = reinterpret_cast<index_entry_t*>(mmap.get_ptr());
        }

        page_start_ptr += page_id * INDEX_PAGE_NUM_ENTRIES;

        const index_entry_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        return sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    async::task<expected<std::optional<value_ptr_t>>> sorted_index::lookup_async(const key_t& key, const std::shared_ptr<async::executor_context>& executor) const
    {
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return std::nullopt;

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        auto maybe_page_ptr = co_await this->_load_page_async(page_start_offset, executor);

        if(!maybe_page_ptr)
            co_return maybe_page_ptr.error();

        auto* page_start_ptr = reinterpret_cast<index_entry_t*>(maybe_page_ptr.value().get());
        index_entry_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        co_return sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    async::task<expected<std::unique_ptr<uint8_t>>> sorted_index::_load_page_async(size_t offset, const std::shared_ptr<async::executor_context>& executor) const
    {
        auto response = co_await executor->submit_request(
            async::read_request{
                .fd = this->get_fd(),
                .offset = offset,
                .size = PAGE_SIZE_IN_BYTES});

        if(response.error_code != 0)
        {
            auto err_msg = std::format(
                "An error occurred while reading page at offset {} from file {}:  {}",
                offset,
                this->path().string(),
                strerror(response.error_code));
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

        co_return std::move(response.data);
    }

    std::optional<value_ptr_t> sorted_index::_find_in_page(const key_t& key, const index_entry_t* start, const index_entry_t* end)
    {
        const auto* it = std::lower_bound(start, end, index_entry_t{.key = key, .value_ptr = {}});

        if(it != end && it->key == key)
            return it->value_ptr;

        // for(auto* it = start; it != end; ++it)
        // {
        // std::cout << "Index entry key: " << it->key << std::endl;
        // }

        return std::nullopt;
    }

    async::task<hedge::status> sorted_index::_update_in_page(const index_entry_t& entry, size_t page_id, const index_entry_t* start, const index_entry_t* end, const std::shared_ptr<async::executor_context>& executor)
    {
        const auto* it = std::lower_bound(start, end, index_entry_t{.key = entry.key, .value_ptr = {}});

        if(it == end || it->key != entry.key)
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        const auto* entry_ptr = reinterpret_cast<const uint8_t*>(&entry);

        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->get_fd(), // todo: use a rw fd or write fd + fsync at the end
            .data = const_cast<uint8_t*>(entry_ptr),
            .size = sizeof(index_entry_t),
            .offset = (PAGE_SIZE_IN_BYTES * page_id) + (it - start),
        });

        if(write_response.error_code != 0)
            co_return hedge::error("An error occurred while updating an index entry: " + std::string(strerror(-write_response.error_code)));

        co_return hedge::ok();
    }

    async::task<hedge::status> sorted_index::try_update_async(const index_entry_t& entry, const std::shared_ptr<async::executor_context>& executor)
    {
        std::unique_lock<std::mutex> lock(*this->_compaction_mutex, std::try_to_lock); // try to acquire

        if(!lock.owns_lock())
            co_return hedge::error("Busy, index is being compacted", errc::BUSY);

        auto maybe_page_id = this->_find_page_id(entry.key);
        if(!maybe_page_id)
            co_return hedge::error("Not found", errc::KEY_NOT_FOUND);

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        auto maybe_page_ptr = co_await this->_load_page_async(page_start_offset, executor);

        if(!maybe_page_ptr)
            co_return maybe_page_ptr.error();

        auto* page_start_ptr = reinterpret_cast<index_entry_t*>(maybe_page_ptr.value().get());
        index_entry_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        co_return co_await this->_update_in_page(entry, page_id, page_start_ptr, page_end_ptr, executor);
    }

    void sorted_index::clear_index()
    {
        this->_index = std::vector<index_entry_t>{};
    }

    hedge::status sorted_index::load_index()
    {
        if(!this->_index.empty())
            return hedge::ok(); // already loaded

        auto mmap = fs::non_owning_mmap::from_fd_wrapper(*this);

        if(!mmap.has_value())
            throw std::runtime_error("Failed to mmap index file: " + mmap.error().to_string());

        auto* mmap_ptr = reinterpret_cast<index_entry_t*>(mmap.value().get_ptr());

        this->_index.assign(mmap_ptr, mmap_ptr + this->_footer.indexed_keys);

        return hedge::ok();
    }

    sorted_index::sorted_index(fs::file fd, std::vector<index_entry_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer)
        : fs::file(std::move(fd)), _index(std::move(index)), _meta_index(std::move(meta_index)), _footer(footer)
    {
    }

} // namespace hedge::db