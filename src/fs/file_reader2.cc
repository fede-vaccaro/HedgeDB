#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <sched.h>

#include <perf_counter.h>

#include "cache.h"
#include "db/sst.h"
#include "io_executor.h"
#include "mailbox_impl.h"
#include "page_aligned_buffer.h"
#include "types.h"
#include "utils.h"

#include "file_reader2.h"

namespace hedge::fs
{
    template <typename FILE>
    file_reader2<FILE>::file_reader2(const FILE& fd, const file_reader2_config& config)
        : _file(fd), _config(config), _current_offset(config.start_offset)
    {
        size_t buffer_size = this->_config.read_ahead_size;

        if(!hedge::is_page_aligned(buffer_size))
            buffer_size = hedge::ceil_page_align(buffer_size);

        // page align read_ahead_size
        this->_config.read_ahead_size = buffer_size;
    }

    template <typename FILE>
    std::vector<typename file_reader2<FILE>::awaitable_from_cache_or_fs_t> file_reader2<FILE>::next(const std::shared_ptr<db::sharded_page_cache>& cache)
    {
        prof::counter_guard counter_guard(prof::get<"file_reader_next">());

        if(cache == nullptr)
            return this->_next_no_cache();

        auto res = this->_next_cache(cache);
        return res;
    }

    template <typename FILE>
    std::vector<typename file_reader2<FILE>::awaitable_from_cache_or_fs_t> file_reader2<FILE>::_next_no_cache()
    {
        if(this->_current_offset >= this->_config.end_offset)
            return {}; // EOF reached

        size_t bytes_to_read = this->_config.read_ahead_size;
        size_t page_aligned_bytes_to_read = bytes_to_read;

        // Clip to end offset
        if(this->_current_offset + this->_config.read_ahead_size > this->_config.end_offset)
        {
            bytes_to_read = this->_config.end_offset - this->_current_offset;
            page_aligned_bytes_to_read = bytes_to_read;
        }

        // Add padding to page align the request
        if(this->_file.has_direct_access() && !hedge::is_page_aligned(page_aligned_bytes_to_read))
            page_aligned_bytes_to_read = hedge::ceil_page_align(page_aligned_bytes_to_read);

        page_aligned_buffer<uint8_t> buffer(page_aligned_bytes_to_read);

        auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
            .fd = this->_file.fd(),
            .data = buffer.data(),
            .offset = this->_current_offset,
            .size = page_aligned_bytes_to_read,
        });

        this->_current_offset += page_aligned_bytes_to_read;

        std::vector<file_reader2::awaitable_from_cache_or_fs_t> out;
        out.reserve(1);
        out.emplace_back(file_reader2::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});

        return out;
    }

    template <typename FILE>
    std::vector<typename file_reader2<FILE>::awaitable_from_cache_or_fs_t> file_reader2<FILE>::_next_cache(const std::shared_ptr<db::sharded_page_cache>& cache)
    {
        assert(cache != nullptr && "Use _next_no_cache when cache is nullptr");

        if(this->_current_offset >= this->_config.end_offset)
            return {}; // EOF reached

        size_t bytes_to_read = this->_config.read_ahead_size;
        size_t page_aligned_bytes_to_read = bytes_to_read;

        // Clip to end offset
        if(this->_current_offset + this->_config.read_ahead_size > this->_config.end_offset)
        {
            bytes_to_read = this->_config.end_offset - this->_current_offset;
            page_aligned_bytes_to_read = bytes_to_read;
        }

        // Add padding to page align the request
        if(!hedge::is_page_aligned(page_aligned_bytes_to_read))
            page_aligned_bytes_to_read = hedge::ceil_page_align(page_aligned_bytes_to_read);

        const size_t num_pages_to_read = page_aligned_bytes_to_read / PAGE_SIZE_IN_BYTES;

        // try polling cache
        assert(hedge::is_page_aligned(this->_current_offset) && "Current offset must be page aligned when reading with cache");

        prof::get<"merge_cache_bulk_lookup">().start();
        // Lookup_range requests a sequence of pages (page handles/guards) from the page cache
        // Notice that the underlying buffers might be scattered around the cache, and there might be "holes" in this range
        // If a page is not found, just std::nullopt is returned
        std::vector<std::optional<db::page_cache::awaitable_page_guard>> page_guards =
            cache->lookup_range(this->_file.id(),
                                this->_current_offset / PAGE_SIZE_IN_BYTES,
                                num_pages_to_read,
                                true);
        prof::get<"merge_cache_bulk_lookup">().stop();

        size_t hits = std::count_if(page_guards.begin(), page_guards.end(), [](const auto& pg)
                                    { return pg.has_value(); });

        prof::get<"merge_cache_hits">().add(hits, page_guards.size());

        std::vector<file_reader2::awaitable_from_cache_or_fs_t> result;
        result.reserve(page_guards.size());

        // As stated before, there might be some holes in the page guards vector
        // If there are consecutive "holes", those are coalesced into a single read request
        // The algorithm listed below create these awaitable requests

        int64_t coalescing_sequence_start_it = -1; // -1 means unassigned
        int64_t coalescing_sequence_end_it = -1;

        size_t stat_from_fs_count_coalesced = 0;

        for(size_t i = 0; i < page_guards.size(); ++i)
        {
            if(page_guards[i].has_value()) // A sequence to coalesce is over when a valued page guard is found
            {
                // First create the single request for reading the previous `coalesced_requests` consecutive pages (if any)
                if(auto coalesced_requests = (coalescing_sequence_end_it - coalescing_sequence_start_it); coalesced_requests > 0)
                {
                    page_aligned_buffer<uint8_t> buffer(coalesced_requests * PAGE_SIZE_IN_BYTES);

                    auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                        .fd = this->_file.fd(),
                        .data = buffer.data(),
                        .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                        .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
                    });

                    // myassert(size_t(buffer_head) < this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));
                    // myassert(size_t(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) <= this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));

                    stat_from_fs_count_coalesced++;
                    result.emplace_back(file_reader2::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});

                    // Reset iterators
                    coalescing_sequence_start_it = -1;
                    coalescing_sequence_end_it = -1;
                }

                result.emplace_back(std::move(page_guards[i].value()));

                continue;
            }

            // Otherwise, should be loaded from the file system, set coalescing start iterator
            if(coalescing_sequence_start_it == -1)
                coalescing_sequence_start_it = i;

            coalescing_sequence_end_it = i + 1;
        }

        // If any, create the request for requesting the last batch of requests
        if(auto coalesced_requests = (coalescing_sequence_end_it - coalescing_sequence_start_it); coalesced_requests > 0)
        {
            page_aligned_buffer<uint8_t> buffer(coalesced_requests * PAGE_SIZE_IN_BYTES);

            auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                .fd = this->_file.fd(),
                .data = buffer.data(),
                .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
            });

            stat_from_fs_count_coalesced++;
            result.emplace_back(file_reader2::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});
        }

        prof::get<"fs_read_requests">().add(stat_from_fs_count_coalesced);

        // Finally confirm offset shift
        this->_current_offset += page_aligned_bytes_to_read;

        return result;
    }

    template <typename FILE>
    size_t file_reader2<FILE>::get_current_offset() const
    {
        return this->_current_offset;
    }

    template <typename FILE>
    bool file_reader2<FILE>::is_eof() const
    {
        return this->_current_offset >= this->_config.end_offset;
    }

    template class file_reader2<db::sst>;
    template class file_reader2<fs::file>;

} // namespace hedge::fs