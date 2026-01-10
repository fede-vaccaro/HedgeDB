#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <stdexcept>

#include "cache.h"
#include "file_reader.h"
#include "fs.hpp"
#include "io_executor.h"
#include "mailbox_impl.h"
#include "types.h"

namespace hedge::fs
{
    file_reader::file_reader(const fs::file& fd, const file_reader_config& config)
        : _fd(fd), _config(config), _current_offset(config.start_offset)
    {

        size_t buffer_size = this->_config.read_ahead_size;

        if(this->_fd.has_direct_access() && buffer_size % PAGE_SIZE_IN_BYTES != 0)
            buffer_size += PAGE_SIZE_IN_BYTES - (buffer_size % PAGE_SIZE_IN_BYTES);

        void* alloc = aligned_alloc(PAGE_SIZE_IN_BYTES, buffer_size);

        if(alloc == nullptr)
            throw std::runtime_error("Could not allocate memory for file reader");

        this->_buffer = std::unique_ptr<uint8_t>(static_cast<uint8_t*>(alloc));

        // page align read_ahead_size
        this->_config.read_ahead_size = buffer_size;
    }

    std::optional<file_reader::awaitable_read_request_t> file_reader::next()
    {
        if(this->_current_offset >= this->_config.end_offset)
            return std::nullopt; // EOF reached

        size_t bytes_to_read = this->_config.read_ahead_size;
        size_t page_aligned_bytes_to_read = bytes_to_read;

        // clip to end offset
        if(this->_current_offset + this->_config.read_ahead_size > this->_config.end_offset)
        {
            bytes_to_read = this->_config.end_offset - this->_current_offset;
            page_aligned_bytes_to_read = bytes_to_read;
        }

        // add padding to page align the request
        if(this->_fd.has_direct_access() && page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES != 0)
            page_aligned_bytes_to_read += PAGE_SIZE_IN_BYTES - (page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES);

        auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
            .fd = this->_fd.fd(),
            .data = this->_buffer.get(),
            .offset = this->_current_offset,
            .size = page_aligned_bytes_to_read,
        });

        this->_current_offset += page_aligned_bytes_to_read;

        std::span<uint8_t> memory_span{
            this->_buffer.get(),
            bytes_to_read,
        };

        return file_reader::awaitable_read_request_t{awaitable_mailbox, memory_span};
    }

    std::vector<file_reader::awaitable_from_cache_or_fs_t> file_reader::next(const std::shared_ptr<db::shared_page_cache>& cache)
    {
        if(this->_current_offset >= this->_config.end_offset)
            return {}; // EOF reached

        size_t bytes_to_read = this->_config.read_ahead_size;
        size_t page_aligned_bytes_to_read = bytes_to_read;

        // clip to end offset
        if(this->_current_offset + this->_config.read_ahead_size > this->_config.end_offset)
        {
            bytes_to_read = this->_config.end_offset - this->_current_offset;
            page_aligned_bytes_to_read = bytes_to_read;
        }

        // add padding to page align the request
        if(this->_fd.has_direct_access() && page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES != 0)
            page_aligned_bytes_to_read += PAGE_SIZE_IN_BYTES - (page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES);

        size_t num_pages_to_read = page_aligned_bytes_to_read / PAGE_SIZE_IN_BYTES;

        // try polling cache
        auto page_guards = cache->lookup_range(this->_fd.fd(), this->_current_offset, num_pages_to_read, true);

        std::vector<file_reader::awaitable_from_cache_or_fs_t> result;
        result.reserve(page_guards.size());

        // -1 means unassigned
        int64_t coalescing_sequence_start_it = -1;
        int64_t coalescing_sequence_end_it = -1;

        int64_t buffer_head = 0;

        for(size_t i = 0; i < page_guards.size(); ++i)
        {
            if(page_guards[i].has_value())
            {
                result.emplace_back(std::move(page_guards[i].value()));

                // If page_guards[i], then we can create the single request for reading the previous `coalesced_requests` consecutive pages
                if(coalescing_sequence_start_it - coalescing_sequence_end_it > 0)
                {
                    auto coalesced_requests = (coalescing_sequence_end_it - coalescing_sequence_start_it);

                    auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                        .fd = this->_fd.fd(),
                        .data = this->_buffer.get() + buffer_head,
                        .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                        .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
                    });

                    assert(size_t(buffer_head) < this->_config.read_ahead_size);
                    assert(size_t((buffer_head + coalesced_requests) * PAGE_SIZE_IN_BYTES) < this->_config.read_ahead_size);

                    buffer_head += coalesced_requests * PAGE_SIZE_IN_BYTES;

                    std::span<uint8_t> memory_span{
                        this->_buffer.get() + buffer_head,
                        coalesced_requests * PAGE_SIZE_IN_BYTES,
                    };

                    result.emplace_back(file_reader::awaitable_read_request_t{awaitable_mailbox, memory_span});

                    // Reset iterators
                    coalescing_sequence_start_it = -1;
                    coalescing_sequence_end_it = -1;
                }

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
            auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                .fd = this->_fd.fd(),
                .data = this->_buffer.get() + buffer_head,
                .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
            });

            size_t last_chunk_bytes = bytes_to_read - (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES);

            std::span memory_span{
                this->_buffer.get() + buffer_head,
                last_chunk_bytes,
            };

            result.emplace_back(file_reader::awaitable_read_request_t{awaitable_mailbox, memory_span});
        }

        // Finally confirm offset shift
        this->_current_offset += page_aligned_bytes_to_read;

        return result;
    }

    size_t file_reader::get_current_offset() const
    {
        return this->_current_offset;
    }

    bool file_reader::is_eof() const
    {
        return this->_current_offset >= this->_config.end_offset;
    }

    void file_reader::reset_it(size_t it)
    {
        this->_current_offset = std::min(it, this->_config.end_offset);
    }

    uint8_t* file_reader::get_buffer_ptr() const
    {
        return this->_buffer.get();
    }

} // namespace hedge::fs