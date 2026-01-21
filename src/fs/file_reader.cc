
#include <cstdint>
#include <cstdlib>
#include <stdexcept>

#include "cache.h"
#include "file_reader.h"
#include "fs.hpp"
#include "io_executor.h"
#include "mailbox_impl.h"
#include "types.h"
#include "utils.h"

#include <perf_counter.h>

namespace hedge::fs
{
    file_reader::file_reader(const fs::file& fd, const file_reader_config& config)
        : _file(fd), _config(config), _current_offset(config.start_offset)
    {

        size_t buffer_size = this->_config.read_ahead_size;

        if(buffer_size % PAGE_SIZE_IN_BYTES != 0)
            buffer_size += PAGE_SIZE_IN_BYTES - (buffer_size % PAGE_SIZE_IN_BYTES);

        // page align read_ahead_size
        this->_config.read_ahead_size = buffer_size;
    }

    std::vector<file_reader::awaitable_from_cache_or_fs_t> file_reader::next(const std::shared_ptr<db::shared_page_cache>& cache)
    {
        prof::get<"file_reader_next">().start();
        if(cache == nullptr)
            return this->_next();

        auto res = this->_next(cache);
        prof::get<"file_reader_next">().stop();
        return res;
    }

    std::vector<file_reader::awaitable_from_cache_or_fs_t> file_reader::_next()
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
        if(this->_file.has_direct_access() && page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES != 0)
            page_aligned_bytes_to_read += PAGE_SIZE_IN_BYTES - (page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES);

        page_aligned_buffer<index_entry_t> buffer(page_aligned_bytes_to_read / sizeof(index_entry_t));

        auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
            .fd = this->_file.fd(),
            .data = static_cast<uint8_t*>(buffer.raw_data()),
            .offset = this->_current_offset,
            .size = page_aligned_bytes_to_read,
        });

        this->_current_offset += page_aligned_bytes_to_read;

        auto last_chunk_size = bytes_to_read / sizeof(index_entry_t);
        buffer.resize(last_chunk_size);

        std::vector<file_reader::awaitable_from_cache_or_fs_t> out;
        out.reserve(1);
        out.emplace_back(file_reader::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});

        return out;
    }

    std::vector<file_reader::awaitable_from_cache_or_fs_t> file_reader::_next(const std::shared_ptr<db::shared_page_cache>& cache)
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
        if(page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES != 0)
            page_aligned_bytes_to_read += PAGE_SIZE_IN_BYTES - (page_aligned_bytes_to_read % PAGE_SIZE_IN_BYTES);

        size_t num_pages_to_read = page_aligned_bytes_to_read / PAGE_SIZE_IN_BYTES;

        // try polling cache
        assert(this->_current_offset % PAGE_SIZE_IN_BYTES == 0);
        prof::get<"merge_cache_bulk_lookup">().start();
        auto page_guards = cache->lookup_range(this->_file.id(), this->_current_offset / PAGE_SIZE_IN_BYTES, num_pages_to_read, true);
        // auto page_guards = std::vector<std::optional<db::page_cache::awaitable_page_guard>>{num_pages_to_read};
        prof::get<"merge_cache_bulk_lookup">().stop();

        // std::cout << "try reading: id " << this->_file.id() << " path: " << this->_file.path() << "\n";

        size_t hits = std::count_if(page_guards.begin(), page_guards.end(), [](const auto& pg)
                                    { return pg.has_value(); });

        prof::get<"merge_cache_hits">().add(hits, page_guards.size());

        std::vector<file_reader::awaitable_from_cache_or_fs_t> result;
        result.reserve(page_guards.size());

        // -1 means unassigned
        int64_t coalescing_sequence_start_it = -1;
        int64_t coalescing_sequence_end_it = -1;

        size_t from_fs_count_coalesced = 0;

        for(size_t i = 0; i < page_guards.size(); ++i)
        {
            if(page_guards[i].has_value())
            {
                // First create the single request for reading the previous `coalesced_requests` consecutive pages, if any
                if(auto coalesced_requests = (coalescing_sequence_end_it - coalescing_sequence_start_it); coalesced_requests > 0)
                {
                    page_aligned_buffer<index_entry_t> buffer((coalesced_requests * PAGE_SIZE_IN_BYTES) / sizeof(index_entry_t));

                    auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                        .fd = this->_file.fd(),
                        .data = static_cast<uint8_t*>(buffer.raw_data()),
                        .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                        .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
                    });

                    // myassert(size_t(buffer_head) < this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));
                    // myassert(size_t(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) <= this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));

                    from_fs_count_coalesced++;
                    result.emplace_back(file_reader::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});

                    // Reset iterators
                    coalescing_sequence_start_it = -1;
                    coalescing_sequence_end_it = -1;
                }

                // Then add the page guard to the result

                size_t last_page_size = PAGE_SIZE_IN_BYTES;

                // Detect if last chunk needs to be trimmed
                if(i == (page_guards.size() - 1) && this->_current_offset + bytes_to_read == this->_config.end_offset)
                {
                    size_t remainder = (PAGE_SIZE_IN_BYTES - (this->_config.end_offset % PAGE_SIZE_IN_BYTES)) % PAGE_SIZE_IN_BYTES;
                    last_page_size -= remainder;
                }
                std::span mem{
                    page_guards[i]->pg.data + page_guards[i]->pg.idx,
                    last_page_size};

                result.emplace_back(awaitable_page_guard_t{std::move(page_guards[i].value()), mem});

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
            page_aligned_buffer<index_entry_t> buffer((coalesced_requests * PAGE_SIZE_IN_BYTES) / sizeof(index_entry_t));

            auto awaitable_mailbox = async::this_thread_executor()->submit_request(async::read_request{
                .fd = this->_file.fd(),
                .data = static_cast<uint8_t*>(buffer.raw_data()),
                .offset = this->_current_offset + (coalescing_sequence_start_it * PAGE_SIZE_IN_BYTES),
                .size = coalesced_requests * PAGE_SIZE_IN_BYTES,
            });

            // myassert(size_t(buffer_head) < this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));
            // myassert(size_t(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) <= this->_config.read_ahead_size, "Buffer head " + std::to_string(buffer_head + (coalesced_requests * PAGE_SIZE_IN_BYTES)) + " exceeds read ahead size " + std::to_string(this->_config.read_ahead_size));

            size_t last_chunk_bytes = coalesced_requests * PAGE_SIZE_IN_BYTES;

            // Detect if last chunk needs to be trimmed
            if(this->_current_offset + bytes_to_read == this->_config.end_offset)
            {
                size_t remainder = (PAGE_SIZE_IN_BYTES - (this->_config.end_offset % PAGE_SIZE_IN_BYTES)) % PAGE_SIZE_IN_BYTES;
                last_chunk_bytes -= remainder;
            }

            size_t last_chunk_size = last_chunk_bytes / sizeof(index_entry_t);
            buffer.resize(last_chunk_size);

            from_fs_count_coalesced++;
            result.emplace_back(file_reader::awaitable_read_request_t{awaitable_mailbox, std::move(buffer)});
        }

        // verify sequentiality in requests
        // auto DEBUG_VERIFY_SEQUENTIALITY = [&]()
        // {
        //     size_t expected_offset = this->_current_offset;

        //     for(size_t i = 0; i < result.size(); ++i)
        //     {
        //         auto& r = result[i];

        //         if(std::holds_alternative<file_reader::awaitable_read_request_t>(r))
        //         {
        //             auto& read_req = std::get<file_reader::awaitable_read_request_t>(r);

        //             auto read_request = *static_cast<async::read_request*>(read_req.first.mbox->get_request());

        //             myassert(read_request.offset == expected_offset, "Expected offset " + std::to_string(expected_offset) + ", got " + std::to_string(read_request.offset));

        //             expected_offset += read_request.size;
        //         }
        //         else if(std::holds_alternative<file_reader::awaitable_page_guard_t>(r))
        //         {
        //             auto& page_guard = std::get<file_reader::awaitable_page_guard_t>(r);
        //             myassert(page_guard.first.await_ready());
        //             myassert(page_guard.first.pg.frame()->key.page_index * PAGE_SIZE_IN_BYTES == expected_offset, "Expected offset " + std::to_string(expected_offset) + ", got " + std::to_string(page_guard.first.pg.frame()->key.page_index * PAGE_SIZE_IN_BYTES));
        //             expected_offset += PAGE_SIZE_IN_BYTES;
        //         }
        //     }
        //     myassert(expected_offset == this->_current_offset + page_aligned_bytes_to_read, "Expected final offset " + std::to_string(this->_current_offset + page_aligned_bytes_to_read) + ", got " + std::to_string(expected_offset));
        // };

        // DEBUG_VERIFY_SEQUENTIALITY();

        prof::get<"fs_read_requests">().add(from_fs_count_coalesced);

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

} // namespace hedge::fs