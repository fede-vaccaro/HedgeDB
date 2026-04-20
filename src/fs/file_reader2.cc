#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <sched.h>

#include <perf_counter.h>

#include "db/sst.h"
#include "io/io_ctx.h"
#include "io/io_requests.hpp"
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
    std::optional<typename file_reader2<FILE>::awaitable_read_request_t> file_reader2<FILE>::next()
    {
        return this->_next_no_cache();
    }

    template <typename FILE>
    std::optional<typename file_reader2<FILE>::awaitable_read_request_t> file_reader2<FILE>::_next_no_cache()
    {
        if(this->_current_offset >= this->_config.end_offset)
            return std::nullopt; // EOF reached

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

        page_aligned_buffer<std::byte> buffer(page_aligned_bytes_to_read);

        auto aw = io::read(this->_file.fd(), buffer.data(), page_aligned_bytes_to_read, this->_current_offset);

        this->_current_offset += page_aligned_bytes_to_read;

        return awaitable_read_request_t{std::move(aw), std::move(buffer)};
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
