#include <cstdint>
#include <cstdlib>
#include <format>
#include <stdexcept>

#include "file_reader.h"
#include "fs.hpp"
#include "io_executor.h"
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

        return file_reader::awaitable_read_request_t{awaitable_mailbox, bytes_to_read};
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