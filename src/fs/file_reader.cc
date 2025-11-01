#include <format>

#include "types.h"
#include "file_reader.h"
#include "fs.hpp"

namespace hedge::fs
{
    file_reader::file_reader(const fs::file& fd, const file_reader_config& config, std::shared_ptr<async::executor_context> executor)
        : _fd(fd), _config(config), _current_offset(config.start_offset), _executor(std::move(executor))
    {
    }

    async::task<expected<std::vector<uint8_t>>> file_reader::next(size_t num_bytes_to_read, bool clamp_at_end)
    {
        if(this->_fd.use_direct() && (num_bytes_to_read % PAGE_SIZE_IN_BYTES != 0))
            co_return hedge::error(std::format("Requested bytes to read ({}) from O_DIRECT file is not page aligned (page size: {}).", num_bytes_to_read, PAGE_SIZE_IN_BYTES));

        if(this->_current_offset >= this->_config.end_offset)
            co_return std::vector<uint8_t>{}; // EOF

        if(this->_current_offset + num_bytes_to_read > this->_config.end_offset)
        {
            if(clamp_at_end)
                num_bytes_to_read = this->_config.end_offset - this->_current_offset;
            else
                co_return hedge::error(std::format("Requested pages beyond limit: {} + {} >= {}", this->_current_offset, num_bytes_to_read, this->_config.end_offset));
        }

        // round to page size if using direct I/O
        auto actual_num_bytes_to_read = num_bytes_to_read;

        if(this->_fd.use_direct() && num_bytes_to_read % PAGE_SIZE_IN_BYTES != 0)
            num_bytes_to_read += PAGE_SIZE_IN_BYTES - (num_bytes_to_read % PAGE_SIZE_IN_BYTES);

        auto response = co_await this->_executor->submit_request(async::read_request{.fd = this->_fd.get_fd(), .offset = this->_current_offset, .size = num_bytes_to_read});

        this->_current_offset += num_bytes_to_read;

        if(response.error_code != 0)
            co_return hedge::error(std::format("An error occurred with request fd: {}, current_offset: {}, size: {}. Error: {}", this->_fd.get_fd(), this->_current_offset, num_bytes_to_read, strerror(-response.error_code)));

        if(response.bytes_read != num_bytes_to_read)
            co_return hedge::error(std::format("Unexpected bytes read: {}. Expected: {}", response.bytes_read, num_bytes_to_read));

        co_return std::vector<uint8_t>(response.data.get(), response.data.get() + actual_num_bytes_to_read);
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

} // namespace hedge::fs