#pragma once

#include <cstdint>
#include <error.hpp>
#include <sys/types.h>

#include "async/io_executor.h"
#include "fs.hpp"
#include "mailbox.h"
#include "mailbox_impl.h"

/*
    HedgeFS File Reader

    Provides an asynchronous file reader abstraction for streaming chunks of data from files using HedgeFS's async I/O executor.
*/
namespace hedge::fs
{
    struct file_reader_config
    {
        size_t start_offset{0};
        size_t end_offset{0};
        size_t read_ahead_size;
    };

    class file_reader
    {
        const fs::file& _fd;
        file_reader_config _config;
        size_t _current_offset{0};
        std::unique_ptr<uint8_t> _buffer;

    public:
        file_reader(const fs::file& fd, const file_reader_config& config);
        
        using awaitable_read_request_t = std::pair<async::awaitable_mailbox<async::read_response>, size_t>; // size_t is bytes requested before padding

        std::optional<awaitable_read_request_t> next();

        [[nodiscard]] size_t get_current_offset() const;
        [[nodiscard]] bool is_eof() const;
        [[nodiscard]] uint8_t* get_buffer_ptr() const;

        void reset_it(size_t it);
    };

} // namespace hedge::fs