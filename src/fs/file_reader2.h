#pragma once

#include <cstdint>
#include <error.hpp>
#include <optional>
#include <sys/types.h>

#include "fs.hpp"
#include "io/io_ctx.h"
#include "page_aligned_buffer.h"

/*
    HedgeFS File Reader

    Provides an asynchronous file reader abstraction for streaming chunks of data from files using HedgeFS's async I/O executor.
*/
namespace hedge::fs
{
    struct file_reader2_config
    {
        size_t start_offset{};
        size_t end_offset{};
        size_t read_ahead_size;
    };

    template <typename FILE = fs::file>
    class file_reader2
    {
        const FILE& _file;
        file_reader2_config _config;
        size_t _current_offset{0};

    public:
        file_reader2(const FILE& fd, const file_reader2_config& config);

        struct awaitable_read_request_t
        {
            hedge::io::aw_io awaitable;          // Awaitable request (just a handler, does not own the memory)
            page_aligned_buffer<uint8_t> buffer; // Where the read_response result is written to (owned memory)
        };

        std::optional<awaitable_read_request_t> next();

        [[nodiscard]] size_t get_current_offset() const;
        [[nodiscard]] bool is_eof() const;
        [[nodiscard]] const auto& file() const
        {
            return this->_file;
        }

    private:
        std::optional<awaitable_read_request_t> _next_no_cache();
    };

} // namespace hedge::fs
