#pragma once

#include <cstdint>
#include <error.hpp>
#include <sys/types.h>

#include "cache.h"
#include "fs.hpp"
#include "io/io_ctx.h"
#include "mailbox.h"
#include "mailbox_impl.h"
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
    // requires std::is_base_of_v<fs::file, FILE>
    class file_reader2
    {
        const FILE& _file;
        file_reader2_config _config;
        size_t _current_offset{0};

    public:
        file_reader2(const FILE& fd, const file_reader2_config& config);

        // This object represents an asynchronous read request.
        // The caller can co_await on the mailbox to get the result, and the buffer is where the data will be written to.
        struct awaitable_read_request_t
        {
            hedge::io::aw_io awaitable;          // Awaitable request (just a handler, does not own the memory)
            page_aligned_buffer<uint8_t> buffer; // Where the read_response result is written to (owned memory)
        };

        // This object represents an asynchronous page guard request.
        // The buffer associated with it is owned and managed by the cache
        // However, the page guard acts as a reference counter for keeping the page alive while it's being processed
        using awaitable_page_guard_t = db::page_cache::awaitable_page_guard;

        using awaitable_from_cache_or_fs_t = std::variant<awaitable_read_request_t, awaitable_page_guard_t>;

        std::vector<awaitable_from_cache_or_fs_t> next(const std::shared_ptr<db::sharded_page_cache>& cache);

        [[nodiscard]] size_t get_current_offset() const;
        [[nodiscard]] bool is_eof() const;
        [[nodiscard]] const auto& file() const
        {
            return this->_file;
        }

    private:
        std::vector<awaitable_from_cache_or_fs_t> _next_no_cache();
        // std::vector<awaitable_from_cache_or_fs_t> _next_cache(const std::shared_ptr<db::sharded_page_cache>& cache);
    };

} // namespace hedge::fs