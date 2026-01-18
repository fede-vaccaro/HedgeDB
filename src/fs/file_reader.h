#pragma once

#include <cstdint>
#include <error.hpp>
#include <sys/types.h>

#include "fs.hpp"
#include "mailbox.h"
#include "mailbox_impl.h"

#include "cache.h"
#include "utils.h"

/*
    HedgeFS File Reader

    Provides an asynchronous file reader abstraction for streaming chunks of data from files using HedgeFS's async I/O executor.
*/
namespace hedge::fs
{
    struct file_reader_config
    {
        size_t start_offset{};
        size_t end_offset{};
        size_t read_ahead_size;
    };

    class file_reader
    {
        const fs::file& _file;
        file_reader_config _config;
        size_t _current_offset{0};
        aligned_buffer_t _buffer = aligned_buffer_t(nullptr, std::free);

    public:
        file_reader(const fs::file& fd, const file_reader_config& config);

        using awaitable_read_request_t = std::pair<async::awaitable_mailbox<async::read_response>, std::span<uint8_t>>; // size_t is bytes requested before padding

        using awaitable_page_guard_t = std::pair<db::page_cache::awaitable_page_guard, std::span<uint8_t>>; // size_t is bytes requested before padding

        std::optional<awaitable_read_request_t> next();

        using awaitable_from_cache_or_fs_t = std::variant<awaitable_read_request_t, awaitable_page_guard_t>;
        std::vector<awaitable_from_cache_or_fs_t> next(const std::shared_ptr<db::shared_page_cache>& cache);

        [[nodiscard]] size_t get_current_offset() const;
        [[nodiscard]] bool is_eof() const;
        [[nodiscard]] uint8_t* get_buffer_ptr() const;

        void reset_it(size_t it);
    };

} // namespace hedge::fs