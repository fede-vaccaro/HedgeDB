#pragma once

#include <cstdint>
#include <error.hpp>
#include <sys/types.h>

#include "fs.hpp"
#include "io_executor.h"
namespace hedgehog::async
{
    struct file_reader_config
    {
        size_t start_offset{0};
        size_t end_offset{0};
    };

    class file_reader
    {
        fs::file_descriptor& _fd;
        file_reader_config _config;
        size_t _current_offset{0};

        std::shared_ptr<async::executor_context> _executor;

    public:
        file_reader(fs::file_descriptor& fd, const file_reader_config& config, std::shared_ptr<async::executor_context> executor);

        async::task<expected<std::vector<uint8_t>>> next(size_t num_bytes, bool clamp_at_end = true);

        [[nodiscard]] size_t get_current_offset() const;
        [[nodiscard]] bool is_eof() const;

        void reset_it(size_t it);
    };

} // namespace hedgehog::async