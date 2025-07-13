#pragma once

#include <cstdint>
#include <error.hpp>
#include <format>
#include <sys/types.h>

#include "common.h"
#include "io_executor.h"

namespace hedgehog::async
{
    struct paginated_view_config
    {
        size_t initial_page_it{0};
        size_t last_page_it{0};
        size_t page_size_bytes{0};
    };

    class paginated_view
    {
        int32_t _fd{};
        paginated_view_config _config;
        size_t _page_it{0};

        std::shared_ptr<async::executor_context> _executor;

    public:
        paginated_view(int32_t fd, const paginated_view_config& config, std::shared_ptr<async::executor_context> executor);

        async::task<expected<std::vector<uint8_t>>> next(size_t num_pages, bool clamp_at_end);

        [[nodiscard]] size_t get_current_page_it() const;
        [[nodiscard]] bool is_eof() const;
        async::task<hedgehog::expected<std::vector<uint8_t>>> next(size_t bytes_to_read);

        void reset_it(size_t it);
    };

} // namespace hedgehog::async