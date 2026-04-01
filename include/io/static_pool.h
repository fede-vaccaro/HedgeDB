#pragma once

#include "io_executor.h"

namespace hedge::io
{

    struct static_pool
    {
        static std::shared_ptr<io_executor>& instance();

        static_pool() = delete;
        ~static_pool() = delete;
    };

} // namespace hedge::io