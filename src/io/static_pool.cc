#include "io/static_pool.h"
#include "io/io_executor.h"

namespace hedge::io
{
    std::shared_ptr<io_executor>& static_pool::instance()
    {
        static std::shared_ptr<io_executor> executor = std::make_shared<io_executor>();
        return executor;
    }
} // namespace hedge::io