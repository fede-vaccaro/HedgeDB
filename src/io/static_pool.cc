#include "io/static_pool.h"
#include "io/io_executor.h"

namespace hedge::io
{
    io_executor& static_pool::instance()
    {
        static io_executor executor;
        return executor;
    }
} // namespace hedge::io