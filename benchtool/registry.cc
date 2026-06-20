#include "common.h"

namespace hedge::db
{
    latency_registry& get_latency_registry()
    {
        static latency_registry instance;
        return instance;
    }
} // namespace hedge::db
