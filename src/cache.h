#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace hedge::db
{
    struct page_handle
    {
        std::byte* data;
        size_t idx;
    };

    struct sharded_page_cache
    {
        sharded_page_cache(size_t /*size_bytes*/, uint32_t /*num_shards*/) {}

        std::vector<std::optional<page_handle>> get_write_slots_range(uint32_t /*file_id*/,
                                                                      size_t /*start_page*/,
                                                                      size_t /*num_pages*/)
        {
            return {};
        }
    };
} // namespace hedge::db
