#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <error.hpp>

#include "cache.h"
#include "merge/merge_utils.h"
#include "merge/rolling_buffer.h"
#include "sst.h"
#include "tmc/task.hpp"
#include "types.h"

namespace hedge::db
{

    class scan_iterator
    {
        using sst_ptr_t = std::shared_ptr<sst>;

        struct heap_item_t
        {
            merge_entry_t entry;
            rolling_buffer2* source;
            size_t epoch;
        };

        std::vector<sst_ptr_t> _ssts;
        std::unique_ptr<rolling_buffer_set> _rbufs;
        std::shared_ptr<sharded_page_cache> _cache;

        std::optional<key_t> _lower;
        std::optional<key_t> _upper;

        merge_iterator2 _dedup;
        std::vector<heap_item_t> _heap;

        bool _initialized{false};
        bool _exhausted{false};

        static auto _heap_cmp()
        {
            return [](const heap_item_t& lhs, const heap_item_t& rhs)
            {
                auto cmp = lhs.entry.key <=> rhs.entry.key;
                if(cmp != 0)
                    return cmp > 0;           // min-heap by key
                return lhs.epoch < rhs.epoch; // higher epoch pops first (newer wins)
            };
        }

        tmc::task<hedge::status> _refresh_buffers();
        tmc::task<hedge::status> _init();
        expected<std::pair<key_t, value_t>> _emit(merge_entry_t& item);
        tmc::task<hedge::expected<std::pair<key_t, value_t>>> _next_inner();

    public:
        scan_iterator(std::vector<sst_ptr_t> ssts,
                      std::unique_ptr<rolling_buffer_set> rbufs,
                      std::shared_ptr<sharded_page_cache> cache,
                      std::optional<key_t> lower,
                      std::optional<key_t> upper);

        scan_iterator(scan_iterator&&) = default;
        scan_iterator& operator=(scan_iterator&&) = default;
        scan_iterator(const scan_iterator&) = delete;
        scan_iterator& operator=(const scan_iterator&) = delete;

        /// Returns the next key-value pair in the range, or errc::END_OF_SCAN when exhausted.
        [[nodiscard]] tmc::task<expected<std::pair<key_t, value_t>>> next();

        static hedge::expected<scan_iterator> from_partition(
            const partition_t& partition,
            std::optional<key_t> lower,
            std::optional<key_t> upper,
            std::shared_ptr<sharded_page_cache> cache = nullptr,
            size_t read_ahead_size = 256 * 1024);
    };

} // namespace hedge::db
