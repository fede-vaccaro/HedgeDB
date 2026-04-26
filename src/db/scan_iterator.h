#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <error.hpp>

#include "memtable.h"
#include "merge/merge_utils.h"
#include "merge/sst_stream.h"
#include "sst.h"
#include "tmc/task.hpp"
#include "types.h"

namespace hedge::db
{
    using scan_source_t = std::variant<sst_stream*, memtable_cursor*>;

    inline bool source_is_eof(const scan_source_t& s)
    {
        return std::visit([](auto* p)
                          { return p->is_eof(); }, s);
    }

    inline bool source_buffer_empty(const scan_source_t& s)
    {
        return std::visit([](auto* p)
                          { return p->buffer_empty(); }, s);
    }

    inline hedge::expected<merge_entry_t> source_pop_entry(const scan_source_t& s)
    {
        return std::visit(
            [](auto* p) -> hedge::expected<merge_entry_t>
            {
                merge_entry_t e{
                    .key = key_t{p->front().key()},
                    .value = value_buffer_t{p->front().value()},
                    .epoch = p->epoch()};
                auto ok = p->pop_front();

                if(!ok) [[unlikely]]
                    return ok.error();

                return e;
            },
            s);
    }

    class scan_iterator
    {
        using sst_ptr_t = std::shared_ptr<sst>;

        struct slot_t
        {
            merge_entry_t entry{};
            scan_source_t source;
        };

        struct heap_item_t
        {
            slot_t* slot;
        };

        memtable::snapshot _memtable_snapshot;
        std::vector<sst_ptr_t> _ssts;
        std::unique_ptr<sst_stream_set> _rbufs;
        std::vector<memtable_cursor> _mem_cursors;

        std::optional<key_t> _lower;
        std::optional<key_t> _upper;

        deduplicator _dedup;
        std::deque<slot_t> _slots;
        std::vector<heap_item_t> _heap;

        bool _initialized{false};
        bool _exhausted{false};

        static auto _heap_cmp()
        {
            return [](const heap_item_t& lhs, const heap_item_t& rhs)
            {
                auto cmp = lhs.slot->entry.key <=> rhs.slot->entry.key;
                if(cmp != 0)
                    return cmp > 0;                                   // min-heap by key
                return lhs.slot->entry.epoch < rhs.slot->entry.epoch; // higher epoch pops first (newer wins)
            };
        }

        tmc::task<hedge::status> _refresh_buffers();
        tmc::task<hedge::status> _init();
        expected<std::pair<key_t, value_t>> _emit(merge_entry_t& item);
        tmc::task<hedge::expected<std::pair<key_t, value_t>>> _next_inner();

    public:
        scan_iterator(
            memtable::snapshot snapshot,
            std::vector<sst_ptr_t> ssts,
            std::unique_ptr<sst_stream_set> rbufs,
            std::optional<key_t> lower,
            std::optional<key_t> upper);

        scan_iterator(scan_iterator&&) = default;
        scan_iterator& operator=(scan_iterator&&) = default;
        scan_iterator(const scan_iterator&) = delete;
        scan_iterator& operator=(const scan_iterator&) = delete;

        /// Returns the next key-value pair in the range, or errc::END_OF_SCAN when exhausted.
        [[nodiscard]] tmc::task<expected<std::pair<key_t, value_t>>> next();

        static hedge::expected<scan_iterator> from_partition(
            memtable* memtable,
            const partition_t* partition,
            std::optional<key_t> lower,
            std::optional<key_t> upper,
            size_t read_ahead_size = 4 * KiB);
    };

} // namespace hedge::db
