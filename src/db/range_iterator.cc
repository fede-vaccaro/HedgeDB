#include <algorithm>
#include <cstdint>
#include <limits>

#include "db/memtable.h"
#include "db/range_iterator.h"
#include "error.hpp"
#include "sst.h"
#include "types.h"

// This specific class has been for good part agentic-coded.
// I find the code particularly confusing.
// I will rewrite it it soon or later.

namespace hedge::db
{

    hedge::expected<range_iterator> range_iterator::make_new(
        memtable* memtable,
        const partition_t* partition,
        std::optional<key_t> lower,
        std::optional<key_t> upper,
        size_t read_ahead_size)
    {
        memtable::snapshot snapshot;
        if(memtable != nullptr)
            snapshot = memtable->acquire_snapshot();
        return make_new(std::move(snapshot), partition, std::move(lower), std::move(upper), read_ahead_size);
    }

    hedge::expected<range_iterator> range_iterator::make_new(
        memtable::snapshot snapshot,
        const partition_t* partition,
        std::optional<key_t> lower,
        std::optional<key_t> upper,
        size_t read_ahead_size)
    {
        std::vector<sst_ptr_t> matching_ssts;
        std::vector<page_range> ranges;

        // Collect SSTs that overlap with the range and compute their page ranges
        if(partition != nullptr)
        {
            for(const auto& level : *partition)
            {
                for(const auto& sst_ptr : level)
                {
                    auto range = sst_ptr->find_range(lower, upper);
                    if(!range)
                        continue;

                    matching_ssts.push_back(sst_ptr);
                    ranges.push_back(*range);

                    // Pending memtables in flush might have overlapping keys with SSTs in L0
                    auto sst_epoch = sst_ptr->epoch();

                    // --> discard the memtables (to favour memory space)
                    if(auto it = snapshot.pending_flushes.find(sst_epoch); it != snapshot.pending_flushes.end())
                        snapshot.pending_flushes.erase(it);
                }
            }

            read_ahead_size = hedge::ceil_page_align(read_ahead_size);
        }

        // Prepare rolling_buffers (readers)
        auto rbufs = std::make_unique<sst_stream_set>(matching_ssts.size());

        for(size_t i = 0; i < matching_ssts.size(); ++i)
        {
            auto& sst_ptr = matching_ssts[i];
            auto& range = ranges[i];

            size_t start_offset = sst_ptr->footer().index_offset + (range.first_page_id * PAGE_SIZE_IN_BYTES);
            size_t end_offset = sst_ptr->footer().index_offset + ((range.last_page_id + 1) * PAGE_SIZE_IN_BYTES);

            rbufs->emplace_back(*sst_ptr,
                                fs::file_reader2_config{
                                    .start_offset = start_offset,
                                    .end_offset = end_offset,
                                    .read_ahead_size = read_ahead_size},
                                read_ahead_size);
        }

        return range_iterator{
            std::move(snapshot),
            std::move(matching_ssts),
            std::move(rbufs),
            std::move(lower),
            std::move(upper)};
    };

    range_iterator::range_iterator(
        memtable::snapshot snapshot,
        std::vector<sst_ptr_t> ssts,
        std::unique_ptr<sst_stream_set> rbufs,
        std::optional<key_t> lower,
        std::optional<key_t> upper)

        : _memtable_snapshot(std::move(snapshot)),
          _ssts(std::move(ssts)),
          _rbufs(std::move(rbufs)),
          _lower(std::move(lower)),
          _upper(std::move(upper))
    {
        auto seq = this->_memtable_snapshot.seq_nr;
        if(this->_memtable_snapshot.curr)
            this->_mem_cursors.emplace_back(*this->_memtable_snapshot.curr->ptr(), this->_lower, this->_upper, std::numeric_limits<uint64_t>::max(), seq);

        for(auto& [epoch, table_ptr] : this->_memtable_snapshot.pending_flushes)
            this->_mem_cursors.emplace_back(*table_ptr->ptr(), this->_lower, this->_upper, epoch);

        this->_heap.reserve(this->_ssts.size() + this->_mem_cursors.size());
    }

    tmc::task<hedge::status> range_iterator::_refresh_buffers()
    {
        for(auto* it = this->_rbufs->begin(); it < this->_rbufs->end(); ++it)
        {
            if(auto status = co_await it->refresh(); !status)
                co_return hedge::error("Failed to refresh scan buffer: " + status.error().to_string());
        }

        co_return hedge::ok();
    }

    tmc::task<hedge::status> range_iterator::_init()
    {
        auto status = co_await this->_refresh_buffers();
        if(!status)
            co_return status;

        for(auto* rbuf = this->_rbufs->begin(); rbuf < this->_rbufs->end(); ++rbuf)
        {
            if(rbuf->is_eof())
                continue;

            if(rbuf->buffer_empty())
            {
                auto s = co_await rbuf->refresh();
                if(!s)
                    co_return s;
                if(rbuf->is_eof())
                    continue;
            }

            scan_source_t src = rbuf;
            auto maybe_entry = source_pop_entry(src);
            if(!maybe_entry)
                co_return maybe_entry.error();

            this->_slots.push_back({std::move(maybe_entry.value()), src});
            this->_heap.push_back({&this->_slots.back()});
            std::ranges::push_heap(this->_heap, range_iterator::_heap_cmp());
        }

        for(auto& mc : this->_mem_cursors)
        {
            if(mc.is_eof())
                continue;

            scan_source_t src = &mc;
            auto maybe_entry = source_pop_entry(src);
            if(!maybe_entry)
                co_return maybe_entry.error();

            this->_slots.push_back({std::move(maybe_entry.value()), src});
            this->_heap.push_back({&this->_slots.back()});
            std::ranges::push_heap(this->_heap, range_iterator::_heap_cmp());
        }

        this->_initialized = true;
        co_return hedge::ok();
    }

    expected<std::pair<key_t, value_t>> range_iterator::_emit(merge_entry_t& item)
    {
        if(this->_lower && item.key < *this->_lower)
            return hedge::error("", errc::SKIP);

        if(this->_upper && *this->_upper < item.key)
        {
            this->_exhausted = true;
            return hedge::error("End of scan", errc::END_OF_SCAN);
        }

        auto maybe_value = value_from_span(item.value);
        if(!maybe_value.has_value())
            return hedge::error("Failed to parse value: " + maybe_value.error().to_string());

        return std::pair{std::move(item.key), std::move(maybe_value.value())};
    };

    tmc::task<hedge::expected<std::pair<key_t, value_t>>> range_iterator::_next_inner()
    {
        while(true)
        {
            if(this->_heap.empty())
                co_return hedge::error("eof", errc::END_OF_SCAN);

            std::ranges::pop_heap(this->_heap, range_iterator::_heap_cmp());
            auto* slot = this->_heap.back().slot;
            this->_heap.pop_back();

            this->_dedup.push(std::move(slot->entry));

            // Refill heap from the slot's source
            if(!source_is_eof(slot->source)) [[likely]]
            {
                if(source_buffer_empty(slot->source)) [[unlikely]]
                {
                    auto* rbuf = std::get<sst_stream*>(slot->source);
                    auto s = co_await rbuf->refresh();
                    if(!s)
                        co_return hedge::error("Failed to refresh scan buffer: " + s.error().to_string());
                }

                if(!source_is_eof(slot->source) && !source_buffer_empty(slot->source))
                {
                    auto maybe_new_entry = source_pop_entry(slot->source);
                    if(!maybe_new_entry) [[unlikely]]
                        co_return maybe_new_entry.error();

                    slot->entry = std::move(maybe_new_entry.value());
                    this->_heap.push_back({slot});
                    std::ranges::push_heap(this->_heap, range_iterator::_heap_cmp());
                }
            }

            if(this->_dedup.ready())
            {
                auto item = this->_dedup.pop();
                auto result = this->_emit(item);

                if(!result.has_value())
                {
                    if(result.error().code() == errc::END_OF_SCAN)
                        co_return result.error();

                    // another error could be SKIP (skip keys below lower bound)
                    continue;
                }

                co_return std::move(result.value());
            }
        }
    }

    tmc::task<expected<std::pair<key_t, value_t>>> range_iterator::next()
    {
        if(!this->_initialized)
        {
            auto s = co_await this->_init();
            if(!s)
                co_return hedge::error("Failed to initialize scan iterator: " + s.error().to_string());
        }

        if(this->_exhausted)
            co_return hedge::error("End of scan", errc::END_OF_SCAN);

        // Main loop: advance the merge until we have a valid entry to return
        auto maybe_kv = co_await this->_next_inner();
        if(maybe_kv.has_value())
            co_return std::move(maybe_kv.value());

        if(maybe_kv.has_error() && maybe_kv.error().code() != errc::END_OF_SCAN)
            co_return hedge::error("Failed to get next item from scan iterator: " + maybe_kv.error().to_string());

        // Drain the dedup lag (up to 2 items)
        if(this->_dedup.ready())
        {
            auto item = this->_dedup.pop();
            auto result = this->_emit(item);

            if(!result.has_value())
            {
                if(result.error().code() == errc::END_OF_SCAN)
                    co_return result.error();

                // Below lower bound — fall through to force_pop
            }
            else
            {
                co_return std::move(result.value());
            }
        }

        // Last item from dedup
        auto item = this->_dedup.force_pop();
        this->_exhausted = true;

        auto result = this->_emit(item);
        if(!result.has_value())
            co_return result.error();

        co_return std::move(result.value());
    }

} // namespace hedge::db
