#include <algorithm>

#include "cache.h"
#include "db/scan_iterator.h"
#include "error.hpp"
#include "sst.h"
#include "types.h"

namespace hedge::db
{

    hedge::expected<scan_iterator> scan_iterator::from_partition(
        const partition_t& partition,
        std::optional<key_t> lower,
        std::optional<key_t> upper,
        std::shared_ptr<sharded_page_cache> cache,
        size_t read_ahead_size)
    {

        // Collect SSTs that overlap with the range and compute their page ranges
        std::vector<sst_ptr_t> matching_ssts;
        std::vector<page_range> ranges;

        for(const auto& level : partition)
        {
            for(const auto& sst_ptr : level)
            {
                auto range = sst_ptr->find_range(lower, upper);
                if(!range)
                    continue;

                matching_ssts.push_back(sst_ptr);
                ranges.push_back(*range);
            }
        }

        // Build rolling buffers for each matching SST
        if(!matching_ssts.empty())
            read_ahead_size = std::max(read_ahead_size / matching_ssts.size(), static_cast<size_t>(PAGE_SIZE_IN_BYTES));

        if(!is_page_aligned(read_ahead_size))
            read_ahead_size = hedge::ceil_page_align(read_ahead_size);

        auto rbufs = std::make_unique<rolling_buffer_set>(matching_ssts.size());

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
                                read_ahead_size,
                                cache);
        }

        return scan_iterator{
            std::move(matching_ssts),
            std::move(rbufs),
            std::move(cache),
            std::move(lower),
            std::move(upper)};
    };

    scan_iterator::scan_iterator(std::vector<sst_ptr_t> ssts,
                                 std::unique_ptr<rolling_buffer_set> rbufs,
                                 std::shared_ptr<sharded_page_cache> cache,
                                 std::optional<key_t> lower,
                                 std::optional<key_t> upper)
        : _ssts(std::move(ssts)),
          _rbufs(std::move(rbufs)),
          _cache(std::move(cache)),
          _lower(std::move(lower)),
          _upper(std::move(upper))
    {
        this->_heap.reserve(_ssts.size());
    }

    tmc::task<hedge::status> scan_iterator::_refresh_buffers()
    {
        for(auto* it = this->_rbufs->begin(); it < this->_rbufs->end(); ++it)
        {
            if(auto status = co_await it->refresh(this->_cache); !status)
                co_return hedge::error("Failed to refresh scan buffer: " + status.error().to_string());
        }

        co_return hedge::ok();
    }

    tmc::task<hedge::status> scan_iterator::_init()
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
                auto s = co_await this->_refresh_buffers();
                if(!s)
                    co_return s;
            }

            merge_entry_t entry{
                .key = rbuf->front().key(),
                .value = rbuf->front().value(),
                .epoch = rbuf->index().epoch()};

            rbuf->pop_front();

            this->_heap.push_back({std::move(entry), rbuf, entry.epoch});
            std::ranges::push_heap(this->_heap, scan_iterator::_heap_cmp());
        }

        this->_initialized = true;
        co_return hedge::ok();
    }

    expected<std::pair<key_t, value_t>> scan_iterator::_emit(merge_entry_t& item)
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

    tmc::task<hedge::expected<std::pair<key_t, value_t>>> scan_iterator::_next_inner()
    {
        while(true)
        {
            if(this->_heap.empty())
                co_return hedge::error("eof", errc::END_OF_SCAN);

            std::ranges::pop_heap(this->_heap, scan_iterator::_heap_cmp());
            auto [keyvalue, rbuf, epoch] = std::move(this->_heap.back());
            this->_heap.pop_back();

            this->_dedup.push(std::move(keyvalue));

            // Refill heap from the source buffer
            if(!rbuf->is_eof()) [[likely]]
            {
                if(rbuf->buffer_empty()) [[unlikely]]
                {
                    auto s = co_await this->_refresh_buffers();
                    if(!s)
                        co_return hedge::error("Failed to refresh scan buffers: " + s.error().to_string());
                }

                if(!rbuf->is_eof() && !rbuf->buffer_empty())
                {
                    merge_entry_t new_entry{
                        .key = rbuf->front().key(),
                        .value = rbuf->front().value(),
                        .epoch = rbuf->index().epoch(),
                    };

                    rbuf->pop_front();

                    this->_heap.push_back({std::move(new_entry), rbuf, new_entry.epoch});
                    std::ranges::push_heap(this->_heap, scan_iterator::_heap_cmp());
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

    tmc::task<expected<std::pair<key_t, value_t>>> scan_iterator::next()
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
