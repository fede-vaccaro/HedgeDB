#include "io_executor.h"
#include "perf_counter.h"
#include "types.h"
#include "utils.h"
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <tsl/robin_map.h>
#include <tsl/sparse_map.h>
#include <utility>

#include "cache.h"

namespace hedge::db
{
    page_cache::page_cache(size_t bytes)
    {
        this->_max_page_capacity = hedge::ceil(bytes, PAGE_SIZE_IN_BYTES);

        this->_frames = std::vector<_metadata>(this->_max_page_capacity); // NOLINT
        this->_lut.reserve(this->_max_page_capacity);

        auto* ptr = aligned_alloc(PAGE_SIZE_IN_BYTES, this->_max_page_capacity * PAGE_SIZE_IN_BYTES);
        if(ptr == nullptr)
            throw std::runtime_error("Failed to allocate memory for page cache");

        auto* uint_ptr = static_cast<uint8_t*>(ptr);
        std::fill_n(uint_ptr, this->_max_page_capacity * PAGE_SIZE_IN_BYTES, 0);
        this->_data = std::unique_ptr<uint8_t>(uint_ptr);
    }

    struct awaitable_page_guard;

    using page_guard = page_cache::page_guard;

    page_guard::page_guard(uint8_t* data, size_t idx, _metadata* frame)
        : data(data),
          idx(idx),
          _frame(frame)
    {
        assert(this->_frame != nullptr);
    }

    page_guard::page_guard(page_guard&& other) noexcept
        : data(std::exchange(other.data, nullptr)),
          idx(std::exchange(other.idx, 0)),
          _frame(std::exchange(other._frame, nullptr))
    {
    }

    page_guard& page_guard::operator=(page_guard&& other) noexcept
    {
        this->data = std::exchange(other.data, nullptr);
        this->idx = std::exchange(other.idx, 0);

        if(this->_frame != nullptr)
        {
            this->_on_destruction();
            this->_frame = nullptr;
        }

        return *this;
    }

    uint8_t* page_data = nullptr;

    page_guard::~page_guard()
    {
        if(this->_frame == nullptr)
            return;

        this->_on_destruction();
    }

    void page_cache::page_guard::_on_destruction()
    {
        // Enter only if ready bit == 0, meaning that the page_guard owner is a writer
        if((this->_frame->flags.fetch_or(PAGE_FLAG_READY) & PAGE_FLAG_READY) == 0)
        {
            assert((this->_frame->flags.load() & PAGE_FLAG_READY) != 0);

            using waiter_container_t = decltype(_metadata::waiters);
            waiter_container_t waiters;

            {
                std::lock_guard lk(this->_frame->waiters_mutex);
                waiters = std::exchange(this->_frame->waiters, waiter_container_t{});
            }

            prof::avg_stat::PERF_STATS["resumed_count"].add(waiters.size(), 0);
            if(!waiters.empty())
                prof::avg_stat::PERF_STATS["avg_resumed_count"].add(waiters.size(), 1);

            for(auto& [task, continuation] : waiters)
                async::this_thread_executor()->transfer_task(std::move(task), continuation);
        }

        this->_frame->flags.fetch_sub(1);
    }

    page_guard page_cache::get_write_slot(page_tag page)
    {
        while(true)
        {
            size_t idx = this->_find_frame() % this->_max_page_capacity;
            _metadata* frame_ptr = &this->_frames[idx];

            {
                std::lock_guard lk(this->_m);

                // still the only one watching it!
                if((frame_ptr->flags.load() & PAGE_FLAG_REFERENCE_COUNT_MASK) != 1)
                {
                    frame_ptr->flags.fetch_sub(1); // Release counter
                    continue;                      // otherwise, look for another one
                }

                this->_lut.erase(frame_ptr->key);
                this->_lut[page] = idx;

                frame_ptr->flags.fetch_or(PAGE_FLAG_RECENTLY_USED); // set recently used flag
                frame_ptr->flags.fetch_and(~PAGE_FLAG_READY);       // unset ready flag
                frame_ptr->waiters.reserve(4);
                frame_ptr->key = page;
            }

            return {
                this->_data.get(),
                idx * 4096,
                frame_ptr};
        }
    }

    std::optional<page_cache::awaitable_page_guard> page_cache::lookup(page_tag page, bool hint_evict)
    {
        size_t idx = 0;
        _metadata* frame_ptr = nullptr;

        {
            std::shared_lock lk(this->_m);

            auto it = this->_lut.find(page);
            if(it == this->_lut.end())
                return std::nullopt;

            idx = it.value();

            frame_ptr = &this->_frames[idx];

            if(hint_evict)
                frame_ptr->flags.fetch_and(~PAGE_FLAG_RECENTLY_USED);
            else
                frame_ptr->flags.fetch_or(PAGE_FLAG_RECENTLY_USED);

            frame_ptr->flags.fetch_add(1); // increase reference count
        }

        return awaitable_page_guard{.pg = page_guard{this->_data.get(), idx * 4096, frame_ptr}};
    }

    std::optional<page_cache::awaitable_page_guard> page_cache::try_lookup(page_tag page, bool hint_evict)
    {
        size_t idx = 0;
        _metadata* frame_ptr = nullptr;

        {
            std::shared_lock lk(this->_m);

            auto it = this->_lut.find(page);
            if(it == this->_lut.end())
                return std::nullopt;

            idx = it.value();

            frame_ptr = &this->_frames[idx];

            // avoid co-awaiting on the page and transferring coroutine to an external executor
            if((frame_ptr->flags.load() & PAGE_FLAG_READY) == 0UL)
                return std::nullopt;

            frame_ptr->flags.fetch_add(1); // increase reference count

            if(hint_evict)
                frame_ptr->flags.fetch_and(~PAGE_FLAG_RECENTLY_USED);
            else
                frame_ptr->flags.fetch_or(PAGE_FLAG_RECENTLY_USED);
        }

        return awaitable_page_guard{.pg = page_guard{this->_data.get(), idx * 4096, frame_ptr}};
    }

    std::vector<std::optional<page_cache::awaitable_page_guard>> shared_page_cache::lookup_range(uint32_t fd, size_t start_page_index, size_t num_pages, bool hint_evict)
    {
        std::vector<std::optional<page_cache::awaitable_page_guard>> results;
        results.reserve(num_pages);

        for(size_t i = 0; i < num_pages; ++i)
        {
            auto tag = page_tag{.fd = fd, .page_index = static_cast<uint32_t>(start_page_index + i)};

            size_t hash = std::hash<page_tag>{}(tag);

            results.emplace_back(this->_caches.get()[hash % this->_num_caches].try_lookup(tag, hint_evict));
            // results.emplace_back(std::nullopt);
        }

        return results;
    }

    std::vector<page_cache::page_guard> shared_page_cache::get_write_slots_range(uint32_t fd, size_t start_page_index, size_t num_pages)
    {
        std::vector<page_cache::page_guard> results;
        results.reserve(num_pages);

        for(size_t i = 0; i < num_pages; ++i)
        {
            auto tag = page_tag{.fd = fd, .page_index = static_cast<uint32_t>(start_page_index + i)};
            results.emplace_back(this->get_write_slot(tag));
        }

        return results;
    }

    size_t page_cache::_find_frame()
    {
        size_t cur_pos = this->_clock_hand.fetch_add(1);

        if(cur_pos < this->_max_page_capacity)
        {
            this->_frames[cur_pos].flags.fetch_add(1);
            return cur_pos;
        }

        // Search for an evictable page
        // If no page is found, this procedure behaves basically as a spin-lock
        while(true)
        {
            // Advance clock head
            auto& frame = this->_frames[cur_pos % this->_max_page_capacity];

            uint64_t curr = frame.flags.load();
            uint64_t desired = (curr & PAGE_FLAG_READY) | 1; // Set reference count to 1, keep ready flag

            bool curr_is_not_recently_used = (curr & PAGE_FLAG_RECENTLY_USED) == 0;
            bool curr_is_not_referenced = (curr & PAGE_FLAG_REFERENCE_COUNT_MASK) == 0;

            if(curr_is_not_referenced && curr_is_not_recently_used && frame.flags.compare_exchange_strong(curr, desired))
                return cur_pos;

            frame.flags.fetch_and(~PAGE_FLAG_RECENTLY_USED); // Unset recently used
            cur_pos = this->_clock_hand.fetch_add(1);
        }
    }

} // namespace hedge::db
