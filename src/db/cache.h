#pragma once

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

#include "async/spinlock.h"
#include "io_executor.h"
#include "types.h"
#include "utils.h"

#include "perf_counter.h"

namespace hedge::db
{
    struct alignas(8) page_tag
    {
        uint32_t fd;
        uint32_t page_index;

        bool operator==(const page_tag& other) const
        {
            return fd == other.fd && page_index == other.page_index;
        }
    };

    inline page_tag to_page_tag(uint32_t fd, size_t offset)
    {
        if(offset % PAGE_SIZE_IN_BYTES != 0)
            throw std::runtime_error("Offset is not aligned to page size");

        return page_tag{.fd = fd, .page_index = static_cast<uint32_t>(offset / PAGE_SIZE_IN_BYTES)};
    }
} // namespace hedge::db

namespace std
{
    template <>
    struct hash<hedge::db::page_tag>
    {
        size_t operator()(const hedge::db::page_tag& page_id) const noexcept
        {
            size_t h;

            h = std::hash<uint32_t>{}(page_id.fd) << 32;
            h |= std::hash<uint32_t>{}(page_id.page_index);

            return h;
        }
    };
} // namespace std

namespace hedge::db
{
    constexpr uint64_t PAGE_FLAG_READY = (1ULL << 63);
    constexpr uint64_t PAGE_FLAG_RECENTLY_USED = (1ULL << 62);
    constexpr uint64_t PAGE_FLAG_REFERENCE_COUNT_MASK = ((1ULL << 62) - 1);

    // Page cache
    // Implemented using a reference-counted CLOCK algorithm;
    class page_cache
    {
        async::rw_spinlock _m{};
        std::atomic_size_t _clock_hand{};
        size_t _max_page_capacity{};

        // The key is not stored here to obtain the correct cache alignement
        struct alignas(64) _metadata
        {
            std::atomic_uint64_t flags{0};
            async::spinlock waiters_mutex{};

            using coro_frame_t = std::pair<async::task<>, std::coroutine_handle<>>;
            std::vector<coro_frame_t> waiters{}; // root task and this continuation
            page_tag key{};
        };

        std::vector<_metadata> _frames;         // Metadata about each pages - ~16 MB overhead for 1 GB worth of pages
        tsl::sparse_map<page_tag, size_t> _lut; // key -> idx mapping
        std::unique_ptr<uint8_t> _data;         // Memory arena

    public:
        explicit page_cache(size_t bytes);

        struct awaitable_page_guard;

        struct page_guard
        {
            page_guard() = default;
            page_guard(uint8_t* data, size_t idx, _metadata* frame);

            explicit page_guard(page_guard&& other) noexcept;

            page_guard& operator=(page_guard&& other) noexcept;

            uint8_t* data;
            size_t idx;

            ~page_guard();

        private:
            _metadata* _frame;

            friend awaitable_page_guard;
        };

        struct awaitable_page_guard
        {
            page_guard pg;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return (pg._frame->flags.load() & PAGE_FLAG_READY) != 0UL;
            }

            template <typename PROMISE_TYPE>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<PROMISE_TYPE> continuation) noexcept
            {

                prof::avg_stat::PERF_STATS["push_coro"].start();

                std::lock_guard lk(pg._frame->waiters_mutex);
                if((pg._frame->flags.load() & PAGE_FLAG_READY) != 0UL)
                {
                    prof::avg_stat::PERF_STATS["push_coro"].stop(true);
                    return continuation;
                }
                auto root_coro = continuation.promise()._root_coro;
                auto root_task = async::this_thread_executor()->extract_task(root_coro);

                pg._frame->waiters.emplace_back(std::move(root_task), continuation);
                prof::avg_stat::PERF_STATS["push_coro"].stop();

                return std::noop_coroutine();
            }

            page_guard&& await_resume()
            {
                return std::move(this->pg);
            }
        };

        page_guard get_write_slot(page_tag page);

        std::optional<awaitable_page_guard> lookup(page_tag page);

    private:
        size_t _find_frame();
    };

    class shared_page_cache
    {
        std::vector<std::unique_ptr<page_cache>> _caches;
        size_t _num_caches;

    public:
        explicit shared_page_cache(size_t bytes, size_t num_caches)
        {
            this->_num_caches = num_caches;
            size_t per_cache_bytes = hedge::ceil(bytes, num_caches);
            for(size_t i = 0; i < num_caches; ++i)
                this->_caches.emplace_back(std::make_unique<page_cache>(per_cache_bytes));
        }

        page_cache::page_guard get_write_slot(page_tag page)
        {
            size_t hash = std::hash<page_tag>{}(page) % this->_num_caches;
            return this->_caches[hash]->get_write_slot(page);
        }

        std::optional<page_cache::awaitable_page_guard> lookup(page_tag page)
        {
            size_t hash = std::hash<page_tag>{}(page) % this->_num_caches;
            return this->_caches[hash]->lookup(page);
        }
    };

    class point_cache
    {
        async::rw_spinlock _m{};
        tsl::robin_map<key_t, value_ptr_t> _cache;
        size_t _max_page_capacity;

    public:
        explicit point_cache(size_t bytes) : _max_page_capacity(hedge::ceil(bytes, sizeof(index_entry_t)))
        {
            this->_cache.reserve(this->_max_page_capacity);
        }

        void put(const key_t& key, const value_ptr_t& value_ptr)
        {
            std::lock_guard lk(this->_m);
            if(this->_cache.size() >= this->_max_page_capacity)
                this->_cache.erase(this->_cache.begin());

            this->_cache[key] = value_ptr;
        }

        std::optional<value_ptr_t> lookup(key_t key)
        {
            std::shared_lock lk(this->_m);
            auto it = this->_cache.find(key);
            if(it == this->_cache.end())
                return std::nullopt;

            return it->second;
        }

        size_t capacity() const
        {
            return this->_max_page_capacity;
        }
    };

} // namespace hedge::db
