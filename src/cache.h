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
        assert(offset % PAGE_SIZE_IN_BYTES == 0);

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

            h = (static_cast<size_t>(page_id.fd) << 32);
            h |= page_id.page_index;

            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdULL;
            h ^= h >> 33;

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
    class alignas(64) page_cache
    {
        friend class PageCacheTest;

        async::rw_spinlock _m{};
        std::atomic_size_t _clock_hand{};
        size_t _max_page_capacity{};

        struct alignas(64) _metadata
        {
            std::atomic_uint64_t flags{0};
            async::spinlock waiters_mutex{};

            using coro_frame_t = std::pair<async::task<>, std::coroutine_handle<>>;
            std::vector<coro_frame_t> waiters{}; // root task and this continuation
            page_tag key{};
        };

        std::vector<_metadata> _frames;
        tsl::sparse_map<page_tag, size_t> _lut;
        std::unique_ptr<uint8_t> _data;

    public:
        explicit page_cache(size_t bytes);

        struct awaitable_page_guard;

        // Handles reading a page. Only releases reference count.
        struct read_page_guard
        {
            read_page_guard() = default;
            read_page_guard(uint8_t* data, size_t idx, _metadata* frame);
            ~read_page_guard();

            read_page_guard(const read_page_guard&) = delete;
            read_page_guard& operator=(const read_page_guard&) = delete;

            read_page_guard(read_page_guard&& other) noexcept;
            read_page_guard& operator=(read_page_guard&& other) noexcept;

            uint8_t* data{nullptr};
            size_t idx{0};

            const _metadata* frame() const { return this->_frame; }

        private:
            friend struct awaitable_page_guard;
            _metadata* _frame{nullptr};
        };

        // Handles writing a page. Resumes waiters and sets READY flag on destruction.
        struct write_page_guard
        {
            write_page_guard() = default;
            write_page_guard(uint8_t* data, size_t idx, _metadata* frame);
            ~write_page_guard();

            write_page_guard(const write_page_guard&) = delete;
            write_page_guard& operator=(const write_page_guard&) = delete;

            write_page_guard(write_page_guard&& other) noexcept;
            write_page_guard& operator=(write_page_guard&& other) noexcept;

            uint8_t* data{nullptr};
            size_t idx{0};

        private:
            friend struct awaitable_page_guard;
            _metadata* _frame{nullptr};
        };

        struct awaitable_page_guard
        {
            read_page_guard pg;

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

            read_page_guard&& await_resume()
            {
                return std::move(this->pg);
            }
        };

        write_page_guard get_write_slot(page_tag page);
        std::optional<awaitable_page_guard> lookup(page_tag page, bool hint_evict = false);
        std::optional<awaitable_page_guard> try_lookup(page_tag page, bool hint_evict = false);

    private:
        size_t _find_frame();
    };

    class shared_page_cache
    {
        std::unique_ptr<page_cache> _caches;
        size_t _num_caches;

    public:
        explicit shared_page_cache(size_t bytes, size_t num_caches)
        {
            void* caches = aligned_alloc(alignof(page_cache), sizeof(page_cache) * num_caches);
            if(caches == nullptr)
                throw std::runtime_error("Could not allocate memory for shared_page_cache caches");

            this->_caches = std::unique_ptr<page_cache>(static_cast<page_cache*>(caches));
            this->_num_caches = num_caches;
            size_t per_cache_bytes = hedge::ceil(bytes, num_caches);

            for(size_t i = 0; i < num_caches; ++i)
                new(this->_caches.get() + i) page_cache(per_cache_bytes);
        }

        page_cache::write_page_guard get_write_slot(page_tag page)
        {
            size_t hash = std::hash<page_tag>{}(page) % this->_num_caches;
            return this->_caches.get()[hash].get_write_slot(page);
        }

        std::optional<page_cache::awaitable_page_guard> lookup(page_tag page)
        {
            size_t hash = std::hash<page_tag>{}(page) % this->_num_caches;
            return this->_caches.get()[hash].lookup(page);
        }

        std::vector<page_cache::write_page_guard> get_write_slots_range(uint32_t fd, size_t start_page_index, size_t num_pages);
        std::vector<std::optional<page_cache::awaitable_page_guard>> lookup_range(uint32_t fd, size_t start_page_index, size_t num_pages, bool hint_evict);
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