#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "error.hpp"
#include "types.h"
#include "utils.h"

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
    // Page cache
    // Implemented using a reference-counted CLOCK algorithm;
    class page_cache
    {
        std::shared_mutex _m{};
        size_t _clock_head{};
        size_t _size{};
        size_t _max_page_capacity{};

        // The key is not stored here to obtain the correct cache alignement
        struct alignas(64) _metadata
        {
            std::shared_mutex m{};

            std::atomic_uint32_t reference_count{0};
            std::atomic_uint8_t recently_used{0};
        };

        std::unique_ptr<_metadata> _frames;        // Metadata about each pages - ~16 MB overhead for 1 GB worth of pages
        std::vector<page_tag> _keys;               // idx -> keys mapping, needed on eviction - ~2MB overhead for 1 GB worth of pages
        std::unordered_map<page_tag, size_t> _lut; // key -> idx mapping - IDK how to estimate the overhead here, damn you std::unordered_map!

        std::unique_ptr<uint8_t> _data; // Memory arena

    public:
        using lock_t = std::variant<std::monostate, std::shared_lock<std::shared_mutex>, std::unique_lock<std::shared_mutex>>;

        explicit page_cache(size_t bytes)
        {
            this->_max_page_capacity = hedge::ceil(bytes, PAGE_SIZE_IN_BYTES);

            this->_keys = std::vector<page_tag>(this->_max_page_capacity);

            this->_frames = std::unique_ptr<_metadata>(new _metadata[this->_max_page_capacity]); // NOLINT
            this->_lut.reserve(this->_max_page_capacity);

            auto* ptr = aligned_alloc(PAGE_SIZE_IN_BYTES, this->_max_page_capacity * PAGE_SIZE_IN_BYTES);
            if(ptr == nullptr)
                throw std::runtime_error("Failed to allocate memory for page cache");

            auto* uint_ptr = static_cast<uint8_t*>(ptr);
            std::fill_n(uint_ptr, this->_max_page_capacity * PAGE_SIZE_IN_BYTES, 0);
            this->_data = std::unique_ptr<uint8_t>(uint_ptr);
        }

        struct page_guard
        {
            page_guard() = default;
            page_guard(lock_t lk, uint8_t* page, std::atomic_uint32_t* counter)
                : page_data(page),
                  _lock(std::move(lk)),
                  _counter_ptr(counter)
            {
                assert(counter != nullptr);
                this->_counter_ptr->fetch_add(1, std::memory_order::relaxed);
            }

            explicit page_guard(page_guard&& other) noexcept
                : page_data(other.page_data),
                  _lock(std::exchange(other._lock, std::monostate{})),
                  _counter_ptr(std::exchange(other._counter_ptr, nullptr))
            {
            }

            page_guard& operator=(page_guard&& other) noexcept
            {
                this->_lock = std::exchange(other._lock, std::monostate{});
                this->page_data = std::exchange(other.page_data, nullptr);
                this->_counter_ptr = std::exchange(this->_counter_ptr, nullptr);

                return *this;
            }

            uint8_t* page_data;

            ~page_guard()
            {
                // this->confirm_written();
                if(this->_counter_ptr != nullptr)
                    this->_counter_ptr->fetch_sub(1, std::memory_order::relaxed);
            }

        private:
            lock_t _lock;
            std::atomic_uint32_t* _counter_ptr;
        };

        hedge::expected<page_guard> get_write_slot(page_tag page)
        {
            std::lock_guard lk(this->_m);
            this->_find_frame();

            auto& frame = this->_frames.get()[this->_clock_head];

            // Set 'recently_used' bit (also known as 'reference_bit' in literature)
            frame.recently_used.store(1, std::memory_order::relaxed);

            // set lut(s) to point to the correct position
            this->_keys[this->_clock_head] = page;
            this->_lut[page] = this->_clock_head;

            // It should always possible to lock as the reference count is zero
            // Otherwise we could not have evicted this page before
            // Also, here we have the cache lock, so it is not possible that some other thread
            // Has any (shared) ownership of this
            // ..frame
            auto slk = std::unique_lock(frame.m, std::defer_lock_t{});
            bool locked = slk.try_lock();
            if(!locked)
                return hedge::error("busy lock", errc::BUSY);

            return {
                std::move(slk),
                this->_data.get() + (this->_clock_head * 4096),
                &frame.reference_count};
        }

        hedge::expected<std::optional<page_guard>> lookup(page_tag page)
        {
            std::shared_lock lk(this->_m);

            auto it = this->_lut.find(page);
            if(it == this->_lut.end())
                return std::nullopt;

            auto idx = it->second;

            auto& frame = this->_frames.get()[idx];

            auto frame_lock = std::shared_lock(frame.m, std::defer_lock_t{});

            bool locked = frame_lock.try_lock();
            if(!locked)
                return hedge::error("lock busy", hedge::errc::BUSY);

            frame.recently_used.store(1, std::memory_order::relaxed);

            return std::make_optional<page_guard>(page_guard(
                std::move(frame_lock),
                this->_data.get() + (idx * 4096),
                &frame.reference_count));
        }

        size_t capacity() const
        {
            return this->_max_page_capacity;
        }

        size_t size() const
        {
            return this->_size;
        }

    private:
        void _find_frame()
        {
            if(this->_size < this->_max_page_capacity)
            {
                this->_clock_head = ++this->_size % this->_max_page_capacity;
                return;
            }

            // Search for an evictable page
            // If no page is found, this procedure behaves basically as a spin-lock
            while(true)
            {
                auto& frame = this->_frames.get()[this->_clock_head];

                // a non-referenced page will be evicted
                if(frame.recently_used.load(std::memory_order_relaxed) == 0U &&
                   frame.reference_count.load(std::memory_order_relaxed) == 0U)
                {
                    this->_lut.erase(this->_keys[this->_clock_head]);
                    return;
                }

                // Give the page a second chance, set the ref to 0
                // It makes the page associated to the frame "evictable", once the reference count goes to zero
                frame.recently_used.store(0, std::memory_order::relaxed);

                // Advance clock head
                this->_clock_head = (this->_clock_head + 1) % this->_max_page_capacity;
            }
        }
    };

    class point_cache
    {
        std::shared_mutex _m{};
        std::unordered_map<key_t, value_ptr_t> _lut;
        size_t _max_page_capacity;

    public:
        explicit point_cache(size_t bytes) : _max_page_capacity(hedge::ceil(bytes, sizeof(index_entry_t)))
        {
            this->_lut.reserve(this->_max_page_capacity);
        }

        void put(key_t key, value_ptr_t value_ptr)
        {
            std::lock_guard lk(this->_m);
            if(this->_lut.size() >= this->_max_page_capacity)
                this->_lut.erase(this->_lut.begin());

            this->_lut[key] = value_ptr;
        }

        std::optional<value_ptr_t> lookup(key_t key)
        {
            std::shared_lock lk(this->_m);
            auto it = this->_lut.find(key);
            if(it == this->_lut.end())
                return std::nullopt;

            return it->second;
        }

        size_t capacity() const
        {
            return this->_max_page_capacity;
        }
    };

} // namespace hedge::db
