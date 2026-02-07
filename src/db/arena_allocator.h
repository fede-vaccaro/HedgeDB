#pragma once

#include "utils.h"
#include <emmintrin.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <vector>

namespace hedge::db
{

    template <typename T>
    class arena_allocator
    {
    public:
        /**
         * @brief Constructs an arena allocator.
         * @param budget Maximum bytes allowed to allocate.
         * @param extent_bytes_hint Desired size of each memory block (extent) in bytes.
         */
        arena_allocator(size_t budget, size_t extent_bytes_hint = 4 * 1024 * 1024)
            : _budget(budget)
        {
            constexpr size_t MIN_EXTENT_SIZE_BYTES = hedge::ceil(4096UL, sizeof(T)) * sizeof(T);

            extent_bytes_hint = hedge::ceil(extent_bytes_hint, sizeof(T)) * sizeof(T);

            this->_extent_size = std::max(extent_bytes_hint, MIN_EXTENT_SIZE_BYTES);
            this->_items_per_extent = std::max(this->_extent_size / sizeof(T), 1UL); // at least 1 extent

            size_t n_extents = hedge::ceil(budget, this->_extent_size);
            this->_extents.reserve(n_extents);

            this->_curr_extent = this->_allocate_new_extent();
        }

        ~arena_allocator()
        {
        }

        // Disable copy/move
        arena_allocator(const arena_allocator&) = delete;
        arena_allocator& operator=(const arena_allocator&) = delete;

        /**
         * @brief Allocates one object of type T.
         * @return Pointer to allocated memory, or nullptr if budget exceeded or allocation failed.
         */
        T* allocate()
        {
            std::lock_guard lk(this->_mutex);

            if(this->_current_slot_index >= this->_items_per_extent) [[unlikely]]
            {
                this->_curr_extent = this->_allocate_new_extent();
                this->_current_slot_index = 0;
            }

            if(this->_curr_extent == nullptr) [[unlikely]]
                return nullptr;

            T* ptr = this->_curr_extent + this->_current_slot_index++;
            return ptr;
        }

        [[nodiscard]] size_t
        allocated_size() const
        {
            return this->_total_allocated.load(std::memory_order_relaxed);
        }

    private:
        size_t _budget;
        size_t _extent_size;
        size_t _items_per_extent;

        std::atomic<size_t> _total_allocated{0};

        // Current extent state
        T* _curr_extent{nullptr};
        std::size_t _current_slot_index{0};

        using extent_t = std::unique_ptr<void, decltype(&std::free)>;

        std::vector<extent_t> _extents;
        std::mutex _mutex;

        T* _allocate_new_extent()
        {
            // Check budget
            size_t current_total = this->_total_allocated.load(std::memory_order_relaxed);
            if(current_total + this->_extent_size > this->_budget)
                return nullptr;

            void* new_extent_mem{nullptr};

            constexpr size_t block_alignment = std::max(size_t(64), alignof(T)); // At least an extent should be cache aligned
            if(posix_memalign(&new_extent_mem, block_alignment, this->_extent_size) != 0)
                return nullptr;

            this->_total_allocated.fetch_add(this->_extent_size, std::memory_order_relaxed);
            this->_extents.emplace_back(new_extent_mem, std::free);

            return reinterpret_cast<T*>(new_extent_mem);
        }
    };

} // namespace hedge::db
