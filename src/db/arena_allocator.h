#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <vector>

#include "utils.h"
namespace hedge::db
{

    template <typename T>
    class arena_allocator
    {
    public:
        arena_allocator(size_t budget, size_t extent_bytes_hint = 4 * 1024 * 1024)
            : _budget(budget)
        {
            constexpr size_t MIN_EXTENT_SIZE_BYTES = hedge::ceil(4096UL, sizeof(T)) * sizeof(T);

            extent_bytes_hint = hedge::ceil(extent_bytes_hint, sizeof(T)) * sizeof(T);

            this->_extent_size = std::max(extent_bytes_hint, MIN_EXTENT_SIZE_BYTES);

            budget = std::max(this->_extent_size, budget);

            this->_items_per_extent = std::max(this->_extent_size / sizeof(T), 1UL);

            size_t n_extents = hedge::ceil(budget, this->_extent_size);
            this->_extents.reserve(n_extents);

            this->_curr_extent = this->_allocate_new_extent();
        }

        ~arena_allocator()
        {
            if(!this->_extents.empty())
            {
                this->_extents.back().count = this->_current_slot_index;
            }
        }

        arena_allocator(const arena_allocator&) = delete;
        arena_allocator& operator=(const arena_allocator&) = delete;

        /**
         * @brief Allocates n_items contiguous objects of type T, aligned to alignment items.
         * @return Pointer to allocated memory, or nullptr if budget exceeded.
         */
        T* allocate_many(size_t n_items, size_t alignment)
        {
            return this->_allocate_many_impl(hedge::round_up(n_items, alignment));
        }

    private:
        size_t _budget;
        size_t _extent_size;
        size_t _items_per_extent;
        size_t _total_allocated{0};

        T* _curr_extent{nullptr};
        size_t _current_slot_index{0};

        struct extent_t
        {
            std::unique_ptr<void, decltype(&std::free)> memory;
            size_t count{0};

            extent_t(void* mem, decltype(&std::free) free_func) : memory(mem, free_func) {}

            ~extent_t()
            {
                if constexpr(!std::is_trivially_destructible_v<T>)
                {
                    if(memory)
                    {
                        T* ptr = static_cast<T*>(memory.get());
                        for(size_t i = 0; i < count; ++i)
                            ptr[count - 1 - i].~T();
                    }
                }
            }

            extent_t(extent_t&&) = default;
            extent_t& operator=(extent_t&&) = default;

            extent_t(const extent_t&) = delete;
            extent_t& operator=(const extent_t&) = delete;
        };

        std::vector<extent_t> _extents;

        T* _allocate_many_impl(size_t n_items)
        {
            if(this->_current_slot_index + n_items > this->_items_per_extent) [[unlikely]]
            {
                if(n_items > this->_items_per_extent) [[unlikely]]
                    return nullptr;

                this->_curr_extent = this->_allocate_new_extent();
                this->_current_slot_index = 0;
            }

            if(this->_curr_extent == nullptr) [[unlikely]]
                return nullptr;

            T* ptr = this->_curr_extent + this->_current_slot_index;
            this->_current_slot_index += n_items;
            return ptr;
        }

        T* _allocate_new_extent()
        {
            if(!this->_extents.empty())
                this->_extents.back().count = this->_current_slot_index;

            if(this->_total_allocated + this->_extent_size > this->_budget)
                return nullptr;

            void* new_extent_mem{nullptr};

            constexpr size_t block_alignment = std::max(size_t(CACHE_LINE_SIZE), alignof(T));
            if(posix_memalign(&new_extent_mem, block_alignment, this->_extent_size) != 0)
                return nullptr;

            this->_total_allocated += this->_extent_size;
            this->_extents.emplace_back(new_extent_mem, std::free);

            return reinterpret_cast<T*>(new_extent_mem);
        }
    };

} // namespace hedge::db
