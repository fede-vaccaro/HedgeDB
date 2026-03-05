#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace hedge
{
    class single_buffer_arena_allocator
    {
    public:
        explicit single_buffer_arena_allocator(size_t size)
            : _capacity(size),
              _offset(0),
              _buffer(std::make_unique<uint8_t[]>(size))
        {
            // Ensure the base buffer is at least 8-byte aligned.
            // On most 64-bit systems, new[] returns 16-byte aligned memory.
            assert(reinterpret_cast<uintptr_t>(this->_buffer.get()) % 8 == 0);
        }

        single_buffer_arena_allocator(const single_buffer_arena_allocator&) = delete;
        single_buffer_arena_allocator& operator=(const single_buffer_arena_allocator&) = delete;
        single_buffer_arena_allocator(single_buffer_arena_allocator&&) = delete;
        single_buffer_arena_allocator& operator=(single_buffer_arena_allocator&&) = delete;
        ~single_buffer_arena_allocator() = default;

        std::span<uint8_t> allocate(size_t bytes)
        {
            constexpr size_t alignment = 8;
            size_t aligned_bytes = (bytes + alignment - 1) & ~(alignment - 1);

            size_t current_offset = this->_offset.load(std::memory_order_relaxed);

            while(true)
            {
                if(current_offset + aligned_bytes > this->_capacity)
                {
                    return {};
                }

                if(this->_offset.compare_exchange_weak(current_offset,
                                                       current_offset + aligned_bytes,
                                                       std::memory_order_relaxed))
                {
                    return {this->_buffer.get() + current_offset, bytes};
                }
                // current_offset is updated by compare_exchange_weak on failure
            }
        }

        [[nodiscard]] size_t capacity() const
        {
            return this->_capacity;
            return this->_capacity;
        }

        [[nodiscard]] size_t used() const
        {
            return this->_offset.load(std::memory_order_relaxed);
        }

    private:
        size_t _capacity;
        std::atomic<size_t> _offset;
        std::unique_ptr<uint8_t[]> _buffer;
    };
} // namespace hedge
