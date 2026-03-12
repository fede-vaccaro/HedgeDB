#pragma once

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace hedge::db
{
    template <typename T, size_t N>
    class stack_allocator
    {
    public:
        using propagate_on_container_move_assignment = std::false_type;
        using propagate_on_container_copy_assignment = std::false_type;
        using is_always_equal = std::false_type;

        using value_type = T;

        stack_allocator() = default;
        stack_allocator(const stack_allocator& other) : _buffer(other._buffer), _offset(other._offset) {}
        stack_allocator(stack_allocator&& other) noexcept : _buffer(std::move(other._buffer)), _offset(std::exchange(other._offset, 0)) {}

        // Required: tells the container how to rebind, preserving Policy
        template <typename U>
        struct rebind
        {
            using other = stack_allocator<U, N>;
        };

        T* allocate(size_t n)
        {
            if(this->_offset + n * sizeof(T) > N * sizeof(T))
                throw std::bad_alloc();

            auto* item_ptr = reinterpret_cast<T*>(this->_buffer.data() + this->_offset);
            this->_offset += n * sizeof(T);
            return item_ptr;
        }

        void deallocate(T* /*item*/, size_t /*n*/) noexcept
        {
            // No-op
        }

        bool operator==(const stack_allocator& other) const { return this == &other; }
        bool operator!=(const stack_allocator& other) const { return this != &other; }

    private:
        alignas(8) std::array<T, N> _buffer;
        size_t _offset{0};
    };
} // namespace hedge::db