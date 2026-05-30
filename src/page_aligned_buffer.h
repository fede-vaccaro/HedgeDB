#pragma once

#include "types.h"
#include "utils.h"
#include <cstdlib>
#include <span>

namespace hedge
{
    // std::vector<>-like data structure
    // DIRECT_IO needs page aligned (4096-alignment) buffers.
    // These buffers are meant to be written to/read from file system.
    template <typename T>
        requires(sizeof(T) < PAGE_SIZE_IN_BYTES)
    struct page_aligned_buffer
    {
    private:
        buffer_t _buf = buffer_t(nullptr, std::free);
        size_t _size{0};     // Size expressed items count, not bytes
        size_t _capacity{0}; // Capacity expressed in otem counts, not bytes

        template <bool INIT_ZERO_MEMORY = false>
        static void _allocate_buffer(page_aligned_buffer& pab, size_t s, size_t c)
        {
            size_t buf_size_bytes = hedge::ceil_page_align(c * sizeof(T));

            pab._buf = buffer_t(static_cast<std::byte*>(std::aligned_alloc(PAGE_SIZE_IN_BYTES, buf_size_bytes)), std::free);
            pab._capacity = buf_size_bytes / sizeof(T); // Due to page size alignment, actual capacity might be larger than requested
            pab._size = s;

            if constexpr(INIT_ZERO_MEMORY)
                std::fill(pab._buf.get(), pab._buf.get() + buf_size_bytes, std::byte{0});
        }

        static void _init_buffer(page_aligned_buffer& pab)
        {
            std::uninitialized_fill(pab.data(), pab.data() + pab.size(), T{});
        }

    public:
        page_aligned_buffer() = default;

        explicit page_aligned_buffer(size_t size) : _size(size)
        {
            page_aligned_buffer::_allocate_buffer(*this, size, size);
            page_aligned_buffer::_init_buffer(*this);
        }

        explicit page_aligned_buffer(size_t size, size_t capacity) : _size(size)
        {
            assert(size <= capacity);
            page_aligned_buffer::_allocate_buffer(*this, size, capacity);
            page_aligned_buffer::_init_buffer(*this);
        }

        page_aligned_buffer(page_aligned_buffer&& other) noexcept
        {
            *this = std::move(other);
        }

        page_aligned_buffer& operator=(page_aligned_buffer&& other) noexcept
        {
            if(this != &other)
            {
                this->_free();

                this->_buf = std::exchange(other._buf, nullptr);
                this->_size = std::exchange(other._size, 0);
                this->_capacity = std::exchange(other._capacity, 0);
            }
            return *this;
        }

        page_aligned_buffer(const page_aligned_buffer&) = delete;
        page_aligned_buffer& operator=(const page_aligned_buffer&) = delete;

        std::span<std::byte> as_byte_span()
        {
            return {_buf.get(), this->_size * sizeof(T)};
        }

        void* raw_data()
        {
            return this->_buf.get();
        }

        T* data()
        {
            return reinterpret_cast<T*>(this->_buf.get());
        }

        const T* data() const
        {
            return reinterpret_cast<T*>(this->_buf.get());
        }

        T* begin()
        {
            return this->data();
        }

        T* end()
        {
            return this->data() + _size;
        }

        T& back()
        {
            return this->data()[this->_size - 1];
        }

        const T& back() const
        {
            return this->data()[this->_size - 1];
        }

        const T* begin() const
        {
            return this->data();
        }

        const T* end() const
        {
            return this->data() + this->_size;
        }

        T& operator[](size_t idx)
        {
            return this->data()[idx];
        }

        const T& operator[](size_t idx) const
        {
            return this->data()[idx];
        }

        [[nodiscard]] bool empty() const
        {
            return this->_size == 0;
        }

        [[nodiscard]] size_t size() const
        {
            return this->_size;
        }

        [[nodiscard]] size_t capacity() const
        {
            return this->_capacity;
        }

        void resize(size_t new_size)
        {
            if(new_size < this->_size)
            {
                for(auto* i = this->data() + new_size; i < this->data() + this->_size; ++i)
                    i->~T();
            }
            else if(new_size > this->_size)
            {
                [[maybe_unused]] size_t old_size = this->_size;

                if(new_size > this->_capacity)
                    page_aligned_buffer::_grow(*this, new_size);

                std::fill(this->data() + old_size, this->data() + new_size, T{});
            }

            this->_size = new_size;
        }

        void free()
        {
            *this = page_aligned_buffer{};
        }

        void reserve(size_t new_capacity)
        {
            if(new_capacity > this->_capacity)
                page_aligned_buffer::_grow(*this, new_capacity);
        }

        template <typename... Args>
        void emplace_back(Args&&... args)
        {
            if(this->_size == this->_capacity) [[unlikely]]
                page_aligned_buffer::_grow(*this, this->_size * 2);

            this->_append_unsafe(std::forward<Args>(args)...);
        }

        void shrink_to_fit()
        {
            const auto aligned_size_bytes =
                hedge::ceil_page_align(this->_size * sizeof(T));

            const auto aligned_capacity_bytes = this->_capacity * sizeof(T);

            if(aligned_size_bytes < aligned_capacity_bytes)
                page_aligned_buffer::_grow(*this, this->_size);
        }

        ~page_aligned_buffer()
        {
            this->_free();
        }

    private:
        void _free()
        {
            if(this->data() != nullptr)
            {
                for(auto* ptr = this->data(); ptr < this->data() + this->_size; ++ptr)
                    ptr->~T();
            }

            this->_buf.reset();
            this->_size = 0;
            this->_capacity = 0;
        }

        template <typename... Args>
        void _append_unsafe(Args&&... args)
        {
            ::new(this->data() + this->_size++) T{std::forward<Args>(args)...};
        }

        static void _grow(page_aligned_buffer& buf, size_t new_capacity)
        {
            assert(buf._size <= new_capacity);
            auto new_pab = page_aligned_buffer<T>();
            page_aligned_buffer::_allocate_buffer(new_pab, buf.size(), new_capacity);
            std::uninitialized_move(buf.begin(), buf.end(), new_pab.begin());
            buf = std::move(new_pab);
        }
    };
} // namespace hedge