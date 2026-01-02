#pragma once

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <type_traits>
#include <uuid.h>

#include "types.h"

namespace hedge
{
    inline std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext)
    {
        return path.string() + ext.data();
    }

    template <typename T>
    inline std::span<T> view_as(std::vector<uint8_t>& vector)
    {
        auto start_ptr = typename std::vector<T>::iterator(reinterpret_cast<T*>(vector.data()));
        auto end_ptr = typename std::vector<T>::iterator(reinterpret_cast<T*>(vector.data() + vector.size()));

        return std::span<T>(start_ptr, end_ptr);
    }

    template <typename T>
    auto& byte_varray_view(T& value)
    {
        using U = std::remove_reference_t<T>;

        return reinterpret_cast<const std::array<uint8_t, sizeof(U)>&>(value);
    }

    template <typename T>
    constexpr T ceil(T value, T divisor)
    {
        return (value + divisor - 1) / divisor;
    }

    inline uint16_t extract_prefix(const uuids::uuid& key)
    {
        const auto& array_view = byte_varray_view(key);

        size_t prefix = 0;

        // little endian!
        prefix |= static_cast<uint16_t>(array_view[1]);
        prefix |= static_cast<uint16_t>(array_view[0]) << 8;

        return prefix;
    }

    /*
        a partition is identified by its (included) upper bound
        e.g. 00fa is the upper bound of the partition [0000, 00ff]
        and might include elements up to 00fa ffff ffff ffff
        element starting with 01fb will be in the subsequent partition
     */
    inline size_t find_partition_prefix_for_key(key_t key, size_t partition_size)
    {
        const auto& K = extract_prefix(key);
        const auto& P = partition_size;

        return ((K + P) / P * P) - 1;
    };

    [[nodiscard]] std::pair<std::string, std::string> format_prefix(uint16_t prefix);

    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions);

    // Needed for page aligned buffers
    template <typename T>
    struct page_aligned_buffer
    {
    private:
        std::unique_ptr<uint8_t> buf;
        size_t _size;

    public:
        page_aligned_buffer() = default;
        explicit page_aligned_buffer(size_t s) : _size(s)
        {
            size_t mem = hedge::ceil(s * sizeof(T), PAGE_SIZE_IN_BYTES) * PAGE_SIZE_IN_BYTES;

            buf = std::unique_ptr<uint8_t>(static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE_IN_BYTES, mem)));
            assert(buf);

            std::fill(buf.get(), buf.get() + mem, 0);
        }

        T* data()
        {
            return reinterpret_cast<T*>(buf.get());
        }

        const T* data() const
        {
            return reinterpret_cast<T*>(buf.get());
        }

        T* begin()
        {
            return this->data();
        }

        T* end()
        {
            return this->data() + _size;
        }

        const T* begin() const
        {
            return this->data();
        }

        const T* end() const
        {
            return this->data() + _size;
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

        void resize(size_t size)
        {
            assert(size <= this->_size);
            std::fill(this->data() + size, this->data() + this->_size, T{});
            this->_size = size;
        }
    };

} // namespace hedge