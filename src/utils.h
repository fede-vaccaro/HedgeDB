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
        requires std::is_integral_v<T> && std::is_unsigned_v<T>
    constexpr T ceil(T value, T denominator)
    {
        return (value + denominator - 1) / denominator;
    }

    // Usually used for page aligning sizes, so we require denominator to be a power of 2 for optimization
    template <typename T>
        requires std::is_integral_v<T> && std::is_unsigned_v<T>
    constexpr T round_up(T value, T denominator)
    {
        assert(std::has_single_bit(denominator) && "Denominator must be a power of 2");

        return (value + denominator - 1) & ~(denominator - 1);
    }

    template <typename T>
        requires std::is_integral_v<T> && std::is_unsigned_v<T>
    constexpr T ceil_page_align(T value)
    {
        return round_up(value, static_cast<T>(PAGE_SIZE_IN_BYTES));
    }

    template <typename T>
    constexpr T floor_page_align(T value)
    {
        return (value / static_cast<T>(PAGE_SIZE_IN_BYTES)) * static_cast<T>(PAGE_SIZE_IN_BYTES);
    }

    inline uint16_t extract_prefix(const uuids::uuid& key)
    {
        const auto* array_view = reinterpret_cast<const uint8_t*>(key.as_bytes().begin().base());

        size_t prefix = 0;

        // Warning: this is intended for little endian!
        prefix |= static_cast<uint16_t>(array_view[1]);
        prefix |= static_cast<uint16_t>(array_view[0]) << 8;

        return prefix;
    }

    template <typename T>
        requires std::is_integral_v<T> && std::is_unsigned_v<T>
    inline bool is_page_aligned(T value)
    {
        return (value % PAGE_SIZE_IN_BYTES) == 0;
    }

    inline uint16_t extract_prefix(const uint8_t* k)
    {
        size_t prefix = 0;

        // Warning: this is intended for little endian!
        prefix |= static_cast<uint16_t>(k[1]);
        prefix |= static_cast<uint16_t>(k[0]) << 8;

        return prefix;
    }

    inline size_t find_partition_prefix_for_key(const key_t& key, size_t partition_size)
    {
        const auto& K = extract_prefix(key.data());
        const auto& P = partition_size;

        return ((K + P) / P * P) - 1;
    };

    [[nodiscard]] std::pair<std::string, std::string> format_prefix(uint16_t prefix);

    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions);

    using buffer_t = std::unique_ptr<uint8_t[], void (*)(void*)>;

    constexpr buffer_t make_null_buffer()
    {
        return {nullptr, [](void*) {}};
    }

    inline buffer_t make_aligned_buffer(size_t n)
    {
        return {static_cast<uint8_t*>(std::aligned_alloc(PAGE_SIZE_IN_BYTES, n)), std::free};
    }

} // namespace hedge