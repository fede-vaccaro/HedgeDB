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
    inline T ceil(T value, T divisor)
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

} // namespace hedge