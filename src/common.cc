#include <iostream>

#include "common.h"

namespace hedge
{
    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions)
    {
        auto partition_size = (1 << 16) / num_space_partitions;

        std::vector<size_t> prefixes;
        prefixes.reserve(num_space_partitions);

        for(size_t i = 1; i <= num_space_partitions + 1; ++i)
        {
            auto last_prefix_of_partition = ((i * partition_size) - 1);

            prefixes.push_back(last_prefix_of_partition);

            std::cout << "Partition " << i << ": " << format_prefix(last_prefix_of_partition).second << " value: " << last_prefix_of_partition << std::endl;
        }

        std::vector<std::pair<size_t, std::filesystem::path>> paths;

        paths.reserve(prefixes.size());

        for(const auto& prefix : prefixes)
        {
            auto [dir_prefix, file_prefix] = format_prefix(prefix);
            paths.emplace_back(prefix, base_path / dir_prefix / (file_prefix + ".0"));
        }

        return paths;
    }

    std::pair<std::string, std::string> format_prefix(uint16_t prefix)
    {
        // keep in mind this is little-endian!
        auto as_array = reinterpret_cast<std::array<uint8_t, 2>&>(prefix);
        auto path = std::format("{:02x}", as_array[1]);
        auto filename = std::format("{:02x}{:02x}", as_array[1], as_array[0]);
        return std::pair{path, filename};
    }

} // namespace hedge
