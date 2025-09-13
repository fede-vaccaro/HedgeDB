#include <cstdint>
#include <unistd.h>
#include <vector>
#include <limits>
#include <filesystem>
#include <fstream>

#include <error.hpp>
#include <thread>

#include "index.h"
#include "tables.h"
#include "uuid.h"

namespace hedgehog::db
{

    struct partitioned_view
    {
        uint16_t prefix;
        std::vector<index_key_t>::const_iterator start;
        std::vector<index_key_t>::const_iterator end;

        size_t size() const
        {
            return std::distance(start, end);
        }
    };

    auto format_prefix(uint16_t prefix)
    {
        auto as_array = reinterpret_cast<std::array<uint8_t, 2>&>(prefix);
        auto path = std::format("{:02x}", as_array[0]);
        auto filename = std::format("{:02x}{:02x}", as_array[0], as_array[1]); 
        return std::pair{path, filename};
    }

    uint16_t extract_prefix(const uuids::uuid& key)
    {
        auto& uuids_as_std_array = reinterpret_cast<const std::array<uint16_t, 16 / sizeof(uint16_t)>&>(key);
        return uuids_as_std_array[0];
    }

    hedgehog::status merge_with_partition(const std::filesystem::path& base_path, const partitioned_view& partition)
    {  
        auto [dir, filename] = format_prefix(partition.prefix);
        auto partition_path = base_path / dir;

        if(!std::filesystem::exists(partition_path))
        {
            std::filesystem::create_directories(partition_path);
        }

        auto file_path = partition_path / filename;

        std::vector<index_key_t> partitioned_data{};

        bool first_creation = !std::filesystem::exists(file_path);

        if(!first_creation)
        {
            auto file_size = std::filesystem::file_size(file_path);

            auto reader = std::ifstream(file_path, std::ios::binary);

            if(!reader.good())
                return hedgehog::error("Failed to open partition file: " + file_path.string());

            partitioned_data.reserve(file_size / sizeof(index_key_t) + partition.size());

            partitioned_data.resize(file_size / sizeof(index_key_t));

            reader.read(reinterpret_cast<char*>(partitioned_data.data()), static_cast<std::streamsize>(file_size));
        }

        partitioned_data.insert(partitioned_data.end(), partition.start, partition.end);
        std::sort(partitioned_data.begin(), partitioned_data.end());

        // todo: get sub partitions
        //

        {
            auto partitioned_file = std::ofstream(with_extension(file_path, ".tmp"), std::ios::binary | std::ios::trunc);
            if(!partitioned_file.good())
                return hedgehog::error("Failed to open partition file for writing: " + file_path.string());

            partitioned_file.write(reinterpret_cast<const char*>(partitioned_data.data()), static_cast<std::streamsize>(partitioned_data.size() * sizeof(index_key_t)));

            if(!partitioned_file.good())
                return hedgehog::error("Failed to write partition data to file: " + file_path.string());
        }

        if(!first_creation)
            std::filesystem::rename(file_path, with_extension(file_path, ".old"));

        std::filesystem::rename(with_extension(file_path, ".tmp"), file_path);

        if(!first_creation)
            std::filesystem::remove(with_extension(file_path, ".old"));

        return hedgehog::ok();
    }


    index_key_t _key_from_prefix(uint16_t prefix){
        auto key = uuids::uuid{};

        auto& uuids_as_std_array = reinterpret_cast<std::array<uint16_t, 16 / sizeof(uint16_t)>&>(key);

        std::fill(uuids_as_std_array.begin(), uuids_as_std_array.end(), 0);

        uuids_as_std_array[0] = prefix;
        
        return index_key_t{key, {}};
    };

    std::vector<partitioned_view> find_partitions(const std::vector<index_key_t>& index_sorted)
    {
        constexpr auto PREFIX_MAX = std::numeric_limits<uint16_t>::max();
        
        std::vector<partitioned_view> partitions;
        partitions.reserve(PREFIX_MAX);

        partitioned_view current_partition;

        current_partition.prefix = extract_prefix(index_sorted.begin()->key);
        current_partition.start = index_sorted.begin();

        // auto range_start = index_sorted.begin();
        for(auto it = index_sorted.begin() + 1; it != index_sorted.end(); ++it)
        {
            auto prefix = extract_prefix(it->key);

            if(prefix != current_partition.prefix)
            {
                current_partition.end = it;
                partitions.emplace_back(current_partition);

                current_partition.prefix = prefix;
                current_partition.start = it;
            }
        }

        current_partition.end = index_sorted.end();
        partitions.emplace_back(current_partition);

        return partitions;
    }


    hedgehog::status index_ops::flush_and_compact(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, bool use_multithreaded)
    {
        auto total_size = std::accumulate(indices.begin(), indices.end(), 0, [](size_t acc, const mem_index& idx) {
            return acc + idx._index.size();
        });

        std::vector<index_key_t> index_sorted;
        index_sorted.reserve(total_size);

        for(auto& idx : indices)
        {
            for(const auto& [key, value] : idx._index)
                index_sorted.push_back({key, value});

            idx._index = mem_index::index_t{};
        }

        std::sort(index_sorted.begin(), index_sorted.end());

        std::vector<partitioned_view> partitions = find_partitions(index_sorted);

        if(use_multithreaded)
        {
            std::vector<std::thread> thread_pool;
            thread_pool.reserve(4);

            for(int i = 0; i < 4; ++i)
            {
                thread_pool.emplace_back([&, i]() { 
                    auto partition_per_thread = partitions.size() / 4; // 65535 / 4 = 16383
                    
                    auto start_index = i * partition_per_thread;
                    auto end_index = (i + 1) * partition_per_thread;

                    if(i == 3) // Last thread takes the rest
                        end_index = partitions.size();

                    for(size_t j = start_index; j < end_index; ++j)
                    {
                        auto status = merge_with_partition(base_path, partitions[j]);

                        if(!status)
                            throw std::runtime_error("Failed to merge partition: " + status.error().to_string());
                    }
                });
            }

            for(auto& thread : thread_pool)
            {
                if(thread.joinable())
                    thread.join();
            }

            return hedgehog::ok();
        }

        for(const auto& partition : partitions)
        {
            auto status = merge_with_partition(base_path, partition);

            if(!status)
                return hedgehog::error("Failed to merge partition: " + status.error().to_string());
        }

        return hedgehog::ok();

    }

}