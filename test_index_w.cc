#include <filesystem>
#include <fstream>
#include <iostream>
#include <malloc.h>
#include <map>
#include <random>

#include <error.h>
#include <thread>
#include <uuid.h>

#include "index.h"
#include "common.h"

int main(int argc, char* argv[])
{
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> dist(0, 15);
    uuids::uuid_random_generator gen{generator};

    auto N_UUIDS = std::stoul(argv[1]);
    auto BASE_PATH = std::filesystem::path(argv[2]);

    std::cout << "N_UUIDS: " << N_UUIDS << '\n';
    std::cout << "BASE_PATH: " << BASE_PATH << '\n';

    const auto MAX_MEMTABLES_BEFORE_FLUSH = 4;
    constexpr auto MB = 1024 * 1024;
    const auto INDEX_MAX_SIZE = 64 * MB / sizeof(hedgehog::index_key_t); // 64 MB worth of keys

    std::vector<hedgehog::db::mem_index> mem_indices;

    hedgehog::db::mem_index current_index;

    auto elapsed_total = 0;

    std::map<uint16_t, std::vector<hedgehog::db::sorted_index>> sorted_indices;

    std::vector<uuids::uuid> uuids;

    auto uuid_fake_size = [](const uuids::uuid& uuid) -> uint32_t
    {
        const auto& uuids_as_std_array = reinterpret_cast<const std::array<uint8_t, 16>&>(uuid);
        return (uuids_as_std_array[0] + uuids_as_std_array[1]) % 100; // Just a fake size based on the first two bytes
    };

    for(size_t i = 0; i < N_UUIDS; ++i)
    {
        uuids.emplace_back(gen());

        current_index.add(uuids.back(), {static_cast<uint64_t>(i), uuid_fake_size(uuids.back()), 0});

        if(current_index.size() >= INDEX_MAX_SIZE)
        {
            mem_indices.emplace_back();

            std::swap(mem_indices.back(), current_index);
        }

        if(mem_indices.size() >= MAX_MEMTABLES_BEFORE_FLUSH)
        {
            std::cout << "Flushing " << (MAX_MEMTABLES_BEFORE_FLUSH * INDEX_MAX_SIZE) << " indices to disk... UUID is: " << i << "\n";

            auto t0 = std::chrono::high_resolution_clock::now();
            auto partitioned_sorted_indices = hedgehog::db::index_ops::merge_and_flush(BASE_PATH, std::move(mem_indices), 4);
            auto t1 = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

            elapsed_total += elapsed;

            if(!partitioned_sorted_indices)
            {
                std::cerr << "Failed to flush indices: " << partitioned_sorted_indices.error().to_string() << '\n';
                return 1;
            }

            for(auto& index : partitioned_sorted_indices.value())
            {
                auto prefix = index.upper_bound();
                index.clear_index();
                sorted_indices[prefix].emplace_back(std::move(index));
            }

            std::cout << "Flushed " << (MAX_MEMTABLES_BEFORE_FLUSH * INDEX_MAX_SIZE) << " entries in " << elapsed << " ms\n";
            std::cout << "Flush rate: " << (double)(MAX_MEMTABLES_BEFORE_FLUSH * INDEX_MAX_SIZE) / elapsed * 1000.0 << " key/s\n";

            mem_indices.clear();
        }
    }

    if(current_index.size() > 0)
        mem_indices.emplace_back(std::move(current_index));

    if(!mem_indices.empty())
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        auto partitioned_indices = hedgehog::db::index_ops::merge_and_flush(BASE_PATH, std::move(mem_indices), 4);
        auto t1 = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        elapsed_total += elapsed;

        if(!partitioned_indices)
        {
            std::cerr << "Failed to flush remaining indices: " << partitioned_indices.error().to_string() << '\n';
            return 1;
        }

        for(auto& index : partitioned_indices.value())
        {
            auto prefix = index.upper_bound();
            index.clear_index();
            sorted_indices[prefix].emplace_back(std::move(index));
        }

        std::cout << "Flushed remaining entries in " << elapsed << " ms\n";
        std::cout << "Total time for flushing: " << (double)elapsed_total / 1000 << " s\n";
    }

    std::vector<hedgehog::db::mem_index>().swap(mem_indices);
    std::shuffle(uuids.begin(), uuids.end(), generator);
    uuids.resize(1000);
    uuids.shrink_to_fit();
    malloc_trim(0);

    std::ofstream ofs(BASE_PATH / "uuids", std::ios::binary); // current dir
    ofs.write(reinterpret_cast<const char*>(uuids.data()), uuids.size() * sizeof(uuids::uuid));
    if(!ofs.good())
    {
        std::cerr << "Failed to write uuids to file.\n";
        return 1;
    }
    ofs.close();

    for(auto& [prefix, indices] : sorted_indices)
    {
        indices.shrink_to_fit();

        std::cout << "Prefix: " << prefix << ", indices size: " << indices.size() << '\n';
        for(auto& index : indices)
            index.stats();
    }

    std::cout << "Testing " << uuids.size() << " UUIDs...\n";

    auto t0 = std::chrono::high_resolution_clock::now();
    for(const auto& uuid : uuids)
    {
        auto prefix = hedgehog::extract_prefix(uuid);

        auto it = std::lower_bound(sorted_indices.begin(), sorted_indices.end(), prefix,
                                   [](const auto& pair, uint16_t value)
                                   { return pair.first < value; });

        if(it != sorted_indices.end())
        {
            for(auto& index : it->second)
            {
                auto value_ptr = index.lookup(uuid);
                if(!value_ptr.has_value())
                {
                    std::cerr << "Value not found for UUID: " << uuid << '\n';
                    return 1;
                }
                else
                {
                    if(!value_ptr.value().has_value())
                    {
                        std::cerr << "Value pointer is empty for UUID: " << uuid << " prefix: " << prefix << '\n';
                        return 1;
                    }
                    else
                    {
                        auto& value = value_ptr.value().value();
                        if(value.size != uuid_fake_size(uuid))
                        {
                            std::cerr << "Value size mismatch for UUID: " << uuid << ", expected: " << uuid_fake_size(uuid) << ", got: " << value.size << '\n';
                            return 1;
                        }
                    }
                }
            }
        }
        else
        {
            std::cout << "No sorted index found for prefix: " << prefix << ": " << uuid << '\n';
            return 1;
        }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000.0;
    std::cout << "Lookup time: " << elapsed << " ms\n";
    std::cout << "Avg lookup time: " << elapsed * 1000.0 / uuids.size() << " us\n";
    std::cout << "Lookup rate: " << (double)uuids.size() / elapsed * 1000.0 << " key/s\n";
    return 0;
}