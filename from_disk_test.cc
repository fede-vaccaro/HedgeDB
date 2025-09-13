#include <filesystem>
#include <fstream>
#include <malloc.h>
#include <map>
#include <random>

#include <error.h>
#include <thread>
#include <uuid.h>

#include "common.h"
#include "index.h"

int main(int argc, char* argv[])
{
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> dist(0, 15);
    uuids::uuid_random_generator gen{generator};

    auto N_UUIDS = std::stoul(argv[1]);
    auto BASE_PATH = std::filesystem::path(argv[2]);

    std::cout << "N_UUIDS_TO_BE_READ: " << std::stoul(argv[1]) << '\n';
    std::cout << "BASE_PATH: " << BASE_PATH << '\n';

    std::map<uint16_t, std::vector<hedgehog::db::sorted_index>> sorted_indices_reloaded;

    std::vector<uuids::uuid> uuids(N_UUIDS);
    std::ifstream ifs(BASE_PATH / "uuids", std::ios::binary); // current dir
    ifs.read(reinterpret_cast<char*>(uuids.data()), N_UUIDS * sizeof(uuids::uuid));
    if(ifs.fail())
    {
        std::cerr << "Failed to open uuids file for reading.\n";
        return 1;
    }

    // print uuids
    for(const auto& uuid : uuids)
    {
        std::cout << "UUID: " << uuid << '\n';
    }

    auto paths = hedgehog::get_prefixes(BASE_PATH, 16);

    for(const auto& [prefix, path] : paths)
        std::cout << "Prefix: " << prefix << ", Path: " << path.string() << '\n';

    std::cout << "Reloading sorted tables...\n";

    for(const auto& [prefix, path] : paths)
    {
        auto loaded_index = hedgehog::db::index_ops::load_sorted_index(path, false);

        if(!loaded_index.has_value())
        {
            std::cerr << "Failed to load sorted index: " << loaded_index.error().to_string() << '\n';
            return 1;
        }

        sorted_indices_reloaded[prefix].emplace_back(std::move(loaded_index.value()));
    }

    for(auto& [prefix, indices] : sorted_indices_reloaded)
    {
        indices.shrink_to_fit();

        std::cout << "Prefix: " << prefix << ", indices size: " << indices.size() << '\n';
        for(auto& index : indices)
        {
            index.clear_index();
            index.stats();
        }
    }

    std::cout << "Testing " << uuids.size() << " UUIDs...\n";

    auto uuid_fake_size = [](const uuids::uuid& uuid) -> uint32_t
    {
        const auto& uuids_as_std_array = reinterpret_cast<const std::array<uint8_t, 16>&>(uuid);
        return (uuids_as_std_array[0] + uuids_as_std_array[1]) % 100; // Just a fake size based on the first two bytes
    };

    auto t0 = std::chrono::high_resolution_clock::now();
    for(const auto& uuid : uuids)
    {
        auto prefix = hedgehog::extract_prefix(uuid);

        auto it = std::lower_bound(sorted_indices_reloaded.begin(), sorted_indices_reloaded.end(), prefix,
                                   [](const auto& pair, uint16_t value)
                                   { return pair.first < value; });

        if(it != sorted_indices_reloaded.end())
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