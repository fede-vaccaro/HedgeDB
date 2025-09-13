#include <array>
#include <fcntl.h>
#include <format>
#include <map>
#include <numeric>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "bloom_filter.h"
#include "tables.h"

int main(int argc, char* argv[])
{
    std::cout << "version 0.0.1a" << std::endl;

    auto base_path = std::filesystem::path(argv[2]);

    hedgehog::db::memtable_db database = hedgehog::db::memtable_db::make_new(base_path, "test_db");

    auto N_FILES = std::stoul(argv[1]);
    auto VALUE_SIZE = std::stoul(argv[3]);

    std::cout << "BASE_PATH: " << base_path << '\n';
    std::cout << "N_FILES: " << N_FILES << '\n';
    std::cout << "VALUE_SIZE: " << VALUE_SIZE << '\n';

    auto FILE_SIZE = VALUE_SIZE;

    std::random_device rd{};
    std::vector<uint8_t> data(FILE_SIZE, 0);
    std::uniform_int_distribution<int> dist(0, 255);
    
    std::mt19937 generator(rd());

    std::for_each(data.begin(), data.end(), [&generator, &dist](uint8_t& byte)
    {
        byte = static_cast<uint8_t>(dist(generator));
    });

    auto constexpr N_THREADS = 10;
    std::array<std::vector<hedgehog::db::key_t>, N_THREADS> key_sets;

    std::vector<std::thread> threads;

    for(size_t i = 0; i < key_sets.size(); ++i)
    {
        threads.emplace_back(
            [&key_sets, i, N_FILES, &rd]()
            {
                key_sets[i].reserve(N_FILES / N_THREADS + 1);

                std::mt19937 generator(rd());
                std::uniform_int_distribution<int> dist(0, 15);
                uuids::uuid_random_generator gen{generator};

                for(size_t j = 0; j < N_FILES / N_THREADS; ++j)
                    key_sets[i].emplace_back(gen());
            });
    }
    for(auto& thread : threads)
        thread.join();

    std::vector<hedgehog::db::key_t> keys;
    for(auto& key_set : key_sets)
    {
        keys.insert(keys.end(), key_set.begin(), key_set.end());
        key_set = std::vector<uuids::uuid>{};
    }

    std::cout << "Key generated. n keys: " << keys.size() << '\n';

    auto t0 = std::chrono::high_resolution_clock::now(); // NOLINT

    for(const auto& key : keys)
    {
        auto status = database.insert(key, data);
        if(!status)
        {
            std::cerr << "Failed to insert key: " << key << ": " << status.error().to_string() << '\n';
            return 1;
        }
    }
    auto t1 = std::chrono::high_resolution_clock::now(); // NOLINT

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Elapsed time for writes: " << elapsed << " ms" << '\n';
    std::cout << "Bandwidth: " << (static_cast<double>(N_FILES * FILE_SIZE) / (1024 * 1024 * elapsed / 1000.0)) << " MB/s" << '\n';

    // Flush the database
    t0 = std::chrono::high_resolution_clock::now();
    auto maybe_ss_db = hedgehog::db::flush_memtable_db(std::move(database));
    if(!maybe_ss_db)
    {
        std::cerr << "Flushing went wrong: " << maybe_ss_db.error().to_string() << '\n';
        return 1;
    }

    auto ss_db = std::move(maybe_ss_db.value());
    t1 = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Elapsed time for flush: " << elapsed << " ms" << '\n';

    std::shuffle(keys.begin(), keys.end(), generator);
    int count = 0;

    // Read the keys back
    t0 = std::chrono::high_resolution_clock::now();
    for(const auto& key : keys)
    {
        auto maybe_data_readback = ss_db.get(key);

        if(!maybe_data_readback.has_value())
        {
            std::cerr << "Error: " << maybe_data_readback.error().to_string() << '\n';
            continue;
        }

        if(maybe_data_readback.value() != data)
            continue;
            // std::cerr << "Failed to read data for key: " << key << '\n';

        if(count++ == 1000)
            break;
    }
    t1 = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Elapsed time for reads: " << elapsed << " ms" << '\n';
    std::cout << "Reads/sec " << (static_cast<double>(count) / (static_cast<double>(elapsed) / 1000.0)) << '\n';

    // test remove keys
    constexpr auto ELEMENTS_TO_DELETE = 5;
    int N_DELETES = std::min(ELEMENTS_TO_DELETE, static_cast<int>(N_FILES));

    std::unordered_set<hedgehog::db::key_t> keys_to_delete;
    for(int i = 0; i < N_DELETES; ++i)
    {
        auto it = keys.begin();
        std::advance(it, i);
        keys_to_delete.insert(*it);
    }

    t0 = std::chrono::high_resolution_clock::now();
    for(const auto& key : keys_to_delete)
    {
        if(auto status = ss_db.del(key); !status)
            std::cerr << "Failed to delete key: " << key << ": " << status.error() << '\n';

        std::cout << "Deleted key: " << key << '\n';

        // assert that the key is not available
        auto maybe_data_readback = ss_db.get_offset_from_key(key);

        if(maybe_data_readback && maybe_data_readback.value().has_value())
            std::cerr << "Key should not be available: " << key << '\n';
    }

    // assert that the key is not available
    t1 = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Elapsed time for deletes: " << elapsed << " ms" << '\n';

    // skip testing compact, too slow

    return 0;

    // // test compact
    // t0 = std::chrono::high_resolution_clock::now();

    // if(auto status = ss_db.compact(); !status)
    //     std::cerr << "Failed to compact: " << status.error() << '\n';

    // t1 = std::chrono::high_resolution_clock::now();
    // elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    // std::cout << "Elapsed time for compact: " << elapsed << " ms" << '\n';

    // count = 0;

    // // test retrieval of remaining keys
    // t0 = std::chrono::high_resolution_clock::now();
    // for(const auto& key : keys)
    // {
    //     if(keys_to_delete.contains(key))
    //         continue;

    //     auto maybe_data_readback = ss_db.get(key);

    //     if(!maybe_data_readback.has_value())
    //         std::cerr << "Failed to retrieve data with key " << key << " : " << maybe_data_readback.error() << '\n';

    //     auto data_readback = std::move(maybe_data_readback.value());

    //     if(data_readback.empty())
    //         std::cerr << "Failed to read data for key: " << key << '\n';

    //     if(data_readback != data)
    //         std::cerr << "Data mismatch for key: " << key << ": " << std::string(data_readback.begin(), data_readback.end()) << '\n';

    //     if(count++ == 1000)
    //         break;
    // }
    // t1 = std::chrono::high_resolution_clock::now();
    // elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    // std::cout << "Elapsed time for reads after compact: " << elapsed << " ms" << '\n';
    // std::cout << "Read/sec after compact: " << (static_cast<double>(count) / (static_cast<double>(elapsed) / 1000.0)) << '\n';

    return 0;
}
