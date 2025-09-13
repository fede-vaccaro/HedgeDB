#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <numeric>
#include <random>
#include <string>
#include <vector>
#include <algorithm> 

#include <error.hpp>
#include <uuid.h>

#include "bloom_filter.h" 
#include "database.h"

int main(int argc, char* argv[])
{
    std::cout << "version 0.0.1a" << std::endl;
    std::cout << "Number of arguments: " << std::to_string(argc) << '\n';

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <N_FILES> <base_path>\n";
        return 1;
    }

    std::cout << "N_FILES (arg 1): " << argv[1] << '\n';
    std::cout << "Base Path (arg 2): " << argv[2] << '\n';

    auto base_path = std::filesystem::path(argv[2]);

    // Ensure the base path exists, create if not
    if (!std::filesystem::exists(base_path)) {
        std::filesystem::create_directories(base_path);
        std::cout << "Created base path: " << base_path << '\n';
    }

    // Clean up previous test files for a fresh run
    // These paths should correspond to how your db.indexsorted.h constructs them
    std::filesystem::remove(base_path / "test_db.index.ss");
    std::filesystem::remove(base_path / "test_db.values.v");
    std::filesystem::remove(base_path / "test_db.index.ss.tmp");
    std::filesystem::remove(base_path / "test_db.ts");

    // Initialize the memtable_db
    // This calls memtable_db::make_new from your db.indexsorted.h
    hedgehog::db::DB database = std::move(hedgehog::db::DB::make_new(base_path).value());

    auto N_FILES = std::stoul(argv[1]);
    std::cout << "N_FILES: " << N_FILES << '\n';

    constexpr auto PAYLOAD_SIZE = 32; // 1KB payload
    std::vector<uint8_t> data(PAYLOAD_SIZE, 0);
    std::iota(data.begin(), data.end(), 0); // Fill with sequential data

    // Prepare for key generation and probabilistic selection
    std::random_device rd;
    std::mt19937 generator(rd());
    uuids::uuid_random_generator uuid_gen{generator};
    std::uniform_real_distribution<double> prob_dist(0.0, 1.0);

    // Probability of a key being preserved for checking.
    // Clamped to 1.0 if N_FILES is small (e.g., N_FILES < 1000), ensuring all are selected.
    const double preserve_probability = std::min(1.0, 1000.0 / static_cast<double>(N_FILES));

    std::vector<hedgehog::db::key_t> keys_to_read;
    // Pre-allocate space for the expected number of preserved keys to avoid reallocations
    keys_to_read.reserve(static_cast<size_t>(std::min(1000.0, static_cast<double>(N_FILES))));

    std::cout << "Starting key generation and insertion...\n";

    // --- Write Benchmark ---
    auto t0 = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < N_FILES; ++i)
    {
        hedgehog::db::key_t current_key = uuid_gen(); // Generate UUID on spot

        // Probabilistically add key to keys_to_read for later verification
        if (prob_dist(generator) < preserve_probability) {
            keys_to_read.emplace_back(current_key);
        }

        // Insert the key-value pair into the memtable_db
        if(auto status = database.insert(current_key, data); !status) {
            std::cerr << "Failed to insert key " << uuids::to_string(current_key) << ": " << status.error().to_string() << '\n';
            return 1; // Exit on critical insertion failure
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Elapsed time for writes: " << elapsed << " ms" << '\n';
    if (elapsed > 0) {
        std::cout << "Bandwidth: " << (static_cast<double>(N_FILES * PAYLOAD_SIZE) / (1024.0 * 1024.0 * elapsed / 1000.0)) << " MB/s" << '\n';
    } else {
        std::cout << "Bandwidth: N/A (elapsed time too small)" << '\n';
    }

    std::cout << "Total keys inserted: " << N_FILES << '\n';
    std::cout << "Keys selected for reading: " << keys_to_read.size() << '\n';

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // --- Read Benchmark (Preserved Keys) ---
    std::cout << "\nStarting reads for preserved keys...\n";
    size_t successful_reads = 0;
    t0 = std::chrono::high_resolution_clock::now();

    for(const auto& key : keys_to_read)
    {
        // This calls sortedstring_db::get from your db.indexsorted.h
        auto maybe_data_readback = database.get(key);

        if(!maybe_data_readback.has_value())
        {
            std::cerr << "Error reading key " << uuids::to_string(key) << ": " << maybe_data_readback.error().to_string() << '\n';
            continue;
        }

        // Check for data correctness
        if(maybe_data_readback.value() != data) {
            std::cerr << "Data mismatch for key: " << uuids::to_string(key) << '\n';
        } else {
            successful_reads++;
        }
    }
    t1 = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::cout << "Elapsed time for reads of " << successful_reads << " keys: " << elapsed << " ms" << '\n';
    if (elapsed > 0) {
        std::cout << "Reads/sec: " << (static_cast<double>(successful_reads) / (static_cast<double>(elapsed) / 1000.0)) << '\n';
    } else {
        std::cout << "Reads/sec: N/A (elapsed time too small)" << '\n';
    }

    std::cout << "All benchmarks completed." << std::endl;

    return 0;
}
