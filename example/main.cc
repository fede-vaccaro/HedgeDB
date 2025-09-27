#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>

#include <uuid.h>

#include <api/api.h>
#include <api/config.h>
#include <async/working_group.h>

// Utility struct to hold necessary test variables and functions
struct api_example_util
{
    api_example_util(size_t n_keys = 10'000'000,
                     size_t payload_size = 100,
                     size_t memtable_capacity = 1'000'000) : _n_keys(n_keys),
                                                             _payload_size(payload_size),
                                                             _memtable_capacity(memtable_capacity)
    {
        if(std::filesystem::exists(this->_base_path))
        {
            std::cout << "Removing existing database path: " << this->_base_path << std::endl;
            std::filesystem::remove_all(this->_base_path);
        }
        this->_uuids.reserve(_n_keys);
    }

    // Configurable (todo) test parameters
    size_t _n_keys;            // number of keys
    size_t _payload_size;      // payload size in bytes
    size_t _memtable_capacity; // memtable capacity

    // Runtime data
    std::filesystem::path _base_path = "/tmp/db";
    std::vector<uuids::uuid> _uuids;
    size_t seed = 107279581;
    std::mt19937 _generator{seed};
    uuids::basic_uuid_random_generator<std::mt19937> gen{_generator};

    uuids::uuid generate_uuid()
    {
        // The uuids are stored for read-back verification
        this->_uuids.emplace_back(this->gen());
        return this->_uuids.back();
    }

    static std::vector<uint8_t> make_random_vec_seeded(size_t size, size_t seed)
    {
        // The payloads are cached to avoid compute and space overhead
        static std::unordered_map<size_t, std::vector<uint8_t>> cache;
        constexpr size_t MAX_CACHE_ITEMS_CAPACITY = 1024;

        if(size_t hash = seed % MAX_CACHE_ITEMS_CAPACITY; cache.contains(hash))
            return cache[hash];

        std::vector<uint8_t> vec(size);
        std::mt19937 generator{seed};
        std::uniform_int_distribution<uint8_t> dist(0, 255);

        std::ranges::generate(vec, [&dist, &generator]()
                              { return dist(generator); });

        cache[seed % MAX_CACHE_ITEMS_CAPACITY] = vec;
        return cache[seed % MAX_CACHE_ITEMS_CAPACITY];
    }
};

int main()
{
    api_example_util util(10'000'000, // n keys
                          100,        // payload size
                          1'000'000   // memtable capacity
    );

    std::cout << "Starting API comprehensive example..." << std::endl;
    std::cout << "N_KEYS: " << util._n_keys << ", PAYLOAD_SIZE: " << util._payload_size << ", MEMTABLE_CAPACITY: " << util._memtable_capacity << std::endl;

    // --- 1. Database Setup ---
    hedge::async::working_group write_wg;
    write_wg.set(util._n_keys);

    hedge::config config;
    config.auto_compaction = true;
    config.keys_in_mem_before_flush = util._memtable_capacity;
    config.num_partition_exponent = 0;                         // single partition
    config.compaction_read_ahead_size_bytes = 4 * 1024 * 1024; // 4 MB

    auto maybe_db = hedge::api::create_database(util._base_path, config);
    if(!maybe_db)
    {
        std::cerr << "FATAL: An error occurred while creating the database: " << maybe_db.error().to_string() << std::endl;
        exit(1);
    }

    auto db = std::move(maybe_db.value());
    std::cout << "Database created successfully at " << util._base_path << std::endl;

    // --- 2. Insertion Test ---
    auto t0 = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < util._n_keys; ++i)
    {
        auto key = util.generate_uuid();
        auto value = api_example_util::make_random_vec_seeded(util._payload_size, i);

        db->put(key,
                std::move(value),
                [&write_wg](auto status)
                {
                    if(!status)
                        std::cerr << "An error occurred during insertion: " << status.error().to_string() << std::endl;
                    write_wg.decr();
                });
    }

    write_wg.wait();
    auto t1 = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "\n--- Insertion Metrics ---" << std::endl;
    std::cout << "Total duration: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "Average duration per insertion: " << (double)duration.count() / util._n_keys << " us" << std::endl;
    std::cout << "Insertion throughput: " << (uint64_t)(util._n_keys / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
    std::cout << "Insertion bandwidth: " << (double)util._n_keys * (util._payload_size / 1024.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;

    // --- 3. Compaction ---
    std::cout << "\n--- Compaction ---" << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    auto compaction_status_future = db->compact_sorted_indices(true);
    if(!compaction_status_future.get())
    {
        std::cerr << "FATAL: An error occurred during compaction: " << compaction_status_future.get().error().to_string() << std::endl;
        exit(1);
    }
    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Total duration for compaction: " << (double)duration.count() / 1000.0 << " ms" << std::endl;

    double load_factor = db->load_factor();
    std::cout << "Database load factor after compaction: " << load_factor << std::endl;
    if(std::abs(load_factor - 1.0) > 0.0001) // Simple check for near-equality
        std::cerr << "Warning: Read amplification is " << load_factor << " (Expected 1.0 after full compaction)" << std::endl;

    // --- 4. Retrieval Test ---
    hedge::async::working_group read_wg;
    read_wg.set(util._n_keys);
    size_t number_of_errors = 0;

    std::cout << "\n--- Retrieval Test ---" << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < util._n_keys; ++i)
    {
        db->get(util._uuids[i],
                [i, &read_wg, &util, &number_of_errors](hedge::expected<std::vector<uint8_t>> maybe_value)
                {
                    if(!maybe_value)
                    {
                        std::cerr << "An error occurred during retrieval for key " << util._uuids[i] << ": " << maybe_value.error().to_string() << std::endl;
                        number_of_errors++;
                        read_wg.decr();
                        return;
                    }

                    auto& value = maybe_value.value();
                    auto expected_value = api_example_util::make_random_vec_seeded(util._payload_size, i);

                    if(value != expected_value)
                    {
                        std::cerr << "Retrieved value does not match expected value for item nr. " << i << std::endl;
                        number_of_errors++;
                    }

                    read_wg.decr();
                });
    }

    read_wg.wait();
    t1 = std::chrono::high_resolution_clock::now();

    if(number_of_errors > 0)
    {
        std::cerr << "\nFATAL: " << number_of_errors << " errors occurred during retrieval." << std::endl;
        exit(1);
    }
    std::cout << "All " << util._n_keys << " keys retrieved and verified successfully." << std::endl;

    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "\n--- Retrieval Metrics ---" << std::endl;
    std::cout << "Total duration: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
    std::cout << "Average duration per retrieval: " << (double)duration.count() / util._n_keys << " us" << std::endl;
    std::cout << "Retrieval throughput: " << (uint64_t)(util._n_keys / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
    std::cout << "Retrieval bandwidth: " << (double)util._n_keys * (util._payload_size / 1024.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;
}