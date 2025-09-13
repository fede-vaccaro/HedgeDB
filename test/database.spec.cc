#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>

#include "../database.h"
#include "../io_executor.h"
#include "../working_group.h"

namespace hedgehog::db
{
    struct database_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
    {
        void SetUp() override
        {
            this->N_KEYS = std::get<0>(GetParam());
            this->PAYLOAD_SIZE = std::get<1>(GetParam());
            this->MEMTABLE_CAPACITY = std::get<2>(GetParam());

            if(std::filesystem::exists(this->_base_path))
                std::filesystem::remove_all(this->_base_path);

            this->_executor = std::make_shared<hedgehog::async::executor_context>(128);
        }

        uuids::uuid generate_uuid()
        {
            this->_uuids.emplace_back(this->gen());
            return this->_uuids.back();
        }

        std::vector<uint8_t> make_random_vec(size_t size)
        {
            std::vector<uint8_t> vec(size);

            static std::uniform_int_distribution<uint8_t> dist(0, 255);

            std::ranges::generate(vec, [this]()
                                  { return dist(this->_generator); });
            return vec;
        }

        static std::vector<uint8_t> make_random_vec_seeded(size_t size, size_t seed)
        {

            static std::unordered_map<size_t, std::vector<uint8_t>> cache;
            constexpr size_t MAX_CACHE_ITEMS_CAPACITY = 1024;

            if(size_t hash = seed % MAX_CACHE_ITEMS_CAPACITY; cache.contains(hash))
                return cache[hash];

            std::vector<uint8_t> vec(size);
            std::mt19937 generator{seed};
            std::uniform_int_distribution<uint8_t> dist(0, 255);

            std::ranges::generate(vec, [&dist, &generator]()
                                  { return dist(generator); });
            cache[seed] = vec;
            return vec;
        }

        void TearDown() override
        {
        }

        // test parameters
        size_t N_KEYS{};            // number of keys per run
        size_t PAYLOAD_SIZE{};      // payload size in bytes
        size_t MEMTABLE_CAPACITY{}; // memtable capacity

        // runtime
        std::shared_ptr<hedgehog::async::executor_context> _executor;
        std::filesystem::path _base_path = "/tmp/db";

        // rng stuff
        std::vector<uuids::uuid> _uuids;
        size_t seed{107279581};
        std::mt19937 _generator{seed};
        std::uniform_int_distribution<int> dist{0, 15};
        uuids::basic_uuid_random_generator<std::mt19937> gen{_generator};
    };

    TEST_P(database_test, basic_test_no_compactation)
    {
        this->_uuids.reserve(this->N_KEYS);

        async::working_group write_wg;
        write_wg.set(this->N_KEYS);

        db_config config;
        config.auto_compactation = false;
        config.keys_in_mem_before_flush = this->MEMTABLE_CAPACITY;

        auto maybe_db = database::make_new(this->_base_path, config);
        ASSERT_TRUE(maybe_db) << "An error occurred while creating the database: " << maybe_db.error().to_string();

        auto db = std::move(maybe_db.value());

        auto make_put_task = [this, &db, &write_wg](size_t i) -> async::task<void>
        {
            auto key = this->generate_uuid();
            auto value = database_test::make_random_vec_seeded(this->PAYLOAD_SIZE, i);

            auto status = co_await db->put_async(key, std::move(value), this->_executor);

            if(!status)
                std::cerr << "An error occurred during insertion: " << status.error().to_string() << std::endl;

            write_wg.decr();
        };

        auto t0 = std::chrono::high_resolution_clock::now();
        for(size_t i = 0; i < this->N_KEYS; ++i)
            this->_executor->submit_io_task(make_put_task(i));

        write_wg.wait();
        auto t1 = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        std::cout << "Total duration for insertion: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
        std::cout << "Average duration per insertion: " << (double)duration.count() / this->N_KEYS << " us" << std::endl;
        std::cout << "Insertion bandwidth: " << (double)this->N_KEYS * (this->PAYLOAD_SIZE / 1024.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;

        async::working_group read_wg;
        read_wg.set(this->N_KEYS);

        size_t number_of_errors = 0;

        auto make_get_task = [this, &db, &read_wg, &number_of_errors](size_t i) -> async::task<void>
        {
            auto key = this->_uuids[i];
            auto maybe_value = co_await db->get_async(key, this->_executor);

            if(!maybe_value)
            {
                number_of_errors++;
                std::cerr << "An error occurred during retrieval for key " << key << ": " << maybe_value.error().to_string() << std::endl;
                read_wg.decr();
                co_return;
            }
            auto& value = maybe_value.value();

            auto expected_value = database_test::make_random_vec_seeded(this->PAYLOAD_SIZE, i);
            if(value != expected_value)
            {
                std::cerr << "Retrieved value does not match expected value for item nr.  " << i << std::endl;
                number_of_errors++;
            }

            read_wg.decr();
        };

        t0 = std::chrono::high_resolution_clock::now();
        for(size_t i = 0; i < this->N_KEYS; ++i)
            this->_executor->submit_io_task(make_get_task(i));

        read_wg.wait();
        t1 = std::chrono::high_resolution_clock::now();

        if(number_of_errors > 0)
        {
            std::cerr << "Number of errors: " << number_of_errors << std::endl;
            FAIL() << "Some errors occurred during retrieval.";
        }

        duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        std::cout << "Total duration for retrieval: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
        std::cout << "Average duration per retrieval: " << (double)duration.count() / this->N_KEYS << " us" << std::endl;
        std::cout << "Retrieval bandwidth: " << (double)this->N_KEYS * (this->PAYLOAD_SIZE / 1024.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;
    }

    INSTANTIATE_TEST_SUITE_P(
        test_suite,
        database_test,
        testing::Combine(
            testing::Values(8'000'000), // n keys
            testing::Values(8192),      // payload size
            testing::Values(2'000'000)  // memtable capacity
            ),
        [](const testing::TestParamInfo<database_test::ParamType>& info)
        {
            auto num_keys = std::get<0>(info.param);
            auto payload_size = std::get<1>(info.param);
            auto memtable_capacity = std::get<2>(info.param);

            std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(payload_size) + "_C_" + std::to_string(memtable_capacity);
            return name;
        });

} // namespace hedgehog::db