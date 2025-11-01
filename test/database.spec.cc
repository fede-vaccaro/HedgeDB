#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <random>
#include <unordered_set>

#include <gtest/gtest.h>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "db/database.h"

namespace hedge::db
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

            this->_executor = std::make_shared<hedge::async::executor_context>(128);
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
        size_t N_KEYS{};                // number of keys per run
        size_t PAYLOAD_SIZE{};          // payload size in bytes
        size_t MEMTABLE_CAPACITY{};     // memtable capacity
        bool TEST_DELETION{true};       // whether to test deletion or not
        double DELETE_PROBABILITY{0.0}; // if deletion test is enabled, how many keys of total should be removed

        // runtime
        std::shared_ptr<hedge::async::executor_context> _executor;
        std::filesystem::path _base_path = "/tmp/db";

        // rng and seed stuff
        std::vector<uuids::uuid> _uuids;
        std::unordered_set<uuids::uuid> _deleted_keys;
        size_t seed{107279581};
        std::mt19937 _generator{seed};
        std::uniform_int_distribution<int> dist{0, 15};
        uuids::basic_uuid_random_generator<std::mt19937> gen{_generator};
    };

    TEST_P(database_test, database_comprehensive_test)
    {
        this->_uuids.reserve(this->N_KEYS);

        async::wait_group write_wg;
        write_wg.set(this->N_KEYS);

        db_config config;
        config.auto_compaction = true;
        config.keys_in_mem_before_flush = this->MEMTABLE_CAPACITY;
        config.compaction_read_ahead_size_bytes =  16 * 1024 * 1024;
        config.num_partition_exponent = 4;
        config.target_compaction_size_ratio = 1.0 / 3.0;

        auto maybe_db = database::make_new(this->_base_path, config);
        ASSERT_TRUE(maybe_db) << "An error occurred while creating the database: " << maybe_db.error().to_string();

        auto db = std::move(maybe_db.value());

        std::uniform_real_distribution<double> dist(0.0, 1.0);

        auto make_put_task = [this, &db, &write_wg, &dist](size_t i) -> async::task<void>
        {
            auto key = this->generate_uuid();
            auto value = database_test::make_random_vec_seeded(this->PAYLOAD_SIZE, i);

            auto status = co_await db->put_async(key, std::move(value), this->_executor);

            if(!status)
                std::cerr << "An error occurred during insertion: " << status.error().to_string() << std::endl;

            if(this->TEST_DELETION && dist(this->_generator) < this->DELETE_PROBABILITY)
            {
                auto status = co_await db->remove_async(key, this->_executor);

                if(!status)
                    std::cerr << "An error occurred during deletion: " << status.error().to_string() << std::endl;

                this->_deleted_keys.insert(key);
            }

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
        std::cout << "Insertion throughput: " << (uint64_t)(this->N_KEYS / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
        std::cout << "Deleted keys: " << this->_deleted_keys.size() << std::endl;

        // compaction
        t0 = std::chrono::high_resolution_clock::now();
        auto compaction_status_future = db->compact_sorted_indices(true, this->_executor);
        ASSERT_TRUE(compaction_status_future.get()) << "An error occurred during compaction: " << compaction_status_future.get().error().to_string();
        t1 = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        std::cout << "Total duration for a full compaction: " << (double)duration.count() / 1000.0 << " ms" << std::endl;

        EXPECT_DOUBLE_EQ(db->read_amplification_factor(), 1.0) << "Read amplification should be 1.0 after compaction";

        async::wait_group read_wg;
        read_wg.set(this->N_KEYS);

        size_t number_of_errors = 0;

        auto make_get_task = [this, &db, &read_wg, &number_of_errors](size_t i) -> async::task<void>
        {
            auto key = this->_uuids[i];
            auto maybe_value = co_await db->get_async(key, this->_executor);

            if(!maybe_value.has_value() && maybe_value.error().code() == errc::DELETED)
            {
                if(!this->_deleted_keys.contains(key))
                {
                    number_of_errors++;
                    std::cerr << "Key should be between the deleteds: " << key << std::endl;
                }

                read_wg.decr();
                co_return;
            }

            if(!maybe_value)
            {
                number_of_errors++;
                std::cerr << "An error occurred during retrieval for key " << key << ": " << maybe_value.error().to_string() << std::endl;
                read_wg.decr();
                co_return;
            }

            [[maybe_unused]] auto& value = maybe_value.value();

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
        std::cout << "Retrieval throughput: " << (uint64_t)(this->N_KEYS / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
        std::cout << "Retrieval bandwidth: " << (double)this->N_KEYS * (this->PAYLOAD_SIZE / 1024.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;
    }

    INSTANTIATE_TEST_SUITE_P(
        test_suite,
        database_test,
        testing::Combine(
            testing::Values(30'000'000), // n keys
            testing::Values(100),        // payload size
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

} // namespace hedge::db