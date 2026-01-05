#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <sys/types.h>
#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "db/database.h"
#include "fs/fs.hpp"
#include "perf_counter.h"
#include "types.h"
#include "uuid.h"

namespace hedge::db
{
    struct database_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
    {
        static void SetUpTestSuite()
        {
            async::executor_pool::init_static_pool(12, 64);
        }

        void SetUp() override
        {
            this->N_KEYS = std::get<0>(GetParam());
            this->PAYLOAD_SIZE = std::get<1>(GetParam());
            this->MEMTABLE_CAPACITY = std::get<2>(GetParam());

            if(std::filesystem::exists(this->_base_path))
                std::filesystem::remove_all(this->_base_path);
        }

        void TearDown() override
        {
        }

        /*
        This method creates a vector of N_KEYS pairs; each pair consists of a randomly generated uuids and a seed;
        Each seed is needed for generating the value (vector of bytes) associated to the key;
        Due to memory contraints, the seed is hashed to limit the value space size.
        */
        void _setup_key_values(bool create_new)
        {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto maybe_key_value_seeds =
                fs::file::from_path(
                    this->_base_path / "keys",
                    create_new ? fs::file::open_mode::read_write_new : fs::file::open_mode::read_only,
                    false,
                    this->N_KEYS * sizeof(uuids::uuid));

            if(!maybe_key_value_seeds)
                throw std::runtime_error("could not open " + (this->_base_path / "keys").string() + " " + maybe_key_value_seeds.error().to_string());

            this->_keys_file = std::move(maybe_key_value_seeds.value());

            auto maybe_mmap = fs::mmap_view::from_file(this->_keys_file);
            if(!maybe_mmap)
                throw std::runtime_error("could not mmap file " + this->_keys_file.path().string() + " " + maybe_mmap.error().to_string());

            this->_keys_mmap = std::move(maybe_mmap.value());
            auto* start = static_cast<key_t*>(this->_keys_mmap.get_ptr());
            for(auto* it = start; it != (start + this->N_KEYS); ++it)
            {
                if(create_new)
                    *it = this->gen();

                size_t hash = std::hash<uuids::uuid>{}(*it) % MAX_CACHE_ITEMS_CAPACITY;
                if(this->_possible_values.contains(hash))
                    continue;

                std::vector<uint8_t> vec(this->PAYLOAD_SIZE);
                std::mt19937 generator{hash};
                std::uniform_int_distribution<uint8_t> dist(0, 255);

                std::ranges::generate(vec, [&dist, &generator]()
                                      { return dist(generator); });

                this->_possible_values[hash] = vec;
            }

            auto t1 = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
            std::cout << "Total duration for key generation: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
            std::cout << "Generated " << (sizeof(key_t) * this->N_KEYS) / (1024.0 * 1024.0) << " MB of keys. This data will be held in memory." << std::endl;
        }

        // test parameters
        size_t N_KEYS{};                // number of keys per run
        size_t PAYLOAD_SIZE{};          // payload size in bytes
        size_t MEMTABLE_CAPACITY{};     // memtable capacity
        bool TEST_DELETION{false};      // whether to test deletion or not
        double DELETE_PROBABILITY{0.0}; // if deletion test is enabled, how many keys of total should be removed

        // runtime
        std::filesystem::path _base_path = "/tmp/db";

        // rng and seed stuff
        fs::mmap_view _keys_mmap;
        fs::file _keys_file;
        static constexpr size_t MAX_CACHE_ITEMS_CAPACITY = 1024;
        std::unordered_map<size_t, std::vector<uint8_t>> _possible_values;

        std::unordered_set<uuids::uuid> _deleted_keys;
        size_t _gen_seed{107279581};
        std::mt19937 _generator{_gen_seed};
        std::uniform_int_distribution<int> dist{0, 15};
        uuids::basic_uuid_random_generator<std::mt19937> gen{_generator};
    };

    TEST_P(database_test, database_comprehensive_test)
    {
        auto write_wg = hedge::async::wait_group::make_shared();
        write_wg->set(this->N_KEYS);

        db_config config;
        config.auto_compaction = true;
        config.keys_in_mem_before_flush = this->MEMTABLE_CAPACITY;
        config.compaction_read_ahead_size_bytes = 16 * 1024 * 1024;
        config.num_partition_exponent = 4;
        config.target_compaction_size_ratio = 0.85;
        config.use_odirect_for_indices = false;
        config.index_page_clock_cache_size_bytes = 3UL * 1024 * 1024 * 1024;
        config.index_point_cache_size_bytes = 0;

        if(config.index_page_clock_cache_size_bytes > 0)
            std::cout << "Using CLOCK cache. Allocated space: " << config.index_page_clock_cache_size_bytes / (1024.0 * 1024) << "MB" << std::endl;

        auto maybe_db = database::make_new(this->_base_path, config);
        ASSERT_TRUE(maybe_db) << "An error occurred while creating the database: " << maybe_db.error().to_string();

        auto db = std::move(maybe_db.value());

        this->_setup_key_values(true);

        std::uniform_real_distribution<double> dist(0.0, 1.0);

        auto make_put_task = [this, &db, &write_wg, &dist](size_t i) -> async::task<void>
        {
            const auto& key = this->_keys_mmap.get_ptr<key_t>()[i];
            size_t seed = std::hash<uuids::uuid>{}(key) % MAX_CACHE_ITEMS_CAPACITY;

            auto value = this->_possible_values[seed];

            auto status = co_await db->put_async(key, value);

            if(!status)
                std::cerr << "An error occurred during insertion: " << status.error().to_string() << std::endl;

            if(this->TEST_DELETION && dist(this->_generator) < this->DELETE_PROBABILITY)
            {
                auto status = co_await db->remove_async(key);

                if(!status)
                    std::cerr << "An error occurred during deletion: " << status.error().to_string() << std::endl;

                this->_deleted_keys.insert(key);
            }

            write_wg->decr();
        };

        auto t0 = std::chrono::high_resolution_clock::now();

        for(size_t i = 0; i < this->N_KEYS; ++i)
        {
            const auto& executor = async::executor_pool::executor_from_static_pool();
            executor->submit_io_task(make_put_task(i));
        }
        write_wg->wait();
        auto t1 = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        std::cout << "Total duration for insertion: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
        std::cout << "Average duration per insertion: " << (double)duration.count() / this->N_KEYS << " us" << std::endl;
        std::cout << "Insertion bandwidth: " << (double)this->N_KEYS * (this->PAYLOAD_SIZE / 1024.0 / 1024.0) / (duration.count() / 1'000'000.0) << " MB/s" << std::endl;
        std::cout << "Insertion throughput: " << (uint64_t)(this->N_KEYS / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
        std::cout << "Deleted keys: " << this->_deleted_keys.size() << std::endl;

        // flush
        // t0 = std::chrono::high_resolution_clock::now();
        // auto flush_status = db->flush();
        // ASSERT_TRUE(flush_status) << "An error occurred during flush: " << flush_status.error().to_string();
        // t1 = std::chrono::high_resolution_clock::now();
        // duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        // std::cout << "Total duration for flush: " << (double)duration.count() / 1000.0 << " ms" << std::endl;

        // compaction
        // t0 = std::chrono::high_resolution_clock::now();
        // auto compaction_status_future = db->compact_sorted_indices(true);
        // ASSERT_TRUE(compaction_status_future.get()) << "An error occurred during compaction: " << compaction_status_future.get().error().to_string();
        // t1 = std::chrono::high_resolution_clock::now();
        // duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        // std::cout << "Total duration for a full compaction: " << (double)duration.count() / 1000.0 << " ms" << std::endl;

        // EXPECT_DOUBLE_EQ(db->read_amplification_factor(), 1.0) << "Read amplification should be 1.0 after compaction";

        // shuffle keys before reading
        auto* begin = this->_keys_mmap.get_ptr<key_t>();
        auto* end = begin + (this->_keys_mmap.size() / sizeof(key_t));

        std::cout << "Shufflin keys for randread test..." << std::endl;
        std::shuffle(begin, end, this->_generator);

        std::cout << "Syncing FDs..." << std::endl;
        sync();

        auto sleep_time = std::chrono::seconds(1);
        std::cout << "Sleeping " << sleep_time.count() << " seconds before starting retrieval to cool down..." << std::endl;
        std::this_thread::sleep_for(sleep_time);

        // this->N_KEYS = std::min(10'000'000UL, this->N_KEYS); // reduce number of keys to read
        this->N_KEYS /= 8;

        for(int i = 0; i < 2; i++)
        {
            if(i == 0)
                std::cout << "Read test - before cache warm up" << std::endl;
            else
                std::cout << "Read test - after cache warm up" << std::endl;

            std::cout << "Starting readback now with " << this->N_KEYS << " keys." << std::endl;

            auto read_wg = hedge::async::wait_group::make_shared();
            read_wg->set(this->N_KEYS);

            std::atomic_size_t number_of_errors = 0;

            auto make_get_task = [this, &db, &read_wg, &number_of_errors](size_t i) -> async::task<void>
            {
                const auto& key = this->_keys_mmap.get_ptr<key_t>()[i];
                size_t seed = std::hash<uuids::uuid>{}(key) % MAX_CACHE_ITEMS_CAPACITY;

                auto maybe_value = co_await db->get_async(key);

                if(!maybe_value.has_value() && maybe_value.error().code() == errc::DELETED)
                {
                    if(!this->_deleted_keys.contains(key))
                    {
                        number_of_errors++;
                        std::cerr << "Key should be between the deleteds: " << key << std::endl;
                    }

                    read_wg->decr();
                    co_return;
                }

                if(!maybe_value)
                {
                    number_of_errors++;
                    std::cerr << "An error occurred during retrieval for key " << key << ": " << maybe_value.error().to_string() << std::endl;
                    read_wg->decr();
                    co_return;
                }

                [[maybe_unused]] auto& value = maybe_value.value();

                auto expected_value = this->_possible_values[seed];

                if(value != expected_value)
                {
                    std::cerr << "Retrieved value does not match expected value for item nr.  " << i << std::endl;
                    number_of_errors++;
                }

                read_wg->decr();
            };

            t0 = std::chrono::high_resolution_clock::now();

            for(size_t i = 0; i < this->N_KEYS; ++i)
            {
                const auto& executor = async::executor_pool::executor_from_static_pool();
                executor->submit_io_task(make_get_task(i));
            }

            read_wg->wait();
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

            prof::print_internal_perf_stats(true);
        }
    }

    INSTANTIATE_TEST_SUITE_P(
        test_suite,
        database_test,
        testing::Combine(
            testing::Values(80'000'000), // n keys
            testing::Values(1024),       // payload size
            testing::Values(2'000'000)   // memtable capacity
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