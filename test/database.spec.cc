#include <algorithm>
#include <chrono>
#include <cstddef>
#include <execution>
#include <filesystem>
#include <limits>
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

namespace hedge::db
{
    struct alignas(64) fiber_timing
    {
        std::chrono::high_resolution_clock::time_point t_submit;
        std::chrono::high_resolution_clock::time_point t_start;
        std::chrono::high_resolution_clock::time_point t_end;
    };

    void print_latency_stats(const std::string& label, std::vector<fiber_timing>& timings)
    {
        size_t n = timings.size();
        if(n == 0)
            return;

        std::vector<double> queue_us(n);
        std::vector<double> exec_us(n);
        std::vector<double> total_us(n);
        for(size_t i = 0; i < n; ++i)
        {
            queue_us[i] = std::chrono::duration<double, std::micro>(timings[i].t_start - timings[i].t_submit).count();
            exec_us[i] = std::chrono::duration<double, std::micro>(timings[i].t_end - timings[i].t_start).count();
            total_us[i] = std::chrono::duration<double, std::micro>(timings[i].t_end - timings[i].t_submit).count();
        }

        std::ranges::sort(queue_us);
        std::ranges::sort(exec_us);
        std::ranges::sort(total_us);

        auto p50 = [&](const std::vector<double>& v)
        { return v[n * 50 / 100]; };
        auto p99 = [&](const std::vector<double>& v)
        { return v[n * 99 / 100]; };

        std::cout << label << " latency (us) — "
                  << "queue p50=" << p50(queue_us) << " p99=" << p99(queue_us)
                  << " | exec p50=" << p50(exec_us) << " p99=" << p99(exec_us)
                  << " | total p50=" << p50(total_us) << " p99=" << p99(total_us)
                  << std::endl;
    }

    struct database_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
    {
        static void SetUpTestSuite()
        {
            async::executor_pool::init_static_pool(20, 16);
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

        static void fill_key_as_random(key_t& key)
        {
            // constexpr size_t _gen_seed{107279581};
            thread_local std::mt19937 _generator{std::random_device{}()};
            thread_local std::uniform_int_distribution<uint8_t> dist{0, 255};

            auto span = key.as_bytes();
            std::ranges::generate(span, []()
                                  { return static_cast<uint8_t>(dist(_generator)); });
        }

        /*
        This method creates a vector of N_KEYS pairs; each pair consists of a randomly generated key and a seed;
        Each seed is needed for generating the value (vector of bytes) associated to the key;
        Due to memory contraints, the seed is hashed to limit the value space size.
        */
        void _setup_key_values(bool /* create_new */)
        {
            this->_keys.resize(this->N_KEYS);
            std::hash<key_t> hasher;

            auto t0 = std::chrono::high_resolution_clock::now();

            std::for_each(
                std::execution::par_unseq, this->_keys.begin(), this->_keys.end(),
                [](key_t& k)
                {
                    constexpr size_t key_size = 24;
                    k = key_t::make_with_length(key_size);
                    fill_key_as_random(k);
                });

            for(auto idx = 0UL; idx < this->N_KEYS; ++idx)
            {
                key_t& k = this->_keys[idx];

                size_t hash = hasher(k) % MAX_CACHE_ITEMS_CAPACITY;
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
            std::cout << "Generated " << (sizeof(key_t) * this->N_KEYS) / (1000.0 * 1000.0) << " MB worth of keys. This data will be held in memory." << std::endl;
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
        static constexpr size_t MAX_CACHE_ITEMS_CAPACITY = 1024;
        std::unordered_map<size_t, std::vector<uint8_t>> _possible_values;
        std::vector<key_t> _keys;
        std::unordered_set<key_t> _deleted_keys;
        size_t _gen_seed{107279581};
        std::mt19937 _generator{_gen_seed};
        std::uniform_int_distribution<uint8_t> dist{0, 255};
    };

    TEST_P(database_test, database_comprehensive_test)
    {
        auto write_wg = hedge::async::wait_group::make_shared();
        write_wg->set(this->N_KEYS);

        db_config config;
        config.auto_compaction = true;
        config.memtable_budget_bytes = this->MEMTABLE_CAPACITY;
        config.compaction_read_ahead_size_bytes = 2 * 1024 * 1024;
        config.num_partition_exponent = 4;
        config.bucket_ratio = 1.50;
        config.use_odirect_for_indices = true;
        config.index_page_clock_cache_size_bytes = 0 * 256 * 1024 * 1024;
        config.index_point_cache_size_bytes = 0;
        config.compaction_timeout = std::chrono::minutes(20);
        config.compaction_io_workers = 6;
        config.flush_io_workers = 6;

        if(config.index_page_clock_cache_size_bytes > 0)
            std::cout << "Using CLOCK cache. Allocated space: " << config.index_page_clock_cache_size_bytes / (1000.0 * 1000.0) << "MB" << std::endl;

        auto maybe_db = database::make_new(this->_base_path, config);
        ASSERT_TRUE(maybe_db) << "An error occurred while creating the database: " << maybe_db.error().to_string();

        auto db = std::move(maybe_db.value());

        this->_setup_key_values(true);

        std::uniform_real_distribution<double> dist(0.0, 1.0);

        auto make_put_task = [this, &db, &write_wg, &dist](size_t i) -> async::task<void>
        {
            const auto& key = this->_keys[i];
            size_t seed = std::hash<key_t>{}(key) % MAX_CACHE_ITEMS_CAPACITY;

            const auto& value = this->_possible_values[seed];

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
        std::vector<std::shared_ptr<async::executor_context>> executors;
        constexpr size_t NUM_WRITERS = 8;
        executors.reserve(NUM_WRITERS);
        for(size_t i = 0; i < NUM_WRITERS; ++i)
            executors.push_back(async::executor_pool::executor_from_static_pool());

        for(size_t i = 0; i < this->N_KEYS; ++i)
        {
            const auto& executor = executors[i % executors.size()];
            executor->submit_io_task(make_put_task(i));
        }

        write_wg->wait();
        std::cout << "All insertions completed. Waiting for any pending compactions to finish..." << std::endl;
        db->wait_for_compactions_to_finish(); // wait for any pending compactions to finish before measuring time

        auto t1 = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        std::cout << "Total duration for insertion: " << (double)duration.count() / 1000.0 << " ms" << std::endl;
        std::cout << "Average duration per insertion: " << (double)duration.count() / this->N_KEYS << " us" << std::endl;
        std::cout << "Insertion bandwidth: " << (double)this->N_KEYS * ((this->PAYLOAD_SIZE + sizeof(uuid_t)) / 1000.0 / 1000.0) / (duration.count() / 1'000'000.0) << " MB/s" << std::endl;
        std::cout << "Insertion throughput: " << (uint64_t)(this->N_KEYS / (double)duration.count() * 1'000'000) << " items/s" << std::endl;
        std::cout << "Deleted keys: " << this->_deleted_keys.size() << std::endl;
        std::cout << "BACKPRESSURE count (contention on memtable writers): " << hedge::db::memtable::BACKPRESSURE.load() << std::endl;
        prof::print_internal_perf_stats(false);

        db->print_tree_structure();

        // flush
        // t0 = std::chrono::high_resolution_clock::now();
        // auto flush_status = db->flush();
        // ASSERT_TRUE(flush_status) << "An error occurred during flush: " << flush_status.error().to_string();
        // t1 = std::chrono::high_resolution_clock::now();
        // duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        // std::cout << "Total duration for flush: " << (double)duration.count() / 1000.0 << " ms" << std::endl;

    // EXPECT_DOUBLE_EQ(db->read_amplification_factor(), 1.0) << "Read amplification should be 1.0 after compaction";

        // shuffle keys before reading
        auto begin = this->_keys.begin();
        auto end = this->_keys.end();

        std::cout << "Shufflin keys for randread test..." << std::endl;
        std::shuffle(begin, end, this->_generator);

        std::cout << "Syncing FDs..." << std::endl;
        sync();

        auto sleep_time = std::chrono::seconds(1);
        std::cout << "Sleeping " << sleep_time.count() << " seconds before starting retrieval to cool down..." << std::endl;
        std::this_thread::sleep_for(sleep_time);

        this->N_KEYS = std::min(10'000'000UL, this->N_KEYS); // reduce number of keys to read
        this->_keys.resize(this->N_KEYS);
        this->_keys.shrink_to_fit();
        // this->N_KEYS /= 8;

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
                const auto& key = this->_keys[i];
                size_t seed = std::hash<key_t>{}(key) % MAX_CACHE_ITEMS_CAPACITY;

                auto maybe_value = co_await db->get_async(key);

                if(!maybe_value.has_value() && maybe_value.error().code() == errc::DELETED)
                {
                    if(!this->_deleted_keys.contains(key))
                    {
                        number_of_errors++;
                        std::cerr << "Key should be between the deleteds: " << hedge::to_hex_string(key) << std::endl;
                    }

                    read_wg->decr();
                    co_return;
                }

                if(!maybe_value)
                {
                    number_of_errors++;
                    std::cerr << "An error occurred during retrieval for key with index " << i << " " << hedge::to_hex_string(key) << ": " << maybe_value.error().to_string() << std::endl;
                    read_wg->decr();
                    co_return;
                }

                [[maybe_unused]] const auto& value = maybe_value.value();

                const auto& expected_value = this->_possible_values[seed];

                if(value != expected_value)
                {
                    std::cerr << "Retrieved value does not match expected value for item nr.  " << i << std::endl;
                    number_of_errors++;
                }

                read_wg->decr();
            };

            t0 = std::chrono::high_resolution_clock::now();

            std::vector<std::shared_ptr<async::executor_context>> executors;
            constexpr size_t NUM_READERS = 16;
            executors.reserve(NUM_READERS);
            for(size_t i = 0; i < NUM_READERS; ++i)
                executors.push_back(async::executor_pool::executor_from_static_pool());

            for(size_t i = 0; i < this->N_KEYS; ++i)
            {
                const auto& executor = executors[i % executors.size()];
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
            std::cout << "Retrieval bandwidth: " << (double)this->N_KEYS * (this->PAYLOAD_SIZE / 1000.0) / (duration.count() / 1000.0) << " MB/s" << std::endl;

            prof::print_internal_perf_stats(true);
        }
    }

    INSTANTIATE_TEST_SUITE_P(
        test_suite,
        database_test,
        testing::Combine(
            testing::Values(80'000'000), // n keys
            testing::Values(100),        // payload size
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
