#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <random>
#include <ranges>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "cache.h"
#include "db/index_ops.h"
#include "db/memtable.h" // For memtable_impl2_t
#include "db/sst.h"
#include "utils.h"

std::string to_hex_string(const hedge::key_t& key)
{
    auto key_span = static_cast<std::span<const uint8_t>>(key);
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for(uint8_t c : key_span)
    {
        ss << std::setw(2) << static_cast<int>(c);
    }
    return ss.str();
}

// Helper to generate fake size
uint32_t key_fake_size(const hedge::key_t& key)
{
    auto key_span = static_cast<std::span<const uint8_t>>(key);
    if(key_span.size() < 2)
        return 0;
    return key_span[0] + (key_span[1] % 125);
}

struct sst_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, bool>>
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->USE_CACHE = std::get<2>(GetParam());
        this->_keys.reserve(this->N_KEYS);

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        this->_executor = hedge::async::executor_context::make_new(128);
    }

    void TearDown() override
    {
        if(this->_executor)
        {
            this->_executor->shutdown();
            this->_executor.reset();
        }
    }

    hedge::key_t generate_key(size_t length)
    {
        auto k = hedge::key_t::make_with_length(length);
        auto span = static_cast<std::span<uint8_t>>(k);
        for(size_t i = 0; i < length; ++i)
        {
            span[i] = dist(generator);
        }
        return k;
    }

    size_t N_KEYS = 2000000;
    size_t NUM_PARTITION_EXPONENT = 0;
    bool USE_CACHE = false;
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_index2";
    std::shared_ptr<hedge::async::executor_context> _executor{};

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
};

TEST_P(sst_test, test_flush_and_lookup_16b_keys)
{
    std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr;
    if(this->USE_CACHE)
    {
        constexpr auto CACHE_MAX_SIZE = 64 * 1024 * 1024;
        cache = std::make_shared<hedge::db::sharded_page_cache>(CACHE_MAX_SIZE, 1);
    }

    auto memtable = hedge::db::memtable_impl2_t{};

    std::cout << "Generating " << this->N_KEYS << " keys..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        const auto& key = this->_keys.back();

        // Value: offset=j, size=fake_size, table_id=0
        memtable.insert(key, {static_cast<uint64_t>(j), key_fake_size(key), 0});
    }

    std::cout << "Keys generated." << std::endl;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto result = hedge::db::index_ops::flush_mem_index2(
        this->_base_path,
        &memtable,
        this->NUM_PARTITION_EXPONENT,
        0, // flush_iteration
        cache,
        false // use_odirect
    );
    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    if(!result)
    {
        std::cerr << "Failed to flush indices: " << result.error().to_string() << '\n';
        FAIL();
    }

    std::cout << "Flush completed." << std::endl;
    std::cout << "Flush took " << duration / 1000.0 << " ms" << std::endl;

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    // Prepare lookup
    auto query_wg = hedge::async::wait_group::make_shared();
    query_wg->set(this->_keys.size());

    std::atomic<size_t> found_count{0};

    auto lookup_task_factory = [&](const hedge::key_t& key,
                                   const hedge::db::sst& sst,
                                   std::shared_ptr<hedge::async::wait_group> wg) -> hedge::async::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, cache);

        if(!lookup.has_value())
        {
            std::cerr << "Error during lookup for key: " << to_hex_string(key) << " error: " << lookup.error().to_string() << '\n';
        }
        else
        {
            auto val_opt = lookup.value();
            if(val_opt.has_value())
            {
                auto& val = val_opt.value();
                if(val.size() == key_fake_size(key))
                {
                    found_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    std::cerr << "Key found but size mismatch for key: " << to_hex_string(key) << ", expected size: " << key_fake_size(key) << ", actual size: " << val.size() << '\n';
                }
            }
            else
            {
                std::cerr << "Key not found: " << to_hex_string(key) << '\n';
            }
        }
        wg->decr();
    };

    std::cout << "Reading back keys..." << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
    {
        sst_map[sst.upper_bound()] = &sst;
    }

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    for(const auto& key : this->_keys)
    {
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            this->_executor->submit_io_task(lookup_task_factory(key, *it->second, query_wg));
        }
        else
        {
            query_wg->decr();
        }
    }

    query_wg->wait();

    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    std::cout << "Lookup took " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Found " << found_count.load() << " / " << this->_keys.size() << " keys" << std::endl;
    double throughput = (double)this->_keys.size() / (duration / 1000000.0);
    std::cout << "Lookup Throughput: " << std::fixed << std::setprecision(2) << throughput << " ops/s" << std::endl;

    ASSERT_EQ(found_count.load(), this->_keys.size());

    // --- Negative Lookup Test ---
    std::cout << "Generating negative lookup keys..." << std::endl;
    std::vector<hedge::key_t> negative_keys;
    size_t num_negative_keys = std::max(static_cast<size_t>(1), this->N_KEYS / 10); // 10%
    negative_keys.reserve(num_negative_keys);

    for(size_t i = 0; i < num_negative_keys; ++i)
    {
        // Generate a random key.
        negative_keys.emplace_back(generate_key(16));
    }

    std::cout << "Verifying " << num_negative_keys << " non-existent keys..." << std::endl;

    query_wg->set(negative_keys.size());
    std::atomic<size_t> unexpected_found_count{0};

    auto negative_lookup_task_factory = [&](const hedge::key_t& key,
                                            const hedge::db::sst& sst,
                                            std::shared_ptr<hedge::async::wait_group> wg) -> hedge::async::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, cache);

        if(lookup.has_value())
        {
            auto val_opt = lookup.value();
            if(val_opt.has_value())
            {
                unexpected_found_count.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "Unexpectedly found key: " << to_hex_string(key) << '\n';
            }
        }
        wg->decr();
    };

    t0 = std::chrono::high_resolution_clock::now();

    for(const auto& key : negative_keys)
    {
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            this->_executor->submit_io_task(negative_lookup_task_factory(key, *it->second, query_wg));
        }
        else
        {
            // If partition doesn't exist, key definitely doesn't exist in our index
            query_wg->decr();
        }
    }

    query_wg->wait();

    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    std::cout << "Negative lookup took " << duration / 1000.0 << " ms" << std::endl;
    throughput = (double)negative_keys.size() / (duration / 1000000.0);
    std::cout << "Negative Lookup Throughput: " << std::fixed << std::setprecision(2) << throughput << " ops/s" << std::endl;

    if(unexpected_found_count.load() > 0)
    {
        // It's theoretically possible to randomly generate a key that was inserted,
        // but with 128-bit keys and 2M entries, it's statistically impossible (1 in 10^30).
        FAIL() << "Found " << unexpected_found_count.load() << " keys that should not exist.";
    }
}

TEST_P(sst_test, test_flush_and_lookup_variable_keys)
{
    std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr;
    if(this->USE_CACHE)
    {
        constexpr auto CACHE_MAX_SIZE = 128 * 1024 * 1024;
        cache = std::make_shared<hedge::db::sharded_page_cache>(CACHE_MAX_SIZE, 1);
    }

    auto memtable = hedge::db::memtable_impl2_t{};

    std::cout << "Generating " << this->N_KEYS << " keys with variable length..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(len_dist(generator)));
        const auto& key = this->_keys.back();

        // Value: offset=j, size=fake_size, table_id=0
        memtable.insert(key, {static_cast<uint64_t>(j), key_fake_size(key), 0});
    }

    std::cout << "Keys generated." << std::endl;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto result = hedge::db::index_ops::flush_mem_index2(
        this->_base_path,
        &memtable,
        this->NUM_PARTITION_EXPONENT,
        0, // flush_iteration
        cache,
        false // use_odirect
    );
    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    if(!result)
    {
        std::cerr << "Failed to flush indices: " << result.error().to_string() << '\n';
        FAIL();
    }

    std::cout << "Flush completed." << std::endl;
    std::cout << "Flush took " << duration / 1000.0 << " ms" << std::endl;

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    // Prepare lookup
    auto query_wg = hedge::async::wait_group::make_shared();
    query_wg->set(this->_keys.size());

    std::atomic<size_t> found_count{0};

    auto lookup_task_factory = [&](const hedge::key_t& key,
                                   const hedge::db::sst& sst,
                                   std::shared_ptr<hedge::async::wait_group> wg) -> hedge::async::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, cache);

        if(!lookup.has_value())
        {
            std::cerr << "Error during lookup for key: " << to_hex_string(key) << " error: " << lookup.error().to_string() << '\n';
        }
        else
        {
            auto val_opt = lookup.value();
            if(val_opt.has_value())
            {
                auto& val = val_opt.value();
                if(val.size() == key_fake_size(key))
                {
                    found_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    std::cerr << "Key found but size mismatch for key: " << to_hex_string(key) << ", expected size: " << key_fake_size(key) << ", actual size: " << val.size() << '\n';
                }
            }
            else
            {
                std::cerr << "Key not found: " << to_hex_string(key) << '\n';
            }
        }
        wg->decr();
    };

    std::cout << "Reading back keys..." << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
    {
        sst_map[sst.upper_bound()] = &sst;
    }

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    for(const auto& key : this->_keys)
    {
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            this->_executor->submit_io_task(lookup_task_factory(key, *it->second, query_wg));
        }
        else
        {
            query_wg->decr();
        }
    }

    query_wg->wait();

    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    std::cout << "Lookup took " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Found " << found_count.load() << " / " << this->_keys.size() << " keys" << std::endl;
    double throughput = (double)this->_keys.size() / (duration / 1000000.0);
    std::cout << "Lookup Throughput: " << std::fixed << std::setprecision(2) << throughput << " ops/s" << std::endl;

    ASSERT_EQ(found_count.load(), this->_keys.size());
}

TEST_P(sst_test, test_flush_succeeds)
{
    auto memtable = hedge::db::memtable_impl2_t{};

    for(size_t i = 0; i < this->N_KEYS; ++i)
    {
        memtable.insert(generate_key(16), {static_cast<uint64_t>(i), std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max()});
    }

    auto result = hedge::db::index_ops::flush_mem_index2(
        this->_base_path,
        &memtable,
        this->NUM_PARTITION_EXPONENT,
        0,       // flush_iteration
        nullptr, // cache
        false    // use_odirect
    );

    ASSERT_TRUE(result.has_value()) << "Flush failed with error: " << result.error().to_string();
}

TEST_P(sst_test, test_simple_merge)
{
    if(this->NUM_PARTITION_EXPONENT != 0)
        return; // Only test single partition merge for simplicity

    size_t N_KEYS_PER_SST = std::min(this->N_KEYS, (size_t)1000); // Limit keys for merge test

    // Create SST 1
    auto memtable1 = hedge::db::memtable_impl2_t{16 * 1024 * 1024}; // 16MB budget
    std::vector<hedge::key_t> keys1;

    for(size_t i = 0; i < N_KEYS_PER_SST; ++i)
    {
        auto key = generate_key(16);
        keys1.push_back(key);
        memtable1.insert(key, {static_cast<uint64_t>(i), 100, 0});
    }

    auto res1 = hedge::db::index_ops::flush_mem_index2(
        this->_base_path, &memtable1, 0, 1, nullptr, false);
    ASSERT_TRUE(res1.has_value());
    auto ssts1 = std::move(res1.value());
    ASSERT_EQ(ssts1.size(), 1);

    // Create SST 2
    auto memtable2 = hedge::db::memtable_impl2_t{16 * 1024 * 1024}; // 16MB budget
    std::vector<hedge::key_t> keys2;
    for(size_t i = 0; i < N_KEYS_PER_SST; ++i)
    {
        auto key = generate_key(16);
        keys2.push_back(key);
        memtable2.insert(key, {static_cast<uint64_t>(i + N_KEYS_PER_SST), 100, 0});
    }

    auto res2 = hedge::db::index_ops::flush_mem_index2(
        this->_base_path, &memtable2, 0, 2, nullptr, false);
    ASSERT_TRUE(res2.has_value());
    auto ssts2 = std::move(res2.value());
    ASSERT_EQ(ssts2.size(), 1);

    // Prepare Merge
    std::vector<const hedge::db::sst*> indices_to_merge;
    indices_to_merge.push_back(&ssts1[0]);
    indices_to_merge.push_back(&ssts2[0]);

    hedge::db::index_ops::merge_config config{
        .read_ahead_size = 64 * 1024,
        .new_index_id = 3,
        .base_path = this->_base_path,
        .discard_deleted_keys = false,
        .create_new_with_odirect = false,
        .populate_cache_with_output = true,
        .try_reading_from_cache = false};

    auto merge_task = hedge::db::index_ops::k_way_merge_async2(
        config, indices_to_merge, this->_executor, nullptr);

    auto merge_res = this->_executor->sync_submit(std::move(merge_task));

    ASSERT_TRUE(merge_res.has_value()) << "Merge failed: " << merge_res.error().to_string();

    auto merged_sst = std::move(merge_res.value());

    // Verify
    ASSERT_EQ(merged_sst.size(), keys1.size() + keys2.size());

    // Lookup
    auto query_wg = hedge::async::wait_group::make_shared();
    query_wg->set(keys1.size() + keys2.size());
    std::atomic<size_t> found_count{0};

    auto lookup_func = [&](const hedge::key_t& key, uint64_t expected_val_offset) -> hedge::async::task<void>
    {
        auto res = co_await merged_sst.lookup_async(key, nullptr);
        if(res.has_value() && res.value().has_value())
        {
            if(res.value().value().offset() == expected_val_offset)
                found_count.fetch_add(1);
        }
        query_wg->decr();
    };

    for(size_t i = 0; i < keys1.size(); ++i)
        this->_executor->submit_io_task(lookup_func(keys1[i], i));

    for(size_t i = 0; i < keys2.size(); ++i)
        this->_executor->submit_io_task(lookup_func(keys2[i], i + N_KEYS_PER_SST));

    query_wg->wait();

    ASSERT_EQ(found_count.load(), keys1.size() + keys2.size());
}

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    sst_test,
    testing::Combine(
        testing::Values(1000, 5000, 10'000, 200'000, 1'000'000),
        testing::Values(0, 1, 4, 10),
        testing::Bool()),
    [](const testing::TestParamInfo<sst_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto use_cache = std::get<2>(info.param);
        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + (use_cache ? "_CACHE" : "_NOCACHE");
        return name;
    });
