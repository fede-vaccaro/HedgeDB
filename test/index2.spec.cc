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
#include "db/memtable.h" // For memtable_impl3_t
#include "db/sst.h"
#include "single_buffer_arena_allocator.h"
#include "types.h"
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

struct sst_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, bool, hedge::value_type>>
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->USE_CACHE = std::get<2>(GetParam());
        this->VALUE_TYPE = std::get<3>(GetParam());

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

    static void generate_inline_value(std::span<uint8_t> buffer, size_t seed)
    {
        assert(buffer.size() == TEST_VALUE_SIZE);
        seed = seed * 31 + 17; // Simple LCG for deterministic pseudo-random generation
        auto byte_seed = static_cast<uint8_t>(seed & 0xFF);
        std::ranges::for_each(
            buffer,
            [&](uint8_t& byte)
            {
                byte = byte_seed;
                byte_seed = byte_seed * 31 + 17; // Update seed for next byte
            });
    }

    static void generate_value_ptr(std::span<uint8_t> buffer, size_t seed)
    {
        size_t fake_offset = (seed * 1234567) % (1ULL << 32); // Keep it within a reasonable range
        size_t fake_size = (seed % 1024) + 1;                 // Size between 1 and 1024 bytes
        hedge::value_ptr_t vp(fake_offset, static_cast<uint32_t>(fake_size), 0);
        std::memcpy(buffer.data(), &vp, sizeof(vp)); // Write the value_ptr_t into the buffer
    }

    bool check_returned_payload(const hedge::value_t& value, size_t seed)
    {
        bool match =
            std::visit(hedge::overloaded{
                           [seed](const hedge::value_ptr_t& v) -> bool
                           {
                               hedge::value_ptr_t groundtruth{};
                               generate_value_ptr(std::span<uint8_t>{reinterpret_cast<uint8_t*>(&groundtruth), sizeof(groundtruth)}, seed);
                               return groundtruth == v;
                           },
                           [seed](const std::vector<uint8_t>& v) -> bool
                           {
                               std::vector<uint8_t> groundtruth(TEST_VALUE_SIZE);
                               generate_inline_value(groundtruth, seed);
                               return std::equal(groundtruth.begin(), groundtruth.end(), v.begin());
                           },
                           [](const hedge::tombstone_t& /*v*/) -> bool
                           {
                               return true;
                           },
                       },
                       value);
        return match;
    }

    static auto lookup_task_factory(size_t j,
                                    const hedge::key_t& key,
                                    const hedge::db::sst& sst,
                                    std::shared_ptr<hedge::async::wait_group> wg,
                                    sst_test& test_instance,
                                    std::atomic<size_t>& found_count,
                                    std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr) -> hedge::async::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, cache);

        if(!lookup.has_value())
        {
            std::cerr << "Error during lookup for key: " << to_hex_string(key) << " error: " << lookup.error().to_string() << '\n';
        }
        else
        {
            auto val = std::move(lookup.value());

            if(test_instance.check_returned_payload(val, j))
            {
                found_count.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                std::cerr << "Value mismatch for key: " << to_hex_string(key) << '\n';
            }
        }

        wg->decr();
    };

    std::span<uint8_t> generate_value(hedge::single_buffer_arena_allocator& arena, size_t seed, hedge::value_type type)
    {
        std::span<uint8_t> buffer;

        switch(type)
        {
            case hedge::value_type::VALUE_PTR:
                buffer = arena.allocate(sizeof(hedge::value_ptr_t) + 1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<uint8_t>(hedge::value_type::VALUE_PTR);
                generate_value_ptr(buffer.subspan(1), seed);
                break;
            case hedge::value_type::IN_PLACE_VALUE:
                buffer = arena.allocate(TEST_VALUE_SIZE + 1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<uint8_t>(hedge::value_type::IN_PLACE_VALUE);
                generate_inline_value(buffer.subspan(1), seed);
                break;
            case hedge::value_type::TOMBSTONE:
                buffer = arena.allocate(1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<uint8_t>(hedge::value_type::TOMBSTONE);
                break;
            default:
                throw std::invalid_argument("Invalid value type");
        }

        return buffer;
    }

    size_t N_KEYS = 2000000;
    size_t NUM_PARTITION_EXPONENT = 0;
    bool USE_CACHE = false;
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_index2";
    std::shared_ptr<hedge::async::executor_context> _executor{};

    static constexpr size_t TEST_VALUE_SIZE = 100; // Fixed size for simplicity

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 128};
};

TEST_P(sst_test, test_flush_and_lookup_16b_keys)
{
    std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr;
    if(this->USE_CACHE)
    {
        constexpr auto CACHE_MAX_SIZE = 64 * 1024 * 1024;
        cache = std::make_shared<hedge::db::sharded_page_cache>(CACHE_MAX_SIZE, 1);
    }

    auto memtable = hedge::db::memtable_impl3_t{1024 * 1024 * 128};

    std::cout << "Generating " << this->N_KEYS << " keys..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        const auto& key = this->_keys.back();

        auto buffer = generate_value(arena, j, this->VALUE_TYPE);
        memtable.insert(key, buffer);
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

    std::cout << "Reading back keys..." << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
    {
        sst_map[sst.upper_bound()] = &sst;
    }

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    for(size_t j = 0; j < this->_keys.size(); ++j)
    {
        const auto& key = this->_keys[j];

        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            this->_executor->submit_io_task(lookup_task_factory(j, key, *it->second, query_wg, *this, found_count, cache));
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
            unexpected_found_count.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "Unexpectedly found key: " << to_hex_string(key) << '\n';
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

    auto memtable = hedge::db::memtable_impl3_t{1024 * 1024 * 128};

    std::cout << "Generating " << this->N_KEYS << " keys with variable length..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(len_dist(generator)));

        auto buffer = generate_value(arena, j, this->VALUE_TYPE);

        memtable.insert(this->_keys.back(), buffer);
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

    std::cout << "Reading back keys..." << std::endl;
    t0 = std::chrono::high_resolution_clock::now();
    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
    {
        sst_map[sst.upper_bound()] = &sst;
    }

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    for(size_t j = 0; j < this->_keys.size(); ++j)
    {
        const auto& key = this->_keys[j];
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            this->_executor->submit_io_task(lookup_task_factory(j, key, *it->second, query_wg, *this, found_count, cache));
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
    auto memtable = hedge::db::memtable_impl3_t{1024 * 1024 * 128};

    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        const auto& key = this->_keys.back();

        auto buffer = arena.allocate(TEST_VALUE_SIZE + 1);
        ASSERT_NE(buffer.data(), nullptr);
        buffer[0] = static_cast<uint8_t>(hedge::value_type::IN_PLACE_VALUE);
        generate_inline_value(buffer.subspan(1), j);

        memtable.insert(key, buffer);
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

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    sst_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(0, 1, 4, 10),
        testing::Bool(),                                                                   // Use cache or not
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)), // Test using both value pointers and in-place values

    [](const testing::TestParamInfo<sst_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto use_cache = std::get<2>(info.param);
        auto value_type = std::get<3>(info.param);
        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + (use_cache ? "_CACHE" : "_NOCACHE") + (value_type == hedge::value_type::VALUE_PTR ? "_VALUE_PTR" : "_IN_PLACE_VALUE");
        return name;
    });
