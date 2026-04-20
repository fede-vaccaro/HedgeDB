#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <random>
#include <ranges>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "async/wait_group.h"
#include "cache.h"
#include "db/index_ops.h"
#include "db/memtable.h"
#include "db/sst.h"
#include "io/io_executor.h"
#include "single_buffer_arena_allocator.h"
#include "types.h"
#include "utils.h"

static std::string key_to_hex(const hedge::key_t& key)
{
    auto key_span = static_cast<std::span<const std::byte>>(key);
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for(std::byte c : key_span)
        ss << std::setw(2) << static_cast<int>(c);
    return ss.str();
}

struct parallel_flush_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, bool, hedge::value_type>>
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->USE_CACHE = std::get<2>(GetParam());
        this->VALUE_TYPE = std::get<3>(GetParam());

        this->_keys.reserve(this->N_KEYS);

        if(std::filesystem::exists(this->_base_path))
            std::filesystem::remove_all(this->_base_path);
        std::filesystem::create_directories(this->_base_path);

        this->_executor = hedge::async::executor_context::make_new(128);

        this->_flush_pool.resize(4);
        for(auto& ex : this->_flush_pool)
            ex = hedge::async::executor_context::make_new(32);
    }

    void TearDown() override
    {
        for(auto& ex : this->_flush_pool)
        {
            if(ex)
                ex->shutdown();
        }

        if(this->_executor)
        {
            this->_executor->shutdown();
            this->_executor.reset();
        }
    }

    hedge::key_t generate_key(size_t length)
    {
        auto k = hedge::key_t::make_with_length(length);
        auto span = static_cast<std::span<std::byte>>(k);
        for(size_t i = 0; i < length; ++i)
            span[i] = static_cast<std::byte>(dist(generator));
        return k;
    }

    static void generate_inline_value(std::span<std::byte> buffer, size_t seed)
    {
        assert(buffer.size() == TEST_VALUE_SIZE);
        seed = seed * 31 + 17;
        auto byte_seed = static_cast<std::byte>(seed & 0xFF);
        std::ranges::for_each(buffer, [&](std::byte& byte)
                              {
                                  byte = byte_seed;
                                  byte_seed = byte_seed * 31 + 17; });
    }

    static void generate_value_ptr(std::span<std::byte> buffer, size_t seed)
    {
        size_t fake_offset = (seed * 1234567) % (1ULL << 32);
        size_t fake_size = (seed % 1024) + 1;
        hedge::value_ptr_t vp(fake_offset, static_cast<uint32_t>(fake_size), 0);
        std::memcpy(buffer.data(), &vp, sizeof(vp));
    }

    bool check_returned_payload(const hedge::value_t& value, size_t seed)
    {
        return std::visit(hedge::overloaded{
                              [seed](const hedge::value_ptr_t& v) -> bool
                              {
                                  hedge::value_ptr_t groundtruth{};
                                  generate_value_ptr(std::span<std::byte>{reinterpret_cast<std::byte*>(&groundtruth), sizeof(groundtruth)}, seed);
                                  return groundtruth == v;
                              },
                              [seed](const std::vector<std::byte>& v) -> bool
                              {
                                  std::vector<std::byte> groundtruth(TEST_VALUE_SIZE);
                                  generate_inline_value(groundtruth, seed);
                                  return std::equal(groundtruth.begin(), groundtruth.end(), v.begin());
                              },
                              [](const hedge::tombstone_t&) -> bool
                              {
                                  return true;
                              },
                          },
                          value);
    }

    static auto lookup_task_factory(size_t j,
                                    const hedge::key_t& key,
                                    const hedge::db::sst& sst,
                                    std::shared_ptr<hedge::async::wait_group> wg,
                                    parallel_flush_test& test_instance,
                                    std::atomic<size_t>& found_count,
                                    std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr) -> hedge::async::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, cache);

        if(!lookup.has_value())
        {
            std::cerr << "Error during lookup for key: " << key_to_hex(key) << " error: " << lookup.error().to_string() << '\n';
        }
        else
        {
            auto val = std::move(lookup.value());
            if(test_instance.check_returned_payload(val, j))
                found_count.fetch_add(1, std::memory_order_relaxed);
            else
                std::cerr << "Value mismatch for key: " << key_to_hex(key) << '\n';
        }

        wg->decr();
    };

    std::span<std::byte> generate_value(hedge::single_buffer_arena_allocator& alloc, size_t seed, hedge::value_type type)
    {
        std::span<std::byte> buffer;

        switch(type)
        {
            case hedge::value_type::VALUE_PTR:
                buffer = alloc.allocate(sizeof(hedge::value_ptr_t) + 1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<std::byte>(hedge::value_type::VALUE_PTR);
                generate_value_ptr(buffer.subspan(1), seed);
                break;
            case hedge::value_type::IN_PLACE_VALUE:
                buffer = alloc.allocate(TEST_VALUE_SIZE + 1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<std::byte>(hedge::value_type::IN_PLACE_VALUE);
                generate_inline_value(buffer.subspan(1), seed);
                break;
            case hedge::value_type::TOMBSTONE:
                buffer = alloc.allocate(1);
                assert(buffer.data() != nullptr);
                buffer[0] = static_cast<std::byte>(hedge::value_type::TOMBSTONE);
                break;
            default:
                throw std::invalid_argument("Invalid value type");
        }

        return buffer;
    }

    void flush_and_verify(bool use_variable_keys)
    {
        std::shared_ptr<hedge::db::sharded_page_cache> cache = nullptr;
        if(this->USE_CACHE)
        {
            constexpr auto CACHE_MAX_SIZE = 64 * 1024 * 1024;
            cache = std::make_shared<hedge::db::sharded_page_cache>(CACHE_MAX_SIZE, 1);
        }

        auto memtable = hedge::db::memtable_impl3_t{1024 * 1024 * 128};

        for(size_t j = 0; j < this->N_KEYS; ++j)
        {
            size_t key_len = use_variable_keys ? len_dist(generator) : 16;
            this->_keys.emplace_back(generate_key(key_len));
            auto buffer = generate_value(arena, j, this->VALUE_TYPE);
            memtable.insert(this->_keys.back(), buffer);
        }

        auto t0 = std::chrono::high_resolution_clock::now();
        auto result = hedge::db::index_ops::flush_memtable(
            this->_base_path,
            &memtable,
            this->NUM_PARTITION_EXPONENT,
            0,
            cache,
            false,
            this->_flush_pool);
        auto t1 = std::chrono::high_resolution_clock::now();
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        ASSERT_TRUE(result.has_value()) << "Parallel flush failed: " << result.error().to_string();
        std::cout << "Parallel flush took " << duration_us / 1000.0 << " ms" << std::endl;

        std::vector<hedge::db::sst> ssts = std::move(result.value());
        ASSERT_FALSE(ssts.empty());

        // Build lookup map
        std::map<uint16_t, const hedge::db::sst*> sst_map;
        for(const auto& sst : ssts)
            sst_map[sst.upper_bound()] = &sst;

        size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

        // Verify all keys
        auto query_wg = hedge::async::wait_group::make_shared();
        query_wg->set(this->_keys.size());
        std::atomic<size_t> found_count{0};

        t0 = std::chrono::high_resolution_clock::now();

        for(size_t j = 0; j < this->_keys.size(); ++j)
        {
            const auto& key = this->_keys[j];
            size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
            auto it = sst_map.find(partition_id);
            if(it != sst_map.end())
                this->_executor->submit_io_task(lookup_task_factory(j, key, *it->second, query_wg, *this, found_count, cache));
            else
                query_wg->decr();
        }

        query_wg->wait();

        t1 = std::chrono::high_resolution_clock::now();
        duration_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        std::cout << "Lookup took " << duration_us / 1000.0 << " ms" << std::endl;
        std::cout << "Found " << found_count.load() << " / " << this->_keys.size() << " keys" << std::endl;

        ASSERT_EQ(found_count.load(), this->_keys.size());
    }

    size_t N_KEYS{};
    size_t NUM_PARTITION_EXPONENT{};
    bool USE_CACHE{};
    hedge::value_type VALUE_TYPE{};
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_parallel_flush";
    std::shared_ptr<hedge::async::executor_context> _executor{};
    std::vector<std::shared_ptr<hedge::async::executor_context>> _flush_pool;

    static constexpr size_t TEST_VALUE_SIZE = 100;

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 128};
};

TEST_P(parallel_flush_test, fixed_length_keys)
{
    flush_and_verify(false);
}

TEST_P(parallel_flush_test, variable_length_keys)
{
    flush_and_verify(true);
}

INSTANTIATE_TEST_SUITE_P(
    parallel_flush,
    parallel_flush_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(0, 1, 4, 10),
        testing::Bool(),
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)),

    [](const testing::TestParamInfo<parallel_flush_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto use_cache = std::get<2>(info.param);
        auto value_type = std::get<3>(info.param);
        return "N_" + std::to_string(num_keys) +
               "_P_" + std::to_string(num_partitions) +
               (use_cache ? "_CACHE" : "_NOCACHE") +
               (value_type == hedge::value_type::VALUE_PTR ? "_VALUE_PTR" : "_IN_PLACE_VALUE");
    });
