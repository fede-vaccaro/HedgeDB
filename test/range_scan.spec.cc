#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <random>
#include <ranges>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "db/index_ops.h"
#include "db/memtable.h" // For memtable_impl3_t
#include "db/scan_iterator.h"
#include "db/sst.h"
#include "io/io_executor.h"
#include "single_buffer_arena_allocator.h"
#include "tmc/sync.hpp"
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

struct range_scan_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, hedge::value_type>>
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->VALUE_TYPE = std::get<2>(GetParam());

        this->_keys.reserve(this->N_KEYS);

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        this->_executor = std::make_unique<hedge::io::io_executor>(8, 64);
    }

    void TearDown() override
    {
        this->_executor.reset();
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
                                    range_scan_test& test_instance,
                                    std::atomic<size_t>& found_count) -> tmc::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, nullptr);

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
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_index2";
    std::unique_ptr<hedge::io::io_executor> _executor{};

    static constexpr size_t TEST_VALUE_SIZE = 100; // Fixed size for simplicity

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 128};
};

TEST_P(range_scan_test, test_flush_and_range_scan_all_16b_keys)
{
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

    std::filesystem::path base_path = this->_base_path;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto result_future = tmc::post_waitable(
        *_executor,
        hedge::db::index_ops::flush_mem_index2_parallel(
            base_path,
            &memtable,
            this->NUM_PARTITION_EXPONENT,
            0, // flush_iteration
            nullptr,
            false, // use_odirect
            this->_executor->ex(),
            false // fdatasync_ssts
            ));

    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    auto result = result_future.get();

    if(!result.has_value())
    {
        std::cerr << "Failed to flush indices: " << result.error().to_string() << '\n';
        FAIL();
    }
    std::cout << "Flush completed." << std::endl;
    std::cout << "Flush took " << duration / 1000.0 << " ms" << std::endl;

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    // Prepare lookup
    auto make_partition_from_sst = [](hedge::db::sst_ptr_t sst)
    {
        return hedge::db::partition_t{hedge::db::level_t{std::move(sst)}};
    };

    std::map<uint16_t, hedge::db::partition_t> sst_map;
    for(auto& sst : ssts)
        sst_map[sst.upper_bound()] = make_partition_from_sst(std::make_shared<hedge::db::sst>(std::move(sst)));

    // Prepare ground truth
    struct key_with_seed
    {
        hedge::key_t k;
        uint64_t seed;
    };

    using groundtruth_t = std::map<uint16_t, std::vector<key_with_seed>>;
    groundtruth_t keys_gt;
    for(const auto& [i, key] : this->_keys | std::views::enumerate)
    {
        size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        keys_gt[partition_id].emplace_back(key, i);
    }

    for(auto& [prefix, gt_vec] : keys_gt)
    {
        std::ranges::sort(gt_vec, [](const auto& lhs, const auto& rhs)
                          { return lhs.k < rhs.k; });
    }

    // Test range scan

    struct test_result_t
    {
        size_t count;
        size_t expected;
        int64_t duration_us;
    };

    t0 = std::chrono::high_resolution_clock::now();
    std::vector<std::future<hedge::expected<test_result_t>>> futures;
    futures.reserve(sst_map.size());
    for(const auto& [prefix, partition] : sst_map)
    {
        auto f = tmc::post_waitable(
            *this->_executor,
            [](const hedge::db::partition_t& partition, uint16_t prefix, groundtruth_t& keys_gt, auto* test_fixture) -> tmc::task<hedge::expected<test_result_t>>
            {
                auto t0 = std::chrono::high_resolution_clock::now();
                auto maybe_it = hedge::db::scan_iterator::from_partition(
                    partition,
                    std::nullopt,
                    std::nullopt,
                    nullptr,
                    64 * 1024);

                if(!maybe_it)
                    co_return maybe_it.error();

                auto& it = maybe_it.value();

                size_t c = 0;
                while(true)
                {
                    auto maybe_kv = co_await it.next();
                    if(!maybe_kv.has_value() && maybe_kv.error().code() == hedge::errc::END_OF_SCAN)
                        break;

                    if(!maybe_kv.has_value())
                        co_return maybe_kv.error();

                    auto& [k, v] = maybe_kv.value();

                    // check key
                    if(k != keys_gt.at(prefix).at(c).k)
                    {
                        co_return hedge::error(
                            "Key mismatch at partition " + std::to_string(prefix) +
                            " index " + std::to_string(c) +
                            ": " + to_hex_string(k) +
                            " vs " + to_hex_string(keys_gt.at(prefix).at(c).k));
                    }

                    // check value
                    if(!test_fixture->check_returned_payload(v, keys_gt.at(prefix).at(c).seed))
                    {
                        co_return hedge::error(
                            "Value mismatch for key " + to_hex_string(k) +
                            " at partition " + std::to_string(prefix) +
                            " index " + std::to_string(c));
                    }

                    c++;
                }
                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                co_return test_result_t{.count = c, .expected = keys_gt.at(prefix).size(), .duration_us = duration};
            }(partition, prefix, keys_gt, this));
        futures.emplace_back(std::move(f));
    }

    for(auto& f : futures)
    {
        auto s = f.get();
        if(!s)
        {
            std::cerr << "Range scan failed: " << s.error().to_string() << '\n';
            FAIL();
        }

        EXPECT_EQ(s.value().count, s.value().expected);
    }
    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    std::cout << "Range scans completed. Total duration: " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Throughput: " << static_cast<size_t>(this->N_KEYS / (duration / 1'000'000.0)) << " keys/s" << std::endl;
    std::cout << "Average latency per partition: " << (duration / futures.size()) / 1000.0 << " ms" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    range_scan_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(0, 1, 4, 10),
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)),

    [](const testing::TestParamInfo<range_scan_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto value_type = std::get<2>(info.param);
        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + (value_type == hedge::value_type::VALUE_PTR ? "_VALUE_PTR" : "_IN_PLACE_VALUE");
        return name;
    });
