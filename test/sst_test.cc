#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <random>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "cache.h"
#include "db/index_ops.h"
#include "db/memtable.h"
#include "db/sst.h"
#include "io/io_executor.h"
#include "single_buffer_arena_allocator.h"
#include "types.h"
#include "utils.h"

namespace
{
    std::string to_hex_string(const hedge::key_t& key)
    {
        auto key_span = static_cast<std::span<const std::byte>>(key);
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for(std::byte c : key_span)
            ss << std::setw(2) << static_cast<int>(c);
        return ss.str();
    }
} // namespace

struct sst_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, bool, hedge::value_type>>
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

        this->_executor = std::make_unique<hedge::io::io_executor>(
            hedge::io::executor_config{
                .queue_depth = 128,
                .n_threads = 4,
                .auto_detect = false,
            });

        if(this->USE_CACHE)
        {
            constexpr auto CACHE_MAX_SIZE = 64 * 1024 * 1024;
            this->_cache = std::make_shared<hedge::db::sharded_page_cache>(CACHE_MAX_SIZE, 1);
        }
    }

    void TearDown() override
    {
        this->_executor.reset();
        this->_cache.reset();
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
        std::ranges::for_each(
            buffer,
            [&](std::byte& byte)
            {
                byte = byte_seed;
                byte_seed = static_cast<std::byte>(static_cast<unsigned>(byte_seed) * 31 + 17);
            });
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
                                    sst_test& test_instance,
                                    std::atomic<size_t>& found_count,
                                    std::shared_ptr<hedge::db::sharded_page_cache> cache) -> tmc::task<void>
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
                found_count.fetch_add(1, std::memory_order_relaxed);
            else
                std::cerr << "Value mismatch for key: " << to_hex_string(key) << '\n';
        }
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

    hedge::expected<std::vector<hedge::db::sst>> flush(hedge::db::skiplist_wrapper& memtable)
    {
        auto begin = memtable.accessor().cbegin();
        auto end = memtable.accessor().cend();
        return tmc::post_waitable(
                   *_executor,
                   hedge::db::index_ops::flush_memtable(
                       this->_base_path,
                       begin,
                       end,
                       this->NUM_PARTITION_EXPONENT,
                       0,
                       this->_cache,
                       false,
                       this->_executor->ex(),
                       false))
            .get();
    }

    void run_lookups(const std::vector<hedge::key_t>& keys,
                     const std::map<uint16_t, const hedge::db::sst*>& sst_map,
                     size_t partition_key_prefix_range,
                     std::atomic<size_t>& found_count)
    {
        std::vector<std::future<void>> lookup_futures;
        for(size_t j = 0; j < keys.size(); ++j)
        {
            const auto& key = keys[j];
            size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
            auto it = sst_map.find(partition_id);
            if(it != sst_map.end())
            {
                auto f = tmc::post_waitable(
                    *this->_executor,
                    lookup_task_factory(j, key, *it->second, *this, found_count, this->_cache),
                    0,
                    j % this->_executor->num_threads());
                lookup_futures.emplace_back(std::move(f));
            }
        }

        for(auto& f : lookup_futures)
            f.get();
    }

    size_t N_KEYS = 2000000;
    size_t NUM_PARTITION_EXPONENT = 0;
    bool USE_CACHE = false;
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_sst";
    std::unique_ptr<hedge::io::io_executor> _executor{};
    std::shared_ptr<hedge::db::sharded_page_cache> _cache{};

    static constexpr size_t TEST_VALUE_SIZE = 100;

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 128};
};

TEST_P(sst_test, flush_and_lookup_16b_keys)
{
    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    std::cout << "Generating " << this->N_KEYS << " keys..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        auto buffer = generate_value(arena, j, this->VALUE_TYPE);
        memtable.insert(this->_keys.back(), buffer);
    }

    std::cout << "Keys generated." << std::endl;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto result = flush(memtable);
    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    ASSERT_TRUE(result.has_value()) << "Failed to flush indices: " << result.error().to_string();

    std::cout << "Flush took " << duration / 1000.0 << " ms" << std::endl;

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
        sst_map[sst.upper_bound()] = &sst;

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    // --- Positive lookup ---
    std::atomic<size_t> found_count{0};
    t0 = std::chrono::high_resolution_clock::now();
    run_lookups(this->_keys, sst_map, partition_key_prefix_range, found_count);
    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    std::cout << "Lookup took " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Found " << found_count.load() << " / " << this->_keys.size() << " keys" << std::endl;
    double throughput = static_cast<double>(this->_keys.size()) / (duration / 1000000.0);
    std::cout << "Lookup Throughput: " << std::fixed << std::setprecision(2) << throughput << " ops/s" << std::endl;

    ASSERT_EQ(found_count.load(), this->_keys.size());

    // --- Negative lookup: random keys are statistically not in the set (1 in 10^30 collision odds) ---
    std::vector<hedge::key_t> negative_keys;
    size_t num_negative_keys = std::max(static_cast<size_t>(1), this->N_KEYS / 10);
    negative_keys.reserve(num_negative_keys);
    for(size_t i = 0; i < num_negative_keys; ++i)
        negative_keys.emplace_back(generate_key(16));

    std::atomic<size_t> unexpected_found_count{0};

    auto negative_lookup_task_factory = [&](const hedge::key_t& key,
                                            const hedge::db::sst& sst) -> tmc::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, this->_cache);

        if(lookup.has_value())
        {
            unexpected_found_count.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "Unexpectedly found key: " << to_hex_string(key) << '\n';
        }
    };

    t0 = std::chrono::high_resolution_clock::now();
    std::vector<std::future<void>> negative_lookup_futures;
    for(size_t j = 0; j < negative_keys.size(); ++j)
    {
        const auto& key = negative_keys[j];
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        auto it = sst_map.find(partition_id);
        if(it != sst_map.end())
        {
            auto f = tmc::post_waitable(*this->_executor, negative_lookup_task_factory(key, *it->second), 0, j % this->_executor->num_threads());
            negative_lookup_futures.emplace_back(std::move(f));
        }
    }

    for(auto& f : negative_lookup_futures)
        f.get();

    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    std::cout << "Negative lookup took " << duration / 1000.0 << " ms" << std::endl;

    if(unexpected_found_count.load() > 0)
        FAIL() << "Found " << unexpected_found_count.load() << " keys that should not exist.";
}

TEST_P(sst_test, flush_and_lookup_variable_keys)
{
    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(len_dist(generator)));
        auto buffer = generate_value(arena, j, this->VALUE_TYPE);
        memtable.insert(this->_keys.back(), buffer);
    }

    auto result = flush(memtable);
    ASSERT_TRUE(result.has_value()) << "Failed to flush indices: " << result.error().to_string();

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& sst : ssts)
        sst_map[sst.upper_bound()] = &sst;

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    std::atomic<size_t> found_count{0};
    run_lookups(this->_keys, sst_map, partition_key_prefix_range, found_count);

    ASSERT_EQ(found_count.load(), this->_keys.size());
}

TEST_P(sst_test, load_from_disk)
{
    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        auto buffer = generate_value(arena, j, this->VALUE_TYPE);
        memtable.insert(this->_keys.back(), buffer);
    }

    auto result = flush(memtable);
    ASSERT_TRUE(result.has_value()) << "Flush failed: " << result.error().to_string();

    // Collect paths and destroy original SSTs
    std::map<uint16_t, std::filesystem::path> sst_path_map;
    for(const auto& sst : result.value())
        sst_path_map[sst.upper_bound()] = sst.path();
    result.value().clear();

    // Reload each SST from disk
    std::map<uint16_t, hedge::db::sst> loaded_ssts;
    std::map<uint16_t, const hedge::db::sst*> sst_map;
    for(const auto& [ub, p] : sst_path_map)
    {
        auto maybe_loaded = hedge::db::sst::load(p);
        ASSERT_TRUE(maybe_loaded.has_value()) << "sst::load failed for " << p << ": " << maybe_loaded.error().to_string();
        auto [it, _] = loaded_ssts.emplace(ub, std::move(maybe_loaded.value()));
        sst_map[ub] = &it->second;
    }

    size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);

    std::atomic<size_t> found_count{0};
    run_lookups(this->_keys, sst_map, partition_key_prefix_range, found_count);

    ASSERT_EQ(found_count.load(), this->_keys.size());
}

TEST_P(sst_test, flush_succeeds)
{
    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        auto buffer = arena.allocate(TEST_VALUE_SIZE + 1);
        ASSERT_NE(buffer.data(), nullptr);
        buffer[0] = static_cast<std::byte>(hedge::value_type::IN_PLACE_VALUE);
        generate_inline_value(buffer.subspan(1), j);
        memtable.insert(this->_keys.back(), buffer);
    }

    auto result = flush(memtable);
    ASSERT_TRUE(result.has_value()) << "Flush failed: " << result.error().to_string();
}

INSTANTIATE_TEST_SUITE_P(
    sst_suite,
    sst_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(0, 1, 4, 10),
        // USE_CACHE: only `false` is exercised — the cache is currently a stub. Restore `testing::Bool()` once the cache is reimplemented.
        testing::Values(false),
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)),

    [](const testing::TestParamInfo<sst_test::ParamType>& info)
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
