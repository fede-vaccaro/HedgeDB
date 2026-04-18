#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iomanip>
#include <map>
#include <random>
#include <sstream>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "db/index_ops.h"
#include "db/memtable.h"
#include "db/sst.h"
#include "io/io_executor.h"
#include "single_buffer_arena_allocator.h"
#include "tmc/sync.hpp"
#include "types.h"
#include "utils.h"

namespace
{
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
} // namespace

class merge_test_base : public ::testing::Test
{
protected:
    static constexpr size_t TEST_VALUE_SIZE = 100;

    std::filesystem::path _base_path;
    std::unique_ptr<hedge::io::io_executor> _executor{};

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{64, 128};

    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 512};

    void SetUp() override
    {
        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        this->_executor = std::make_unique<hedge::io::io_executor>(4, 32);
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
        assert(buffer.size() == sizeof(hedge::value_ptr_t));
        hedge::value_ptr_t vp(static_cast<uint64_t>(seed), 100, 0);
        std::memcpy(buffer.data(), &vp, sizeof(vp));
    }

    std::span<uint8_t> generate_value(hedge::single_buffer_arena_allocator& arena, size_t seed, hedge::value_type type)
    {
        std::span<uint8_t> buffer;

        switch(type)
        {
            case hedge::value_type::VALUE_PTR:
                buffer = arena.allocate(sizeof(hedge::value_ptr_t) + 1);
                assert(!buffer.empty());
                buffer[0] = static_cast<uint8_t>(hedge::value_type::VALUE_PTR);
                generate_value_ptr(buffer.subspan(1), seed);
                break;
            case hedge::value_type::IN_PLACE_VALUE:
                buffer = arena.allocate(TEST_VALUE_SIZE + 1);
                assert(!buffer.empty());
                buffer[0] = static_cast<uint8_t>(hedge::value_type::IN_PLACE_VALUE);
                generate_inline_value(buffer.subspan(1), seed);
                break;
            case hedge::value_type::TOMBSTONE:
                buffer = arena.allocate(1);
                assert(!buffer.empty());
                buffer[0] = static_cast<uint8_t>(hedge::value_type::TOMBSTONE);
                break;
            default:
                throw std::invalid_argument("Invalid value type: " + std::to_string(static_cast<int>(type)));
        }

        return buffer;
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
};

// Parameters: <N_KEYS_PER_RUN, N_RUNS, NUM_PARTITION_EXPONENT, READ_AHEAD_SIZE, VALUE_TYPE>
class merge_test_suite
    : public merge_test_base,
      public ::testing::WithParamInterface<std::tuple<size_t, size_t, size_t, size_t, hedge::value_type>>
{
public:
    void SetUp() override
    {
        this->N_KEYS_PER_RUN = std::get<0>(GetParam());
        this->N_RUNS = std::get<1>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<2>(GetParam());
        this->READ_AHEAD_SIZE = std::get<3>(GetParam());
        this->VALUE_TYPE = std::get<4>(GetParam());

        // Skip if too few keys for partitions (avoid empty partitions effectively)
        if(this->NUM_PARTITION_EXPONENT > 0 && this->N_KEYS_PER_RUN < (1ULL << this->NUM_PARTITION_EXPONENT))
        {
            GTEST_SKIP() << "Skipping test with fewer keys than partitions";
        }

        this->_base_path = "/tmp/hh/test_merge_suite";
        merge_test_base::SetUp();
    }

    size_t N_KEYS_PER_RUN;
    size_t N_RUNS;
    size_t NUM_PARTITION_EXPONENT;
    size_t READ_AHEAD_SIZE;
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;

    void run_merge_test(bool variable_keys) // NOLINT(readability-function-cognitive-complexity)
    {
        std::cout << "Starting merge test with Params: "
                  << "Keys/Run=" << N_KEYS_PER_RUN
                  << ", Runs=" << N_RUNS
                  << ", PartExp=" << NUM_PARTITION_EXPONENT
                  << ", ReadAhead=" << READ_AHEAD_SIZE
                  << ", VarKeys=" << (variable_keys ? "Yes" : "No") << std::endl;

        // 1. Generate Data and Flush SSTs
        std::vector<std::vector<hedge::key_t>> all_keys_per_run(N_RUNS);

        // Store all SSTs grouped by partition prefix
        // key: partition_prefix, value: vector of SSTs (one from each run that has this partition)
        std::map<uint16_t, std::vector<hedge::db::sst>> ssts_by_partition;

        std::atomic_uint64_t seq_nr{0};
        for(size_t run_idx = 0; run_idx < N_RUNS; ++run_idx)
        {
            auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};
            auto& keys = all_keys_per_run[run_idx];
            keys.reserve(N_KEYS_PER_RUN);

            for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
            {
                auto key = variable_keys ? generate_key(len_dist(generator)) : generate_key(16);
                keys.push_back(key);
                size_t value_seed = (run_idx * 1000000) + i;
                auto buffer = generate_value(arena, value_seed, VALUE_TYPE);
                memtable.insert(key, buffer);
            }

            auto begin = memtable.accessor().cbegin();
            auto end = memtable.accessor().cend();

            auto res = tmc::post_waitable(
                           *this->_executor,
                           hedge::db::index_ops::flush_memtable(
                               _base_path, begin, end, NUM_PARTITION_EXPONENT, run_idx, nullptr, false, this->_executor->ex(), false))
                           .get();

            ASSERT_TRUE(res.has_value()) << "Flush failed: " << res.error().to_string();
            auto ssts = std::move(res.value());

            for(auto& sst : ssts)
            {
                uint16_t prefix = sst.upper_bound();
                ssts_by_partition[prefix].push_back(std::move(sst));
            }
        }

        // 2. Merge SSTs for each partition
        std::vector<hedge::db::sst> merged_ssts;
        std::mutex merged_ssts_mutex;

        auto merge_job = [this, &merged_ssts, &merged_ssts_mutex](
                             std::vector<const hedge::db::sst*> inputs,
                             uint16_t p_prefix) -> tmc::task<void>
        {
            hedge::db::index_ops::merge_config config{
                .read_ahead_size = READ_AHEAD_SIZE,
                .new_index_id = N_RUNS + 1,
                .base_path = _base_path,
                .discard_deleted_keys = false,
                .create_new_with_odirect = false,
                .populate_cache_with_output = false};

            auto res = co_await hedge::db::index_ops::k_way_merge_async2(config, inputs, nullptr);

            if(!res.has_value())
            {
                std::cerr << "Merge failed for prefix " << p_prefix << ": " << res.error().to_string() << "\n";
            }
            else
            {
                std::lock_guard<std::mutex> lock(merged_ssts_mutex);
                merged_ssts.push_back(std::move(res.value()));
            }
        };

        {
            std::vector<tmc::task<void>> merge_tasks;
            for(auto& [prefix, sst_list] : ssts_by_partition)
            {
                std::vector<const hedge::db::sst*> inputs;
                for(const auto& sst : sst_list)
                    inputs.push_back(&sst);
                merge_tasks.push_back(merge_job(std::move(inputs), prefix));
            }
            tmc::post_bulk_waitable(*this->_executor, merge_tasks.begin(), merge_tasks.size()).get();
        }

        // 3. Verify merged SSTs contain all keys with correct values
        size_t total_keys_inserted = N_KEYS_PER_RUN * N_RUNS;
        size_t total_keys_in_merged = 0;
        for(const auto& sst : merged_ssts)
        {
            total_keys_in_merged += sst.size();
        }

        // Note: If duplicate keys were generated (very unlikely with 16b random, slightly more likely with variable length if short keys generated), size might be less.
        // But with 8-128 bytes, collision is still very unlikely for 10k keys.
        ASSERT_EQ(total_keys_in_merged, total_keys_inserted);

        // Prepare lookup map for merged SSTs
        std::map<uint16_t, const hedge::db::sst*> merged_sst_map;
        for(const auto& sst : merged_ssts)
        {
            merged_sst_map[sst.upper_bound()] = &sst;
        }

        std::atomic<size_t> found_count{0};
        size_t partition_key_prefix_range = (1 << 16) / (1 << NUM_PARTITION_EXPONENT);

        auto lookup_func = [&](const hedge::key_t& key, uint64_t value_seed) -> tmc::task<void>
        {
            size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
            auto it = merged_sst_map.find(partition_id);

            if(it != merged_sst_map.end())
            {
                auto res = co_await it->second->lookup_async(key, nullptr);
                if(!res.has_value())
                {
                    std::cerr << "Lookup error for key " << to_hex_string(key) << ": " << res.error().to_string() << "\n";
                }
                else
                {
                    auto val = std::move(res.value());
                    if(check_returned_payload(val, value_seed))
                        found_count.fetch_add(1);
                    else
                        std::cerr << "Value mismatch for key " << to_hex_string(key) << "\n";
                }
            }
        };

        {
            std::vector<tmc::task<void>> lookup_tasks;
            lookup_tasks.reserve(total_keys_inserted);
            for(size_t run_idx = 0; run_idx < N_RUNS; ++run_idx)
            {
                for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
                {
                    const auto& key = all_keys_per_run[run_idx][i];
                    uint64_t expected_val_seed = (run_idx * 1000000) + i;
                    lookup_tasks.push_back(lookup_func(key, expected_val_seed));
                }
            }
            tmc::post_bulk_waitable(*this->_executor, lookup_tasks.begin(), lookup_tasks.size()).get();
        }

        ASSERT_EQ(found_count.load(), total_keys_inserted);
    }
};

TEST_P(merge_test_suite, test_k_way_merge_fixed_keys)
{
    run_merge_test(false);
}

TEST_P(merge_test_suite, test_k_way_merge_variable_keys)
{
    run_merge_test(true);
}

INSTANTIATE_TEST_SUITE_P(
    MergeTests,
    merge_test_suite,
    testing::Combine(
        testing::Values(10'000, 100'000, 500'000),                                       // N_KEYS_PER_RUN
        testing::Values(2, 4),                                                           // N_RUNS
        testing::Values(0, 1, 2, 4),                                                     // NUM_PARTITION_EXPONENT
        testing::Values(4096, 65536),                                                    // READ_AHEAD_SIZE
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE) // VALUE_TYPE
        ),
    [](const testing::TestParamInfo<merge_test_suite::ParamType>& info)
    {
        auto keys = std::get<0>(info.param);
        auto runs = std::get<1>(info.param);
        auto part = std::get<2>(info.param);
        auto ra = std::get<3>(info.param);
        auto value_type = std::get<4>(info.param);
        std::stringstream ss;
        ss << "Keys" << keys << "_Runs" << runs << "_PartExp" << part << "_RA" << ra;
        ss << (value_type == hedge::value_type::VALUE_PTR ? "_V_PTR" : "_V_INPLACE");
        return ss.str();
    });

// Parameters: <N_KEYS_PER_RUN, NUM_PARTITION_EXPONENT, READ_AHEAD_SIZE, deletion_fraction, VALUE_TYPE>
class gc_merge_test_suite
    : public merge_test_base,
      public ::testing::WithParamInterface<std::tuple<size_t, size_t, size_t, double, hedge::value_type>>
{
public:
    void SetUp() override
    {
        this->N_KEYS_PER_RUN = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->READ_AHEAD_SIZE = std::get<2>(GetParam());
        this->deletion_fraction = std::get<3>(GetParam());
        this->VALUE_TYPE = std::get<4>(GetParam());

        if(this->NUM_PARTITION_EXPONENT > 0 && this->N_KEYS_PER_RUN < (1ULL << this->NUM_PARTITION_EXPONENT))
        {
            GTEST_SKIP() << "Skipping test with fewer keys than partitions";
        }

        this->_base_path = "/tmp/hh/test_gc_suite";
        merge_test_base::SetUp();
    }

    size_t N_KEYS_PER_RUN;
    size_t NUM_PARTITION_EXPONENT;
    size_t READ_AHEAD_SIZE;
    double deletion_fraction;
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;

    void run_gc_test(bool variable_keys) // NOLINT(readability-function-cognitive-complexity)
    {
        std::cout << "Starting GC test with Params: "
                  << "Keys=" << N_KEYS_PER_RUN
                  << ", PartExp=" << NUM_PARTITION_EXPONENT
                  << ", ReadAhead=" << READ_AHEAD_SIZE
                  << ", DeletionFraction=" << deletion_fraction
                  << ", VarKeys=" << (variable_keys ? "Yes" : "No") << std::endl;

        // 1. Insert phase (epoch 0)
        std::vector<hedge::key_t> all_keys;
        all_keys.reserve(N_KEYS_PER_RUN);

        std::atomic_uint64_t seq_nr{0};
        auto memtable0 = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};
        for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
        {
            auto key = variable_keys ? generate_key(len_dist(generator)) : generate_key(16);
            all_keys.push_back(key);
            memtable0.insert(key, generate_value(arena, i, VALUE_TYPE));
        }

        auto begin = memtable0.accessor().cbegin();
        auto end = memtable0.accessor().cend();

        auto flush0_res = tmc::post_waitable(
                              *this->_executor,
                              hedge::db::index_ops::flush_memtable(
                                  _base_path, begin, end, NUM_PARTITION_EXPONENT, 0, nullptr, false, this->_executor->ex(), false))
                              .get();
        ASSERT_TRUE(flush0_res.has_value()) << "Flush 0 failed: " << flush0_res.error().to_string();

        // 2. Tombstone overlay phase (epoch 1): flip weighted coin per key
        std::vector<bool> is_deleted(N_KEYS_PER_RUN, false);
        std::bernoulli_distribution coin(deletion_fraction);
        size_t total_deleted = 0;

        auto memtable1 = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};
        for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
        {
            if(coin(generator))
            {
                is_deleted[i] = true;
                ++total_deleted;
                memtable1.insert(all_keys[i], generate_value(arena, i, hedge::value_type::TOMBSTONE));
            }
        }

        std::vector<hedge::db::sst> ssts1;
        if(total_deleted > 0)
        {
            auto begin = memtable1.accessor().cbegin();
            auto end = memtable1.accessor().cend();

            auto flush1_res = tmc::post_waitable(
                                  *this->_executor,
                                  hedge::db::index_ops::flush_memtable(
                                      _base_path, begin, end, NUM_PARTITION_EXPONENT, 1, nullptr, false, this->_executor->ex(), false))
                                  .get();
            ASSERT_TRUE(flush1_res.has_value()) << "Flush 1 failed: " << flush1_res.error().to_string();
            ssts1 = std::move(flush1_res.value());
        }

        // 3. Group all SSTs by partition
        std::map<uint16_t, std::vector<hedge::db::sst>> ssts_by_partition;
        for(auto& sst : flush0_res.value())
            ssts_by_partition[sst.upper_bound()].push_back(std::move(sst));
        for(auto& sst : ssts1)
            ssts_by_partition[sst.upper_bound()].push_back(std::move(sst));

        // 4. Merge per partition with discard_deleted_keys = true
        std::vector<hedge::db::sst> merged_ssts;
        std::mutex merged_mutex;

        auto merge_job = [this, &merged_ssts, &merged_mutex](
                             std::vector<const hedge::db::sst*> inputs,
                             uint16_t p_prefix) -> tmc::task<void>
        {
            hedge::db::index_ops::merge_config config{
                .read_ahead_size = READ_AHEAD_SIZE,
                .new_index_id = 2,
                .base_path = _base_path,
                .discard_deleted_keys = true,
                .create_new_with_odirect = false,
                .populate_cache_with_output = false};

            auto res = co_await hedge::db::index_ops::k_way_merge_async2(config, inputs, nullptr);

            if(!res.has_value())
                std::cerr << "Merge failed for prefix " << p_prefix << ": " << res.error().to_string() << "\n";
            else
            {
                std::lock_guard lock(merged_mutex);
                merged_ssts.push_back(std::move(res.value()));
            }
        };

        {
            std::vector<tmc::task<void>> merge_tasks;
            for(auto& [prefix, sst_list] : ssts_by_partition)
            {
                std::vector<const hedge::db::sst*> inputs;
                for(const auto& sst : sst_list)
                    inputs.push_back(&sst);
                merge_tasks.push_back(merge_job(std::move(inputs), prefix));
            }
            tmc::post_bulk_waitable(*this->_executor, merge_tasks.begin(), merge_tasks.size()).get();
        }

        // 5. Verify: live keys present with correct value, deleted keys absent
        std::map<uint16_t, const hedge::db::sst*> merged_sst_map;
        for(const auto& sst : merged_ssts)
            merged_sst_map[sst.upper_bound()] = &sst;

        size_t partition_prefix_range = (1 << 16) / (1 << NUM_PARTITION_EXPONENT);

        std::atomic<size_t> live_found{0};
        std::atomic<size_t> deleted_absent{0};

        auto verify_func = [&](size_t key_idx) -> tmc::task<void>
        {
            const auto& key = all_keys[key_idx];
            size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_prefix_range);
            auto it = merged_sst_map.find(partition_id);

            if(is_deleted[key_idx])
            {
                bool absent = (it == merged_sst_map.end());
                if(!absent)
                {
                    auto res = co_await it->second->lookup_async(key, nullptr);
                    absent = !res.has_value();
                }
                if(absent)
                    deleted_absent.fetch_add(1);
                else
                    std::cerr << "GC failed: tombstone not removed for key " << to_hex_string(key) << "\n";
            }
            else
            {
                if(it != merged_sst_map.end())
                {
                    auto res = co_await it->second->lookup_async(key, nullptr);
                    if(!res.has_value())
                        std::cerr << "Lookup error for live key " << to_hex_string(key) << ": " << res.error().to_string() << "\n";
                    else if(check_returned_payload(res.value(), key_idx))
                        live_found.fetch_add(1);
                    else
                        std::cerr << "Value mismatch for live key " << to_hex_string(key) << "\n";
                }
                else
                {
                    std::cerr << "Partition not found for live key " << to_hex_string(key) << "\n";
                }
            }
        };

        {
            std::vector<tmc::task<void>> verify_tasks;
            verify_tasks.reserve(N_KEYS_PER_RUN);
            for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
                verify_tasks.push_back(verify_func(i));
            tmc::post_bulk_waitable(*this->_executor, verify_tasks.begin(), verify_tasks.size()).get();
        }

        ASSERT_EQ(live_found.load(), N_KEYS_PER_RUN - total_deleted);
        ASSERT_EQ(deleted_absent.load(), total_deleted);
    }
};

TEST_P(gc_merge_test_suite, test_gc_fixed_keys)
{
    run_gc_test(false);
}

TEST_P(gc_merge_test_suite, test_gc_variable_keys)
{
    run_gc_test(true);
}

INSTANTIATE_TEST_SUITE_P(
    GcMergeTests,
    gc_merge_test_suite,
    testing::Combine(
        testing::Values(10'000, 100'000),                                                // N_KEYS_PER_RUN
        testing::Values(0, 1, 2, 4),                                                     // NUM_PARTITION_EXPONENT
        testing::Values(65536),                                                          // READ_AHEAD_SIZE
        testing::Values(0.01, 0.50, 1.00),                                               // deletion_fraction
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE) // VALUE_TYPE
        ),
    [](const testing::TestParamInfo<gc_merge_test_suite::ParamType>& info)
    {
        auto keys = std::get<0>(info.param);
        auto part = std::get<1>(info.param);
        auto ra = std::get<2>(info.param);
        auto frac = std::get<3>(info.param);
        auto value_type = std::get<4>(info.param);
        std::stringstream ss;
        ss << "Keys" << keys << "_PartExp" << part << "_RA" << ra;
        ss << "_Del" << static_cast<int>(frac * 100) << "pct";
        ss << (value_type == hedge::value_type::VALUE_PTR ? "_V_PTR" : "_V_INPLACE");
        return ss.str();
    });
