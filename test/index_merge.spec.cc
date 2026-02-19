#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <map>
#include <random>
#include <ranges>
#include <sstream>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "cache.h"
#include "db/index_ops.h"
#include "db/memtable.h"
#include "db/sst.h"
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

// Parameters: <N_KEYS_PER_RUN, N_RUNS, NUM_PARTITION_EXPONENT, READ_AHEAD_SIZE, USE_CACHE>
class merge_test_suite : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t, size_t, bool>>
{
public:
    void SetUp() override
    {
        this->N_KEYS_PER_RUN = std::get<0>(GetParam());
        this->N_RUNS = std::get<1>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<2>(GetParam());
        this->READ_AHEAD_SIZE = std::get<3>(GetParam());
        this->USE_CACHE = std::get<4>(GetParam());

        // Skip if too few keys for partitions (avoid empty partitions effectively)
        if(this->NUM_PARTITION_EXPONENT > 0 && this->N_KEYS_PER_RUN < (1ULL << this->NUM_PARTITION_EXPONENT))
        {
            GTEST_SKIP() << "Skipping test with fewer keys than partitions";
        }

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        this->_executor = hedge::async::executor_context::make_new(128);

        if(this->USE_CACHE)
        {
            // TODO: there is a deadlock/livelock issue with prepopulating the cache during flush, if the cache is too small:
            // If (with shared_page_cache::get_write_slots_range) a requested range of pages is larger than than the number of available pages
            // the internal function page_cache::_find_frame() will spin forever because it is blocked from the reference counter not going to 0
            this->_cache = std::make_shared<hedge::db::sharded_page_cache>(1024 * 1024 * 1024, 1);
        }
    }

    void TearDown() override
    {
        if(this->_executor)
        {
            this->_executor->shutdown();
            this->_executor.reset();
        }
        this->_cache.reset();
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

    std::string _base_path = "/tmp/hh/test_merge_suite";
    std::shared_ptr<hedge::async::executor_context> _executor{};
    std::shared_ptr<hedge::db::sharded_page_cache> _cache{};

    size_t N_KEYS_PER_RUN;
    size_t N_RUNS;
    size_t NUM_PARTITION_EXPONENT;
    size_t READ_AHEAD_SIZE;
    bool USE_CACHE;

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{64, 128};

    void run_merge_test(bool variable_keys) // NOLINT(readability-function-cognitive-complexity)
    {
        std::cout << "Starting merge test with Params: "
                  << "Keys/Run=" << N_KEYS_PER_RUN
                  << ", Runs=" << N_RUNS
                  << ", PartExp=" << NUM_PARTITION_EXPONENT
                  << ", ReadAhead=" << READ_AHEAD_SIZE
                  << ", Cache=" << (USE_CACHE ? "Yes" : "No")
                  << ", VarKeys=" << (variable_keys ? "Yes" : "No") << std::endl;

        // 1. Generate Data and Flush SSTs
        std::vector<std::vector<hedge::key_t>> all_keys_per_run(N_RUNS);

        // Store all SSTs grouped by partition prefix
        // key: partition_prefix, value: vector of SSTs (one from each run that has this partition)
        std::map<uint16_t, std::vector<hedge::db::sst>> ssts_by_partition;

        for(size_t run_idx = 0; run_idx < N_RUNS; ++run_idx)
        {
            auto memtable = hedge::db::memtable_impl2_t{};
            auto& keys = all_keys_per_run[run_idx];
            keys.reserve(N_KEYS_PER_RUN);

            for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
            {
                auto key = variable_keys ? generate_key(len_dist(generator)) : generate_key(16);
                keys.push_back(key);
                // Value: run_idx * 1M + i (to be unique enough for verification)
                memtable.insert(key, {static_cast<uint64_t>((run_idx * 1000000) + i), 100, 0});
            }

            auto res = hedge::db::index_ops::flush_mem_index2(
                _base_path, &memtable, NUM_PARTITION_EXPONENT, run_idx, _cache, false);

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
        auto merge_wg = hedge::async::wait_group::make_shared();
        std::mutex merged_ssts_mutex;

        auto merge_job = [this, &merged_ssts, &merged_ssts_mutex](
                             std::vector<const hedge::db::sst*> inputs,
                             uint16_t p_prefix,
                             std::shared_ptr<hedge::async::wait_group> wg) -> hedge::async::task<void>
        {
            hedge::db::index_ops::merge_config config{
                .read_ahead_size = READ_AHEAD_SIZE,
                .new_index_id = N_RUNS + 1, // ID for merged file
                .base_path = _base_path,
                .discard_deleted_keys = false,
                .create_new_with_odirect = false,
                .populate_cache_with_output = USE_CACHE,
                .try_reading_from_cache = USE_CACHE};

            auto res = co_await hedge::db::index_ops::k_way_merge_async2(
                config, inputs, _executor, _cache);

            if(!res.has_value())
            {
                std::cerr << "Merge failed for prefix " << p_prefix << ": " << res.error().to_string() << "\n";
                // Failure will be caught by keys verification, or we could set an atomic flag
            }
            else
            {
                std::lock_guard<std::mutex> lock(merged_ssts_mutex);
                merged_ssts.push_back(std::move(res.value()));
            }
            wg->decr();
        };

        merge_wg->set(ssts_by_partition.size());

        for(auto& [prefix, sst_list] : ssts_by_partition)
        {
            if(sst_list.empty())
                continue;

            std::vector<const hedge::db::sst*> inputs;
            for(const auto& sst : sst_list)
                inputs.push_back(&sst);

            _executor->submit_io_task(merge_job(std::move(inputs), prefix, merge_wg));
        }

        merge_wg->wait();

        // 3. Verify merged SSTs contain all keys with correct values
        // Total keys should match
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

        // Lookup all keys
        auto query_wg = hedge::async::wait_group::make_shared();
        query_wg->set(total_keys_inserted);
        std::atomic<size_t> found_count{0};

        size_t partition_key_prefix_range = (1 << 16) / (1 << NUM_PARTITION_EXPONENT);

        auto lookup_func = [&](const hedge::key_t& key, uint64_t expected_val_offset) -> hedge::async::task<void>
        {
            size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
            auto it = merged_sst_map.find(partition_id);

            if(it != merged_sst_map.end())
            {
                auto res = co_await it->second->lookup_async(key, _cache);
                if(res.has_value() && res.value().has_value())
                {
                    if(res.value().value().offset() == expected_val_offset)
                        found_count.fetch_add(1);
                    else
                        std::cerr << "Value mismatch for key " << to_hex_string(key) << "\n";
                }

                if(res.has_error())
                {
                    std::cerr << "Lookup error for key " << to_hex_string(key) << ": " << res.error().to_string() << "\n";
                }
            }
            query_wg->decr();
        };

        for(size_t run_idx = 0; run_idx < N_RUNS; ++run_idx)
        {
            for(size_t i = 0; i < N_KEYS_PER_RUN; ++i)
            {
                const auto& key = all_keys_per_run[run_idx][i];
                uint64_t expected_val = (run_idx * 1000000) + i;
                _executor->submit_io_task(lookup_func(key, expected_val));
            }
        }

        query_wg->wait();
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
        testing::Values(10'000, 100'000, 1'000'000), // N_KEYS_PER_RUN
        testing::Values(2, 3, 4),                    // N_RUNS
        testing::Values(0, 1, 2, 4),                 // NUM_PARTITION_EXPONENT
        testing::Values(4096, 65536),                // READ_AHEAD_SIZE
        testing::Bool()                              // USE_CACHE
        ),
    [](const testing::TestParamInfo<merge_test_suite::ParamType>& info)
    {
        auto keys = std::get<0>(info.param);
        auto runs = std::get<1>(info.param);
        auto part = std::get<2>(info.param);
        auto ra = std::get<3>(info.param);
        auto cache = std::get<4>(info.param);
        std::stringstream ss;
        ss << "Keys" << keys << "_Runs" << runs << "_PartExp" << part << "_RA" << ra << "_Cache" << (cache ? "On" : "Off");
        return ss.str();
    });
