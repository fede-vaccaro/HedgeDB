#include <algorithm>
#include <chrono>
#include <random>
#include <ranges>
#include <unordered_set>

#include <gtest/gtest.h>
#include <uuid.h>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "db/index_ops.h"
#include "db/mem_index.h"
#include "db/sorted_index.h"
#include "utils.h"

uint32_t uuid_fake_size(const uuids::uuid& uuid)
{
    const auto& uuids_as_std_array = reinterpret_cast<const std::array<uint8_t, 16>&>(uuid);
    return uuids_as_std_array[0] + (uuids_as_std_array[1] % 125); // Just a fake size based on the first two bytes
};

struct sorted_string_merge_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
{
    void SetUp() override
    {
        this->N_KEYS_PER_RUN = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->READ_AHEAD_SIZE_BYTES = std::get<2>(GetParam());

        if(this->NUM_PARTITION_EXPONENT <= 4 && this->N_KEYS_PER_RUN > 1'000'000)
        {
            this->READ_AHEAD_SIZE_BYTES *= 100; // otherwise the test will be too slow
            std::cout << "This test needs larger read ahead size: " << this->READ_AHEAD_SIZE_BYTES << " bytes\n";
        }

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        std::uniform_real_distribution<double> dist{0, 1.0};

        for(size_t i = 0; i < N_RUNS; ++i)
        {
            auto n_keys = this->N_KEYS_PER_RUN;
            if(i == 1)
                n_keys = std::min(n_keys, 20000000UL); // for the second run, limit to 20000000 keys

            auto memtable = hedge::db::mem_index{};

            memtable.reserve(n_keys);

            for(size_t j = 0; j < n_keys; ++j)
            {
                auto uuid = generate_uuid();

                this->_uuids.emplace_back(uuid);
                auto value_ptr = hedge::value_ptr_t(static_cast<uint64_t>(j), uuid_fake_size(uuid), 0);
                memtable.put(uuid, value_ptr);

                if(dist(this->generator) < this->DELETE_PROBABILITY)
                    this->_deleted_items.insert({uuid, value_ptr});
            }

            auto vec_memtable = std::vector<hedge::db::mem_index>{};
            vec_memtable.emplace_back(std::move(memtable));

            auto partitioned_sorted_indices = hedge::db::index_ops::flush_mem_index(this->_base_path, std::move(vec_memtable), NUM_PARTITION_EXPONENT, i);

            if(!partitioned_sorted_indices)
            {
                std::cerr << "Failed to flush indices: " << partitioned_sorted_indices.error().to_string() << '\n';
                FAIL();
            }

            for(auto& index : partitioned_sorted_indices.value())
            {
                auto prefix = index.upper_bound();
                index.clear_index();
                this->_sorted_indices[prefix].emplace_back(std::move(index));
            }
        }

        // deleted items memtable
        if(!this->_deleted_items.empty() && !this->SECOND_TABLE_IS_DELETION_ONLY)
        {
            std::cout << "Deleted keys: " << this->_deleted_items.size() << '\n';

            auto deleted_memtable = hedge::db::mem_index{};
            size_t n_keys = this->SECOND_TABLE_IS_DELETION_ONLY ? this->_deleted_items.size() : this->N_KEYS_PER_RUN;
            deleted_memtable.reserve(n_keys);

            for(const auto& [key, value_ptr] : this->_deleted_items)
            {
                auto deleted_value_ptr = hedge::value_ptr_t::apply_delete(value_ptr);
                deleted_memtable.put(key, deleted_value_ptr);
            }

            assert(n_keys - this->_deleted_items.size() > 0);

            for(size_t j = 0; j < n_keys - this->_deleted_items.size(); ++j)
            {
                auto uuid = generate_uuid();

                this->_uuids.emplace_back(uuid);
                auto value_ptr = hedge::value_ptr_t(static_cast<uint64_t>(j), uuid_fake_size(uuid), 0);
                deleted_memtable.put(uuid, value_ptr);

                if(dist(this->generator) < this->DELETE_PROBABILITY)
                    this->_deleted_items.insert({uuid, value_ptr});
            }

            auto vec_memtable = std::vector<hedge::db::mem_index>{};
            vec_memtable.emplace_back(std::move(deleted_memtable));

            // Flush the deleted items memtable
            auto deleted_partitioned_sorted_indices = hedge::db::index_ops::flush_mem_index(this->_base_path, {std::move(vec_memtable)}, NUM_PARTITION_EXPONENT, N_RUNS);

            if(!deleted_partitioned_sorted_indices)
            {
                std::cerr << "Failed to flush deleted items indices: " << deleted_partitioned_sorted_indices.error().to_string() << '\n';
                FAIL();
            }

            for(auto& index : deleted_partitioned_sorted_indices.value())
            {
                auto prefix = index.upper_bound();
                index.clear_index();
                this->_sorted_indices[prefix].emplace_back(std::move(index));
            }
        }

        this->_executor = std::make_shared<hedge::async::executor_context>(128);
    }

    void TearDown() override
    {
        if(this->_executor)
        {
            this->_executor->shutdown();
            this->_executor.reset();
        }
    }

    uuids::uuid generate_uuid()
    {
        return this->gen();
    }

    std::vector<uuids::uuid> extract_uuids_up_to_prefix(uint16_t prefix)
    {
        std::vector<uuids::uuid> result;

        std::copy_if(
            this->_uuids.begin(),
            this->_uuids.end(),
            std::back_inserter(result),
            [prefix, this](const uuids::uuid& uuid)
            {
                auto matching_partition = hedge::find_partition_prefix_for_key(uuid, (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT));
                return matching_partition == prefix;
            });

        return result;
    }

    [[nodiscard]] uint16_t get_partition_prefix(const uuids::uuid& uuid) const
    {
        return hedge::find_partition_prefix_for_key(uuid, (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT));
    }

    size_t NUM_PARTITION_EXPONENT = 0;
    size_t N_KEYS_PER_RUN = 1000;
    size_t N_RUNS = 1;
    size_t READ_AHEAD_SIZE_BYTES = 4096;
    double DELETE_PROBABILITY = 0.02;
    bool SECOND_TABLE_IS_DELETION_ONLY = false;

    std::vector<uuids::uuid> _uuids;
    std::unordered_map<hedge::key_t, hedge::value_ptr_t> _deleted_items;
    std::map<uint16_t, std::vector<hedge::db::sorted_index>> _sorted_indices;
    std::string _base_path = "/tmp/hh/test";
    std::shared_ptr<hedge::async::executor_context> _executor{};

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<int> dist{0, 15};
    uuids::uuid_random_generator gen{generator};
};

TEST_P(sorted_string_merge_test, test_merge_unified_async)
{
    using sorted_indices_map_t = std::map<uint16_t, hedge::db::sorted_index>;

    sorted_indices_map_t unified_sorted_indices;

    std::vector<std::future<hedge::status>> futures;
    auto merge_wg = hedge::async::wait_group::make_shared();

    auto merge_task_factory =
        [](
            const hedge::db::sorted_index& left,
            const hedge::db::sorted_index& right,
            std::vector<std::future<hedge::status>>& futures,
            std::map<uint16_t, hedge::db::sorted_index>& index_map,
            std::shared_ptr<hedge::async::wait_group> wg,
            auto* _this) -> hedge::async::task<void>
    {
        auto promise = std::promise<hedge::status>{};
        futures.emplace_back(promise.get_future());

        auto merge_config = hedge::db::index_ops::merge_config{
            .read_ahead_size = _this->READ_AHEAD_SIZE_BYTES,
            .new_index_id = _this->N_RUNS + 1,
            .base_path = _this->_base_path,
            .discard_deleted_keys = true};

        auto new_index = co_await hedge::db::index_ops::two_way_merge_async(
            merge_config,
            left,
            right,
            _this->_executor);

        if(!new_index.has_value())
            promise.set_value(new_index.error());

        auto prefix = new_index.value().upper_bound();
        index_map.insert({prefix, std::move(new_index.value())});

        promise.set_value(hedge::ok());

        wg->decr();
    };

    std::chrono::microseconds total_duration{0};
    auto t0 = std::chrono::high_resolution_clock::now();

    for(auto& [prefix, sorted_indices] : this->_sorted_indices)
    {
        ASSERT_LE(sorted_indices.size(), this->N_RUNS + 1) << "Expected no more than " << this->N_RUNS + 1 << " sorted index after merging";

        if(sorted_indices.empty())
            continue;

        if(sorted_indices.size() == 1)
        {
            unified_sorted_indices.insert({prefix, std::move(sorted_indices[0])});
            continue;
        }

        std::ranges::sort(
            sorted_indices,
            [](const hedge::db::sorted_index& a, const hedge::db::sorted_index& b)
            {
                return a.size() >= b.size();
            });

        merge_wg->incr();

        this->_executor->submit_io_task(merge_task_factory(
            sorted_indices[0],
            sorted_indices[1],
            futures,
            unified_sorted_indices,
            merge_wg,
            this));
    }

    merge_wg->wait();

    for(auto& future : futures)
    {
        auto status = future.get();
        ASSERT_TRUE(status) << "Expected successful merge of two sorted indices; Error: " << status.error().to_string();
    }

    futures = std::vector<std::future<hedge::status>>{}; // clear some memory

    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    total_duration += duration;

    auto total_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(total_duration).count();
    std::cout << "Total duration for merging: " << total_duration_ms << " ms" << std::endl;
    std::cout << "Average duration per merge: " << (static_cast<double>(total_duration_ms) / this->_sorted_indices.size()) << " ms" << std::endl;

    auto query_wg = hedge::async::wait_group::make_shared();

    query_wg->set(this->_uuids.size());

    std::unordered_set<uuids::uuid> seen_uuids;

    auto lookup_task_factory = [&](
                                   const uuids::uuid& uuid,
                                   const hedge::db::sorted_index& index,
                                   const std::shared_ptr<hedge::async::executor_context>& executor,
                                   std::shared_ptr<hedge::async::wait_group> wg) -> hedge::async::task<void>
    {
        seen_uuids.insert(uuid);

        auto lookup = co_await index.lookup_async(uuid, executor, nullptr);

        if(!lookup.has_value())
            throw std::runtime_error("Failed to lookup uuid: Error: " + lookup.error().to_string());

        auto lookup_result = lookup.value();

        if(!lookup_result.has_value())
        {
            if(!this->_deleted_items.contains(uuid))
                throw std::runtime_error("Expected to find value for uuid in the second index. uuid: " + uuids::to_string(uuid));

            if(this->_deleted_items.contains(uuid))
                seen_uuids.erase(uuid);

            wg->decr();

            co_return;
        }

        auto& value = lookup_result.value();

        if(value.size() != uuid_fake_size(uuid))
            throw std::runtime_error("Unexpected value size for uuid");

        seen_uuids.erase(uuid);

        wg->decr();
    };

    t0 = std::chrono::high_resolution_clock::now();

    for(const auto& uuid : this->_uuids)
    {
        auto prefix = this->get_partition_prefix(uuid);

        auto it = unified_sorted_indices.find(prefix);
        assert(it != unified_sorted_indices.end() && "Expected to find sorted index for prefix");

        ASSERT_TRUE(it != unified_sorted_indices.end()) << "Expected to find sorted index for prefix " << prefix;

        this->_executor->submit_io_task(lookup_task_factory(uuid, it->second, this->_executor, query_wg));
    }

    query_wg->wait_for(std::chrono::milliseconds(1 * this->_uuids.size()));

    for(const auto& uuid : seen_uuids)
        std::cout << "UUID not seen: " << uuid << std::endl;

    ASSERT_TRUE(seen_uuids.empty()) << "Expected to have seen all uuids, but some are missing";

    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

    auto lookup_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    std::cout << "Total duration for lookups: " << lookup_duration_ms << " ms" << std::endl;
    std::cout << "Average duration per lookup: " << (static_cast<double>(lookup_duration_ms * 1000) / this->_uuids.size()) << " us" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    sorted_string_merge_test,
    testing::Combine(
        testing::Values(1000, 5000, 10'000, 1'000'000), // n keys
        testing::Values(0, 1, 4, 10, 16),               // num partition exponent -> 1, 2, 16, 1024, 65536 partitions
        testing::Values(4096, 8192, 16384)              // Read ahead size
        ),
    [](const testing::TestParamInfo<sorted_string_merge_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto num_runs = std::get<2>(info.param);

        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + "_R_" + std::to_string(num_runs);
        return name;
    });