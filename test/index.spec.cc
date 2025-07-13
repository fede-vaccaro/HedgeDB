#include "gtest/gtest.h"
#include <algorithm>
#include <random>
#include <ranges>

#include <gtest/gtest.h>

#include "../index.h"
#include "../io_executor.h"
#include "../paginated_view.h"

uint32_t uuid_fake_size(const uuids::uuid& uuid)
{
    const auto& uuids_as_std_array = reinterpret_cast<const std::array<uint8_t, 16>&>(uuid);
    return uuids_as_std_array[0] + uuids_as_std_array[1] % 125; // Just a fake size based on the first two bytes
};

struct test_configuration
{
    size_t number_of_keys_to_push;
    size_t num_partition_exponent;
    size_t num_runs;

    std::ostream& operator<<(std::ostream& o)
    {
        o << "number_of_keys_to_push: " << number_of_keys_to_push << "; num_partition_exponent: " << num_partition_exponent << "; num runs: " << num_runs << "\n";
        return o;
    }
};

struct sorted_string_merge_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, size_t>>
{
    void SetUp() override
    {
        this->N_KEYS_PER_RUN = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->N_RUNS = std::get<2>(GetParam());

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        for(size_t i = 0; i < N_RUNS; ++i)
        {
            auto memtable = hedgehog::db::mem_index{};
            for(size_t j = 0; j < N_KEYS_PER_RUN; ++j)
            {
                auto uuid = generate_uuid();
                this->_uuids.emplace_back(uuid);
                memtable.add(uuid, {static_cast<uint64_t>(j), uuid_fake_size(uuid), 0});
            }

            auto vec_memtable = std::vector<hedgehog::db::mem_index>{};
            vec_memtable.emplace_back(std::move(memtable));

            auto partitioned_sorted_indices = hedgehog::db::index_ops::merge_and_flush(this->_base_path, std::move(vec_memtable), NUM_PARTITION_EXPONENT);

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

        this->_executor = std::make_shared<hedgehog::async::executor_context>(32);
    }

    void TearDown() override
    {
        this->_executor->shutdown();
        this->_executor.reset();
    }

    uuids::uuid generate_uuid()
    {
        // static std::random_device rd;
        size_t seed = 107279581;
        static std::mt19937 generator(seed);
        static std::uniform_int_distribution<int> dist(0, 15);
        static uuids::uuid_random_generator gen{generator};

        return gen();
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
                auto key_prefix = hedgehog::extract_prefix(uuid);
                auto matching_partition = hedgehog::find_partition_prefix_for_key(uuid, (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT));
                return matching_partition == prefix;
            });

        return result;
    }

    size_t NUM_PARTITION_EXPONENT = 0; // 2^4 = 16 partitions
    size_t N_KEYS_PER_RUN = 1000;
    size_t N_RUNS = 2;

    std::vector<uuids::uuid> _uuids;
    std::map<uint16_t, std::vector<hedgehog::db::sorted_index>> _sorted_indices;
    std::string _base_path = "/tmp/hh/test";
    std::shared_ptr<hedgehog::async::executor_context> _executor{};

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<int> dist{0, 15};
    uuids::uuid_random_generator gen{generator};
};

TEST_P(sorted_string_merge_test, test_merge)
{
    for(auto& [prefix, sorted_indices] : this->_sorted_indices)
    {
        ASSERT_LE(sorted_indices.size(), this->N_RUNS) << "Expected no more than " << this->N_RUNS << " sorted index after merging";

        if(sorted_indices.size() < 2)
        {
            std::cout << "Skipping merge test. Set is too small." << std::endl;
            GTEST_SKIP();
        }

        size_t cumulative_size = 0;
        for(const auto& index : sorted_indices)
            cumulative_size += index.size();

        auto uuids = this->extract_uuids_up_to_prefix(prefix);

        ASSERT_EQ(uuids.size(), cumulative_size) << "Expected cumulative size of uuids to match the number of indexed keys";

        std::ranges::sort(
            sorted_indices,
            [](const hedgehog::db::sorted_index& a, const hedgehog::db::sorted_index& b)
            {
                return a.size() >= b.size();
            });

        auto maybe_new_index = hedgehog::db::index_ops::two_way_merge(
            this->_base_path,
            4096,
            sorted_indices[0],
            sorted_indices[1],
            this->_executor);

        ASSERT_TRUE(maybe_new_index) << "Expected successful merge of two sorted indices " << maybe_new_index.error().to_string();
        auto new_index = std::move(maybe_new_index.value());

        ASSERT_EQ(new_index.size(), cumulative_size) << "Expected new index size to match cumulative size of uuids";
        ASSERT_EQ(new_index.upper_bound(), prefix) << "Expected new index upper bound to match the prefix";

        for(const auto& uuid : uuids)
        {
            auto result = new_index.lookup(uuid);
            ASSERT_TRUE(result) << "Expected to find uuid " << uuid << " in the new index; Error: " << result.error().to_string();
            auto& value = result.value();
            ASSERT_TRUE(value.has_value()) << "Expected to find value for uuid " << uuid << " in the new index";
            ASSERT_EQ(value->size, uuid_fake_size(uuid));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    sorted_string_merge_test,
    testing::Combine(
        testing::Values(1000, 5000, 10000, 1000000), // n keys
        testing::Values(0, 1, 4, 10, 16),            // num partition exponent -> 1, 2, 16, 1024, 65536 partitions
        testing::Values(2)                           // Num runs
        ),
    [](const testing::TestParamInfo<sorted_string_merge_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto num_runs = std::get<2>(info.param);

        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + "_R_" + std::to_string(num_runs);
        return name;
    });