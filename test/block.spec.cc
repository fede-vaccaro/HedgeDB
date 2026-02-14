#include <algorithm>
#include <cstddef>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "db/block.h"
#include "db/varint.h"

namespace hedge::db
{

    struct BlockTest
    {
        // Poor man's reflection
        [[nodiscard]] size_t get_bytes_written(const block_builder& builder) const
        {
            return static_cast<size_t>(builder._head - builder._base);
        }

        [[nodiscard]] uint32_t shared_prefix_length(std::span<const uint8_t> prev, std::span<const uint8_t> curr) const
        {
            return block_builder::_shared_prefix_length(prev, curr);
        }
    };

    struct BlockTestBasic : ::testing::Test, BlockTest
    {
    };

    std::span<const uint8_t> to_unsigned_span(std::string_view s)
    {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    bool span_cmpr(std::span<const uint8_t> a, std::string_view b)
    {
        if(a.size() != b.size())
            return false;

        return std::equal(a.begin(), a.end(), b.begin());
    }

    std::string generate_rand_value(size_t size)
    {
        static const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        static thread_local std::mt19937 rng(4242);
        static thread_local std::uniform_int_distribution<> dist(0, chars.size() - 1);

        std::string result;
        result.reserve(size);
        for(size_t i = 0; i < size; ++i)
            result += chars[dist(rng)];

        return result;
    }

    void check_restart_key(size_t& bytes_read, const std::vector<uint8_t>& buffer, const std::string& key, const std::string& value, std::string* out_key = nullptr)
    {
        // 1. Check key length
        ASSERT_EQ(buffer[bytes_read], key.size() - 1); // Encoded key size is actual size - 1
        bytes_read += 1;

        // 2. Check key
        std::string key_from_buffer(buffer.begin() + bytes_read, buffer.begin() + bytes_read + key.size());
        ASSERT_EQ(key_from_buffer, key);
        bytes_read += key.size();

        // 3. Check value length, decode varint
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value = try_decode_varint(std::span<const uint8_t>(buffer.data() + bytes_read, MAX_VARINT_LENGTH_32));
        ASSERT_TRUE(decoded_value);
        ASSERT_EQ(decoded_value.value().first, value.size());
        bytes_read += decoded_value.value().second;

        // 4. Check value
        std::string value_from_buffer(buffer.begin() + bytes_read, buffer.begin() + bytes_read + key.size());
        bytes_read += value.size();

        if(out_key != nullptr)
            *out_key = std::move(key_from_buffer);
    }

    void check_delta_key(size_t& bytes_read, size_t shared_prefix_length, const std::vector<uint8_t>& buffer, const std::string& key, const std::string& value, std::string* out_key = nullptr)
    {
        // 1. Check prefix length
        ASSERT_EQ(buffer[bytes_read], shared_prefix_length);
        bytes_read += 1;

        // 2. Check delta key length
        size_t delta_key_length = key.size() - shared_prefix_length;
        ASSERT_EQ(buffer[bytes_read], delta_key_length); // Delta key length
        bytes_read += 1;

        // 3. Check key suffix (delta key)
        std::string key_suffix_from_buffer(buffer.begin() + bytes_read, buffer.begin() + bytes_read + delta_key_length);
        ASSERT_EQ(key_suffix_from_buffer, key.substr(shared_prefix_length)); // Compare with suffix of key_1
        bytes_read += delta_key_length;

        // 4. Check value length, decode varint
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value = try_decode_varint(std::span<const uint8_t>(buffer.data() + bytes_read, MAX_VARINT_LENGTH_32));
        ASSERT_TRUE(decoded_value);
        ASSERT_EQ(decoded_value.value().first, value.size());
        bytes_read += decoded_value.value().second;

        // 5. Check value
        std::string value_from_buffer(buffer.begin() + bytes_read, buffer.begin() + bytes_read + value.size());
        bytes_read += value.size();

        if(out_key != nullptr)
            *out_key = key.substr(0, shared_prefix_length) + key_suffix_from_buffer;
    }

    TEST_F(BlockTestBasic, TestPushOne)
    {
        std::vector<uint8_t> buffer(4096);

        block_config cfg{
            .block_size_in_bytes = 4096,
            .restart_group_size = 16};

        block_builder builder(
            cfg,
            buffer.data());

        const std::string key = "afaf-afaf-afaf-afaf";      // 20 bytes
        const std::string value = generate_rand_value(300); // 300 bytes

        size_t bytes_read{0};

        // Test push one
        auto status = builder.push(to_unsigned_span(key), to_unsigned_span(value));
        ASSERT_TRUE(status);

        check_restart_key(bytes_read, buffer, key, value);

        // Test iterator
        block_iterator it(buffer.data(), buffer.size(), cfg.restart_group_size);

        ASSERT_TRUE(span_cmpr(it.key(), key)) << "Key mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key;
        ASSERT_TRUE(span_cmpr(it.value(), value)) << "Value mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value;
    }

    TEST_F(BlockTestBasic, TestPushTwo_DeltaKey)
    {
        std::vector<uint8_t> buffer(4096);

        block_config cfg{
            .block_size_in_bytes = 4096,
            .restart_group_size = 16};

        block_builder builder(
            cfg,
            buffer.data());

        const std::string key_0 = "afaf-afaf-afaf-afaf00000000000000000000000"; // 43 bytes
        const std::string key_1 = "afaf-ffff-ffff-ffff-ffff";                   // 25 bytes (first 5 bytes shared with key_0)
        const std::string value_0 = generate_rand_value(300);                   // 300 bytes
        const std::string value_1 = generate_rand_value(32);                    // 32 bytes

        // Test push one
        auto status = builder.push(to_unsigned_span(key_0), to_unsigned_span(value_0));
        ASSERT_TRUE(status);

        // Test push two (delta key)
        status = builder.push(to_unsigned_span(key_1), to_unsigned_span(value_1));
        ASSERT_TRUE(status);

        builder.finish();

        size_t bytes_read{0};

        // Readback
        check_restart_key(bytes_read, buffer, key_0, value_0);
        check_delta_key(bytes_read, 5, buffer, key_1, value_1);

        // Test reader
        block_reader reader(cfg, buffer.data());
        block_iterator it = reader.begin();

        ASSERT_TRUE(span_cmpr(it.key(), key_0)) << "Key 0 mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key_0;
        ASSERT_TRUE(span_cmpr(it.value(), value_0)) << "Value 0 mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value_0;

        ++it;

        ASSERT_TRUE(span_cmpr(it.key(), key_1)) << "Key 1 mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key_1;
        ASSERT_TRUE(span_cmpr(it.value(), value_1)) << "Value 1 mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value_1;

        ASSERT_TRUE(++it == reader.end());
    }

    TEST_F(BlockTestBasic, TestPushThree_TwoDeltaKey)
    {
        std::vector<uint8_t> buffer(4096);

        block_config cfg{
            .block_size_in_bytes = 4096,
            .restart_group_size = 16};

        block_builder builder(
            cfg,
            buffer.data());

        const std::string key_0 = "afaf-afaf-afaf-afaf";      // 20 bytes
        const std::string key_1 = "afaf-ffff-ffff-ffff-ffff"; // 25 bytes (first 5 bytes shared with key_0)
        const std::string key_2 = "afff-ffff-ffff-ffff-ffff"; // 25 bytes (first 2 bytes shared with key_1)
        const std::string value_0 = generate_rand_value(300); // 300 bytes
        const std::string value_1 = generate_rand_value(32);  // 32 bytes
        const std::string value_2 = generate_rand_value(128); // 128 bytes

        // Test push one
        auto status = builder.push(to_unsigned_span(key_0), to_unsigned_span(value_0));
        ASSERT_TRUE(status);

        // Test push two (delta key)
        status = builder.push(to_unsigned_span(key_1), to_unsigned_span(value_1));
        ASSERT_TRUE(status);

        // Test push three (delta key)
        status = builder.push(to_unsigned_span(key_2), to_unsigned_span(value_2));
        ASSERT_TRUE(status);

        // Readback
        size_t bytes_read{0};
        check_restart_key(bytes_read, buffer, key_0, value_0);
        check_delta_key(bytes_read, 5, buffer, key_1, value_1);
        check_delta_key(bytes_read, 2, buffer, key_2, value_2);

        // Test reader
        block_reader reader(cfg, buffer.data());

        block_iterator it = reader.begin();

        // Check key 0 and value 0
        ASSERT_TRUE(span_cmpr(it.key(), key_0)) << "Key 0 mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key_0;
        ASSERT_TRUE(span_cmpr(it.value(), value_0)) << "Value 0 mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value_0;
        ++it;

        // Check key 1 and value 1
        ASSERT_TRUE(span_cmpr(it.key(), key_1)) << "Key 1 mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key_1;
        ASSERT_TRUE(span_cmpr(it.value(), value_1)) << "Value 1 mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value_1;
        ++it;

        // Check key 2 and value 2
        ASSERT_TRUE(span_cmpr(it.key(), key_2)) << "Key 2 mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << key_2;
        ASSERT_TRUE(span_cmpr(it.value(), value_2)) << "Value 2 mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << value_2;
        ++it;

        ASSERT_TRUE(it == reader.end());
    }

    using numeric_range = std::pair<uint32_t, uint32_t>;

    struct data_distribution
    {
        numeric_range key_length;
        numeric_range shared_prefix_length;
        numeric_range value_length;

        using tuple_t = std::tuple<numeric_range, numeric_range, numeric_range>;

        static data_distribution from_tuple(tuple_t t)
        {
            return {
                .key_length = std::get<0>(t),
                .shared_prefix_length = std::get<1>(t),
                .value_length = std::get<2>(t)};
        }
    };

    struct BlockTestRandom : ::testing::TestWithParam<data_distribution::tuple_t>, BlockTest
    {
        inline static const std::string CHAR_SET = "0123456789abcdef"; // Hex
        inline static std::uniform_int_distribution<size_t> char_dist = std::uniform_int_distribution<size_t>(0, CHAR_SET.size() - 1);

        std::mt19937 rng{4242};

        data_distribution dist;

        std::uniform_int_distribution<size_t> key_length_dist;
        std::uniform_int_distribution<size_t> prefix_length_dist;
        std::uniform_int_distribution<size_t> value_length_dist;

        void SetUp() final
        {
            dist = data_distribution::from_tuple(GetParam());

            key_length_dist = std::uniform_int_distribution<size_t>(dist.key_length.first, dist.key_length.second);
            prefix_length_dist = std::uniform_int_distribution<size_t>(dist.shared_prefix_length.first, dist.shared_prefix_length.second);
            value_length_dist = std::uniform_int_distribution<size_t>(dist.value_length.first, dist.value_length.second);
        }

        [[nodiscard]] bool skip_invalid_range() const
        {
            return dist.key_length.first < dist.shared_prefix_length.second;
        }

        std::vector<std::string> generate_keys(size_t n)
        {
            // 1. generate N keys between 16 and 48 bytes
            std::vector<std::string> keys;

            for(auto i = 0UL; i < n; ++i)
            {
                size_t key_length = key_length_dist(rng);
                std::string key;
                for(auto j = 0UL; j < key_length; ++j)
                    key += CHAR_SET[char_dist(rng)];

                keys.emplace_back(key);
            }

            // 2. Sort the keys
            std::ranges::sort(keys);

            // 3. Fix the prefixes so that there is some actual sharing
            assert(prefix_length_dist.max() <= key_length_dist.min());

            for(auto i = 1UL; i < n; ++i)
            {
                size_t shared_prefix_length = prefix_length_dist(rng);

                for(auto j = 1UL; j < shared_prefix_length; ++j)
                    keys[i][j] = keys[i - 1][j];
            }

            // 2. Sort again in case the order was messed
            std::ranges::sort(keys);

            return keys;
        };

        std::vector<std::string> generate_values(size_t n)
        {
            std::vector<std::string> values;
            values.reserve(n);
            for(auto i = 0UL; i < n; ++i)
            {
                values.emplace_back(generate_rand_value(value_length_dist(rng)));
            }
            return values;
        }
    };

    TEST_P(BlockTestRandom, TestPushUntilBufferFull)
    {
        if(this->skip_invalid_range())
            GTEST_SKIP_("skipping due to invalid range");

        std::vector<uint8_t> buffer(4096);

        auto cfg = block_config{
            .block_size_in_bytes = 4096,
            .restart_group_size = 16};

        block_builder builder(
            cfg,
            buffer.data());

        constexpr size_t NUM_KEY_PAIRS = 1024; // They won't fit in one block

        std::vector<std::string> keys = generate_keys(NUM_KEY_PAIRS);
        std::vector<std::string> values = generate_values(NUM_KEY_PAIRS);

        // How many prefix bytes the key[i] shares with key[i-1]
        std::vector<size_t> shared_prefix_lengths(keys.size());

        for(auto i = 1UL; i < keys.size(); ++i)
            shared_prefix_lengths[i] = shared_prefix_length(to_unsigned_span(keys[i - 1]), to_unsigned_span(keys[i]));

        size_t inserted_keys{0};

        for(auto i = 0UL; i < keys.size(); ++i)
        {
            auto status = builder.push(to_unsigned_span(keys[i]), to_unsigned_span(values[i]));
            if(!status)
            {
                if(status.error().code() == errc::BUFFER_FULL)
                    break;

                FAIL() << "An error occurred while pushing data " + status.error().to_string();
            }
            ++inserted_keys;
        }

        std::cout << "Block capacity with input keys: " << inserted_keys << "\n";

        std::vector<std::string> readback_keys(inserted_keys);

        size_t bytes_read{0};
        // Readback
        for(auto i = 0UL; i < inserted_keys; ++i)
        {
            if(i % cfg.restart_group_size == 0)
                check_restart_key(bytes_read, buffer, keys[i], values[i]);
            else
                check_delta_key(bytes_read, shared_prefix_lengths[i], buffer, keys[i], values[i]);
        }

        // Test reader
        block_reader reader(cfg, buffer.data());
        size_t idx = 0;
        for(auto it = reader.begin(); it != reader.end(); ++it)
        {
            ASSERT_TRUE(span_cmpr(it.key(), keys[idx])) << "Key " << idx << " mismatch: " << std::string(it.key().begin(), it.key().end()) << " vs " << keys[idx];
            ASSERT_TRUE(span_cmpr(it.value(), values[idx])) << "Value " << idx << " mismatch: " << std::string(it.value().begin(), it.value().end()) << " vs " << values[idx];

            readback_keys[idx] = std::string(it.key().begin(), it.key().end());

            ++idx;
        }

        ASSERT_EQ(idx, inserted_keys) << "Number of keys read back does not match number of keys inserted";

        // Assert that keys are sorted
        ASSERT_TRUE(std::is_sorted(readback_keys.begin(), readback_keys.end())) << "Keys are not sorted in the block";

        // Try find keys with reader
        for(auto i = 0UL; i < keys.size(); ++i)
        {
            std::vector<uint8_t> found_value = reader.find(to_unsigned_span(keys[i]));

            if(i < inserted_keys)
                ASSERT_TRUE(span_cmpr(found_value, values[i])) << "Find value mismatch for key " << i << ": " << std::string(found_value.begin(), found_value.end()) << " vs " << values[i];
            else
                ASSERT_TRUE(found_value.empty()) << "Expected not to find key " << i << " but it was found with value: " << std::string(found_value.begin(), found_value.end());
        }
    }

    INSTANTIATE_TEST_SUITE_P(
        RandomTest,
        BlockTestRandom,
        testing::Combine(
            testing::Values(numeric_range{16, 32}, numeric_range{8, 64}),                          // Key Length range
            testing::Values(numeric_range{1, 6}, numeric_range{0, 16}),                            // Shared Prefix Length range
            testing::Values(numeric_range{16, 256}, numeric_range{8, 1024}, numeric_range{12, 16}) // Value Length range
            // testing::Values(numeric_range{16, 16}), // Key Length range
            // testing::Values(numeric_range{2, 2}),   // Shared Prefix Length range
            // testing::Values(numeric_range{16, 16})  // Value Length range
            ),
        [](const testing::TestParamInfo<BlockTestRandom::ParamType>& info)
        {
            data_distribution d = data_distribution::from_tuple(info.param);

            auto to_string = [](numeric_range r) -> std::string
            {
                return std::format("{}_{}", r.first, r.second);
            };

            return std::format(
                "KEY_{}__PREFIX_{}__VALUE_{}",
                to_string(d.key_length),
                to_string(d.shared_prefix_length),
                to_string(d.value_length));
        });

} // namespace hedge::db