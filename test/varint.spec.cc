#include <gtest/gtest.h>
#include <limits>
#include <random>
#include <span>
#include <vector>

// Check if varint.h is in db/
#include "db/varint.h"

// AI generated

namespace hedge::test
{

    class VarintTest : public ::testing::Test
    {
    protected:
        std::mt19937_64 rng{std::random_device{}()};
    };

    TEST_F(VarintTest, Constants)
    {
        EXPECT_EQ(MAX_VARINT_LENGTH_32, 5);
        EXPECT_EQ(MAX_VARINT_LENGTH_64, 10);
    }

    TEST_F(VarintTest, UnsafeVarintEncode32)
    {
        std::byte buffer[MAX_VARINT_LENGTH_32];

        // 0
        std::byte* end = unsafe_varint(0U, buffer);
        EXPECT_EQ(end - buffer, 1);
        EXPECT_EQ(buffer[0], 0);

        // 127 (last single byte value)
        end = unsafe_varint(127U, buffer);
        EXPECT_EQ(end - buffer, 1);
        EXPECT_EQ(buffer[0], 127);

        // 128 (first 2-byte value)
        end = unsafe_varint(128U, buffer);
        EXPECT_EQ(end - buffer, 2);
        // 1000 0000 -> 0000001 0000000
        // encode:
        // 1st byte: (128 | 0x80) = 0x80 | 0x80 = 0x80? No.
        // val = 128 (0x80). val >= 0x80 is true.
        // *ptr = (128 | 0x80) = 0x80 | 0x80 = 0x80. Correct.
        // val >>= 7 -> 1.
        // 1 < 0x80.
        // *ptr = 1.
        EXPECT_EQ(buffer[0], 0x80);
        EXPECT_EQ(buffer[1], 0x01);

        // Max 32-bit
        end = unsafe_varint(std::numeric_limits<uint32_t>::max(), buffer);
        EXPECT_EQ(end - buffer, 5);
    }

    TEST_F(VarintTest, UnsafeVarintEncode64)
    {
        std::byte buffer[MAX_VARINT_LENGTH_64];

        // Max 64-bit
        std::byte* end = unsafe_varint(std::numeric_limits<uint64_t>::max(), buffer);
        EXPECT_EQ(end - buffer, 10);
        for(int i = 0; i < 9; ++i)
        {
            EXPECT_EQ(buffer[i], 0xFF);
        }
        EXPECT_EQ(buffer[9], 0x01);
    }

    TEST_F(VarintTest, RoundTrip32)
    {
        std::vector<uint32_t> test_values = {
            0, 1, 127, 128, 129, 255, 256, 16383, 16384,
            std::numeric_limits<uint32_t>::max()};

        // Add some random values
        for(int i = 0; i < 100; ++i)
        {
            test_values.push_back(static_cast<uint32_t>(rng()));
        }

        std::byte buffer[MAX_VARINT_LENGTH_32];
        for(uint32_t val : test_values)
        {
            std::byte* end = unsafe_varint(val, buffer);
            std::span<std::byte> span(buffer, end - buffer);
            auto res = try_decode_varint(span);
            ASSERT_TRUE(res.has_value()) << "Failed to decode value: " << val;
            EXPECT_EQ(res.value().first, val);
        }
    }

    TEST_F(VarintTest, RoundTrip64)
    {
        std::vector<uint64_t> test_values = {
            0, 1, 127, 128, 129, 255, 256, 16383, 16384,
            std::numeric_limits<uint32_t>::max(),
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1,
            std::numeric_limits<uint64_t>::max()};

        // Add some random values
        for(int i = 0; i < 100; ++i)
        {
            test_values.push_back(rng());
        }

        std::byte buffer[MAX_VARINT_LENGTH_64];
        for(uint64_t val : test_values)
        {
            std::byte* end = unsafe_varint(val, buffer);
            std::span<std::byte> span(buffer, end - buffer);
            auto res = try_decode_varint(span);
            ASSERT_TRUE(res.has_value()) << "Failed to decode value: " << val;
            EXPECT_EQ(res.value().first, val);
        }
    }

    TEST_F(VarintTest, DecodeFastPathExplicit)
    {
        // Make a buffer larger than 10 bytes to trigger fast path
        std::byte buffer[20];

        // Encode 12345
        // 12345 = 0x3039
        // 0011 0000 0011 1001
        // 7-bit groups:
        // 0111001 (0x39) -> 57
        // 1100000 (0x60) -> 96
        // Encode: 0x39 | 0x80 = 0xB9, 0x60

        uint64_t val = 12345;
        std::byte* end = unsafe_varint(val, buffer);
        // Fill remainder with garbage
        std::fill(end, buffer + 20, 0xCC);

        // Pass large span
        std::span<std::byte> span(buffer, 20);
        auto res = try_decode_varint(span);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().first, val);
    }

    TEST_F(VarintTest, DecodeFailTooFewBytes)
    {
        // A single byte with MSB set, but no more bytes
        std::byte buffer[] = {0x80};
        std::span<std::byte> span(buffer, 1);
        auto res = try_decode_varint(span);
        EXPECT_FALSE(res.has_value());
        EXPECT_EQ(res.error().to_string(), "too few bytes");
    }

    TEST_F(VarintTest, DecodeFailTooManyBytes)
    {
        // 10 bytes all with MSB set (continuation)
        std::byte buffer[10];
        std::fill(std::begin(buffer), std::end(buffer), 0x80);

        std::span<std::byte> span(buffer, 10);
        // This hits the fast path because size >= 10
        auto res = try_decode_varint(span);
        EXPECT_FALSE(res.has_value());
        EXPECT_EQ(res.error().to_string(), "too many bytes");
    }

    TEST_F(VarintTest, DecodeFastPathLengths)
    {
        // Test all possible lengths (1 to 10 bytes) in fast path
        // We need a buffer >= 10 bytes to trigger fast path.
        std::byte buffer[20];

        // Values that require 1, 2, ..., 10 bytes
        // 1 byte: 1
        // 2 bytes: 1 << 7 (128)
        // 3 bytes: 1 << 14
        // ...
        // 10 bytes: 1 << 63

        for(int i = 0; i < 10; ++i)
        {
            uint64_t val = (i == 9) ? (1ULL << 63) : (1ULL << (7 * i));

            // Encode manually or using unsafe_varint?
            // Using unsafe_varint is fine as we trust it (tested above)
            std::byte* end = unsafe_varint(val, buffer);
            size_t encoded_len = end - buffer;
            EXPECT_EQ(encoded_len, i + 1) << "Length mismatch for i=" << i;

            // Fill rest with garbage
            std::fill(end, buffer + 20, 0xAA);

            std::span<std::byte> span(buffer, 20);
            auto res = try_decode_varint(span);
            ASSERT_TRUE(res.has_value()) << "Failed for length " << (i + 1);
            EXPECT_EQ(res.value().first, val);
        }
    }

} // namespace hedge::test
