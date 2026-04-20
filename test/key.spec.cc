#include <cstring>
#include <string>
#include <string_view>

#include "key.h"
#include <gtest/gtest.h>

// Disclaimer: this test was generetad by a coding agent
// Using namespace for convenience in test file
using namespace hedge;

// Test fixture for convenience
class key_test : public ::testing::Test
{
protected:
    void SetUp() override
    {
        short_str_val = "this is a short string";                                             // 22 chars, < 31
        long_str_val = "this is a much longer string that will not fit in the inline buffer"; // 75 chars, > 31
    }

    std::string short_str_val;
    std::string long_str_val;
};

TEST_F(key_test, default_constructor)
{
    key<> k;
    ASSERT_EQ(static_cast<std::string_view>(k).length(), 0);
    ASSERT_TRUE(static_cast<std::string_view>(k).empty());
}

TEST_F(key_test, constructor_from_string_view)
{
    // Short string
    key<> short_k(short_str_val);
    ASSERT_EQ(static_cast<std::string_view>(short_k), short_str_val);
    ASSERT_EQ(short_k.size(), short_str_val.length());

    // Long string
    key<> long_k(long_str_val);
    ASSERT_EQ(static_cast<std::string_view>(long_k), long_str_val);
    ASSERT_EQ(long_k.size(), long_str_val.length());
}

TEST_F(key_test, constructor_from_ptr_and_len)
{
    // Short string
    key<> short_k(short_str_val.data(), short_str_val.length());
    ASSERT_EQ(static_cast<std::string_view>(short_k), short_str_val);

    // Long string
    key<> long_k(long_str_val.data(), long_str_val.length());
    ASSERT_EQ(static_cast<std::string_view>(long_k), long_str_val);
}

TEST_F(key_test, copy_constructor)
{
    // Short string
    key<> short_k1(short_str_val);
    key<> short_k2(short_k1);
    ASSERT_EQ(static_cast<std::string_view>(short_k1), short_str_val);
    ASSERT_EQ(static_cast<std::string_view>(short_k2), short_str_val);

    // Long string
    key<> long_k1(long_str_val);
    key<> long_k2(long_k1);
    ASSERT_EQ(static_cast<std::string_view>(long_k1), long_str_val);
    ASSERT_EQ(static_cast<std::string_view>(long_k2), long_str_val);
    // For long strings, data pointers should be different (deep copy)
    ASSERT_NE(long_k1.data(), long_k2.data());
}

TEST_F(key_test, move_constructor)
{
    // Short string
    key<> short_k1(short_str_val);
    key<> short_k2(std::move(short_k1));
    ASSERT_EQ(static_cast<std::string_view>(short_k2), short_str_val);

    // Long string
    key<> long_k1(long_str_val);
    const std::byte* original_ptr = long_k1.data();
    key<> long_k2(std::move(long_k1));
    ASSERT_EQ(static_cast<std::string_view>(long_k2), long_str_val);
    ASSERT_EQ(long_k2.data(), original_ptr);
    ASSERT_EQ(static_cast<std::string_view>(long_k1).length(), 0); // Moved-from state
}

TEST_F(key_test, copy_assignment)
{
    key<> k_short1(short_str_val);
    key<> k_long1(long_str_val);

    // Case 1: short = short
    key<> k_dest_s_s;
    k_dest_s_s = k_short1;
    ASSERT_EQ(static_cast<std::string_view>(k_dest_s_s), short_str_val);

    // Case 2: long = long
    key<> k_dest_l_l;
    k_dest_l_l = k_long1;
    ASSERT_EQ(static_cast<std::string_view>(k_dest_l_l), long_str_val);
    ASSERT_NE(k_dest_l_l.data(), k_long1.data());

    // Case 3: short = long
    key<> k_dest_s_l(short_str_val);
    k_dest_s_l = k_long1;
    ASSERT_EQ(static_cast<std::string_view>(k_dest_s_l), long_str_val);

    // Case 4: long = short
    key<> k_dest_l_s(long_str_val);
    k_dest_l_s = k_short1;
    ASSERT_EQ(static_cast<std::string_view>(k_dest_l_s), short_str_val);
}

TEST_F(key_test, move_assignment)
{
    // Case 1: short = move(short)
    key<> k_dest_s_s;
    key<> k_src_s_s(short_str_val);
    k_dest_s_s = std::move(k_src_s_s);
    ASSERT_EQ(static_cast<std::string_view>(k_dest_s_s), short_str_val);

    // Case 2: long = move(long)
    key<> k_dest_l_l;
    key<> k_src_l_l(long_str_val);
    const std::byte* original_ptr_ll = k_src_l_l.data();
    k_dest_l_l = std::move(k_src_l_l);
    ASSERT_EQ(static_cast<std::string_view>(k_dest_l_l), long_str_val);
    ASSERT_EQ(k_dest_l_l.data(), original_ptr_ll);
    ASSERT_EQ(static_cast<std::string_view>(k_src_l_l).length(), 0);

    // Case 3: short = move(long)
    key<> k_dest_s_l(short_str_val);
    key<> k_src_s_l(long_str_val);
    const std::byte* original_ptr_sl = k_src_s_l.data();
    k_dest_s_l = std::move(k_src_s_l);
    ASSERT_EQ(static_cast<std::string_view>(k_dest_s_l), long_str_val);
    ASSERT_EQ(k_dest_s_l.data(), original_ptr_sl);
    ASSERT_EQ(static_cast<std::string_view>(k_src_s_l).length(), 0);

    // Case 4: long = move(short)
    key<> k_dest_l_s(long_str_val);
    key<> k_src_l_s(short_str_val);
    k_dest_l_s = std::move(k_src_l_s);
    ASSERT_EQ(static_cast<std::string_view>(k_dest_l_s), short_str_val);
}

TEST_F(key_test, comparison_operators)
{
    key<> short1("abc");
    key<> short2("abd");
    key<> short3("abc");

    key<> long1("this is a long string for testing comparison");
    key<> long2("this is a long string for testing comparison!");
    key<> long3("this is a long string for testing comparison");

    // short vs short
    ASSERT_TRUE(short1 == short3);
    ASSERT_FALSE(short1 == short2);
    ASSERT_TRUE(short1 != short2);
    ASSERT_FALSE(short1 != short3);
    ASSERT_TRUE(short1 < short2);
    ASSERT_FALSE(short2 < short1);

    // long vs long
    ASSERT_TRUE(long1 == long3);
    ASSERT_FALSE(long1 == long2);
    ASSERT_TRUE(long1 != long2);
    ASSERT_FALSE(long1 != long3);
    ASSERT_TRUE(long1 < long2);
    ASSERT_FALSE(long2 < long1);

    // short vs long
    key<> long_v(std::string(80, 'a'));
    key<> short_v("b");

    ASSERT_FALSE(short_v == long_v);
    ASSERT_TRUE(short_v != long_v);
    ASSERT_FALSE(short_v < long_v);
    ASSERT_TRUE(long_v < short_v);
}

TEST_F(key_test, data_and_size)
{
    // short
    key<> short_k(short_str_val);
    ASSERT_EQ(short_k.size(), short_str_val.length());
    ASSERT_EQ(0, std::memcmp(short_k.data(), short_str_val.data(), short_k.size()));

    const key<> const_short_k(short_str_val);
    ASSERT_EQ(static_cast<std::string_view>(const_short_k).length(), short_str_val.length());
    ASSERT_EQ(0, std::memcmp(const_short_k.data(), short_str_val.data(), static_cast<std::string_view>(const_short_k).length()));

    // long
    key<> long_k(long_str_val);
    ASSERT_EQ(long_k.size(), long_str_val.length());
    ASSERT_EQ(0, std::memcmp(long_k.data(), long_str_val.data(), long_k.size()));

    const key<> const_long_k(long_str_val);
    ASSERT_EQ(static_cast<std::string_view>(const_long_k).length(), long_str_val.length());
    ASSERT_EQ(0, std::memcmp(const_long_k.data(), long_str_val.data(), static_cast<std::string_view>(const_long_k).length()));
}

TEST_F(key_test, string_view_conversion)
{
    key<> short_k(short_str_val);
    std::string_view sv_short = short_k;
    ASSERT_EQ(sv_short, short_str_val);

    key<> long_k(long_str_val);
    std::string_view sv_long = long_k;
    ASSERT_EQ(sv_long, long_str_val);
}

TEST_F(key_test, different_sizes)
{
    for(size_t i = 0; i <= 64; ++i)
    {
        std::string s(i, 'a' + (i % 26));
        key k(s);
        ASSERT_EQ(static_cast<std::string_view>(k), s);
        ASSERT_EQ(k.size(), i);
    }
}
