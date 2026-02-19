#include <gtest/gtest.h>
#include <numeric>
#include <string>

#include "page_aligned_buffer.h"
#include "utils.h"

namespace hedge
{

    TEST(page_aligned_buffer_test, default_constructor)
    {
        page_aligned_buffer<int> buf;
        EXPECT_EQ(buf.size(), 0);
        EXPECT_EQ(buf.capacity(), 0);
        EXPECT_TRUE(buf.empty());
        EXPECT_EQ(buf.data(), nullptr);
    }

    TEST(page_aligned_buffer_test, constructor_with_size)
    {
        constexpr size_t test_size = 10;
        page_aligned_buffer<int> buf(test_size);

        EXPECT_EQ(buf.size(), test_size);
        EXPECT_GE(buf.capacity(), test_size);
        EXPECT_FALSE(buf.empty());
        EXPECT_NE(buf.data(), nullptr);

        // Check page alignment
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);

        // Memory should be zero-initialized
        for(size_t i = 0; i < test_size; ++i)
        {
            EXPECT_EQ(buf[i], 0);
        }
    }

    TEST(page_aligned_buffer_test, constructor_with_size_and_capacity)
    {
        constexpr size_t test_size = 10;
        constexpr size_t test_capacity = 20;
        page_aligned_buffer<int> buf(test_size, test_capacity);

        EXPECT_EQ(buf.size(), test_size);
        EXPECT_GE(buf.capacity(), test_capacity);
        EXPECT_FALSE(buf.empty());
        EXPECT_NE(buf.data(), nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);

        for(size_t i = 0; i < test_size; ++i)
        {
            EXPECT_EQ(buf[i], 0);
        }
    }

    TEST(page_aligned_buffer_test, move_constructor)
    {
        constexpr size_t test_size = 128;
        page_aligned_buffer<int> buf1(test_size);
        std::iota(buf1.begin(), buf1.end(), 1);

        const int* old_data_ptr = buf1.data();
        const size_t old_size = buf1.size();
        const size_t old_capacity = buf1.capacity();

        page_aligned_buffer<int> buf2(std::move(buf1));

        // buf2 should have taken ownership
        EXPECT_EQ(buf2.data(), old_data_ptr);
        EXPECT_EQ(buf2.size(), old_size);
        EXPECT_EQ(buf2.capacity(), old_capacity);
        EXPECT_EQ(buf2[0], 1);
        EXPECT_EQ(buf2[test_size - 1], test_size);

        // buf1 should be in a valid, but moved-from state
        EXPECT_EQ(buf1.data(), nullptr);
        EXPECT_EQ(buf1.size(), 0);
        EXPECT_EQ(buf1.capacity(), 0);
    }

    TEST(page_aligned_buffer_test, move_assignment)
    {
        constexpr size_t test_size = 256;
        page_aligned_buffer<int> buf1(test_size);
        std::iota(buf1.begin(), buf1.end(), 42);

        const int* old_data_ptr = buf1.data();
        const size_t old_size = buf1.size();
        const size_t old_capacity = buf1.capacity();

        page_aligned_buffer<int> buf2(10); // Give buf2 some initial state
        buf2 = std::move(buf1);

        // buf2 should have taken ownership
        EXPECT_EQ(buf2.data(), old_data_ptr);
        EXPECT_EQ(buf2.size(), old_size);
        EXPECT_EQ(buf2.capacity(), old_capacity);
        EXPECT_EQ(buf2[0], 42);
        EXPECT_EQ(buf2[test_size - 1], 42 + test_size - 1);

        // buf1 should be in a valid, but moved-from state
        EXPECT_EQ(buf1.data(), nullptr);
        EXPECT_EQ(buf1.size(), 0);
        EXPECT_EQ(buf1.capacity(), 0);
    }

    TEST(page_aligned_buffer_test, data_access_and_iteration)
    {
        constexpr size_t test_size = 100;
        page_aligned_buffer<int> buf(test_size);

        for(size_t i = 0; i < test_size; ++i)
        {
            buf[i] = i;
        }

        // Check operator[]
        for(size_t i = 0; i < test_size; ++i)
        {
            EXPECT_EQ(buf[i], i);
        }

        // Check iterators
        int counter = 0;
        for(const auto& val : buf)
        {
            EXPECT_EQ(val, counter++);
        }
        EXPECT_EQ(counter, test_size);
    }

    TEST(page_aligned_buffer_test, resize_smaller)
    {
        constexpr size_t initial_size = 20;
        constexpr size_t new_size = 10;
        page_aligned_buffer<int> buf(initial_size);
        std::iota(buf.begin(), buf.end(), 1);

        buf.resize(new_size);

        EXPECT_EQ(buf.size(), new_size);
        // Capacity should remain the same
        EXPECT_GE(buf.capacity(), initial_size);

        for(size_t i = 0; i < new_size; ++i)
        {
            EXPECT_EQ(buf[i], i + 1);
        }
    }

    TEST(page_aligned_buffer_test, resize_larger)
    {
        constexpr size_t initial_size = 5;
        constexpr size_t new_size = 15;
        page_aligned_buffer<int> buf(initial_size);
        std::iota(buf.begin(), buf.end(), 1);
        const auto old_capacity = buf.capacity();

        buf.resize(new_size);

        EXPECT_EQ(buf.size(), new_size);
        EXPECT_GE(buf.capacity(), new_size);
        EXPECT_GE(buf.capacity(), old_capacity); // Capacity should grow
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);

        // Check that old data is preserved
        for(size_t i = 0; i < initial_size; ++i)
        {
            EXPECT_EQ(buf[i], i + 1);
        }

        // Check that new elements are zero-initialized
        for(size_t i = initial_size; i < new_size; ++i)
        {
            EXPECT_EQ(buf[i], 0);
        }
    }

    TEST(page_aligned_buffer_test, emplace_back_no_grow)
    {
        page_aligned_buffer<int> buf(0, 10);
        EXPECT_EQ(buf.size(), 0);

        buf.emplace_back(1);
        buf.emplace_back(2);
        buf.emplace_back(3);

        EXPECT_EQ(buf.size(), 3);
        EXPECT_EQ(buf[0], 1);
        EXPECT_EQ(buf[1], 2);
        EXPECT_EQ(buf[2], 3);
    }

    TEST(page_aligned_buffer_test, emplace_back_with_grow)
    {
        page_aligned_buffer<int> buf(0, 1);
        const auto actual_capacity = buf.capacity();

        for(int i = 0; i < (int)actual_capacity; ++i)
            buf.emplace_back(i);

        const int* old_data_ptr = buf.data();
        EXPECT_EQ(buf.size(), buf.capacity());

        buf.emplace_back((int)actual_capacity); // This should trigger a grow

        EXPECT_EQ(buf.size(), actual_capacity + 1);
        EXPECT_GT(buf.capacity(), actual_capacity);

        EXPECT_NE(buf.data(), old_data_ptr); // Buffer should have been reallocated
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);

        // Check inserted items
        int i = 0;
        for(const auto& val : buf)
            EXPECT_EQ(val, i++);
        EXPECT_EQ(buf.size(), actual_capacity + 1);
    }

    TEST(page_aligned_buffer_test, template_with_char)
    {
        constexpr size_t size = 10;
        page_aligned_buffer<char> buf(size);
        strcpy(buf.data(), "hello");

        EXPECT_EQ(buf.size(), size);
        EXPECT_STREQ(buf.data(), "hello");
    }

    struct non_pod_type
    {
        int x;
        std::string s;

        non_pod_type() = default;
        non_pod_type(int x_val, std::string s_val) : x(x_val), s(std::move(s_val)) {}
        non_pod_type(non_pod_type&&) = default;
        non_pod_type(const non_pod_type&) = default;
    };

    TEST(page_aligned_buffer_test, emplace_back_non_pod)
    {
        page_aligned_buffer<non_pod_type> buf(0, 2);

        buf.emplace_back(1, "one");
        buf.emplace_back(2, "two");

        EXPECT_EQ(buf.size(), 2);
        EXPECT_EQ(buf[0].x, 1);
        EXPECT_EQ(buf[0].s, "one");
        EXPECT_EQ(buf[1].x, 2);
        EXPECT_EQ(buf[1].s, "two");

        buf.emplace_back(3, "three"); // Trigger grow
        EXPECT_EQ(buf.size(), 3);
        EXPECT_EQ(buf[0].x, 1);
        EXPECT_EQ(buf[0].s, "one");
        EXPECT_EQ(buf[1].x, 2);
        EXPECT_EQ(buf[1].s, "two");
        EXPECT_EQ(buf[2].x, 3);
        EXPECT_EQ(buf[2].s, "three");
    }

    TEST(page_aligned_buffer_test, shrink_to_fit)
    {
        constexpr size_t element_size = sizeof(int);
        constexpr size_t elements_per_page = PAGE_SIZE_IN_BYTES / element_size;

        // Case 1: Capacity significantly larger than size
        {
            page_aligned_buffer<int> buf(10, elements_per_page * 2);
            EXPECT_EQ(buf.capacity(), elements_per_page * 2);

            buf.shrink_to_fit();
            
            EXPECT_EQ(buf.size(), 10);
            EXPECT_EQ(buf.capacity(), elements_per_page);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);
        }

        // Case 2: Capacity already aligned to page size minimum
        {
            // size=10, capacity=elements_per_page (1 page)
            // Shrinking shouldn't reduce capacity below 1 page because of alignment requirements
            page_aligned_buffer<int> buf(10, elements_per_page);
            EXPECT_EQ(buf.capacity(), elements_per_page);

            const auto old_capacity = buf.capacity();
            buf.shrink_to_fit();

            EXPECT_EQ(buf.size(), 10);
            EXPECT_EQ(buf.capacity(), old_capacity);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(buf.raw_data()) % PAGE_SIZE_IN_BYTES, 0);
        }
    }

} // namespace hedge
