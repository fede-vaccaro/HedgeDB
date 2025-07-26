#include <fstream>
#include <gtest/gtest.h>
#include <numeric>

#include "../file_reader.h"
#include "../fs.hpp"
#include "../io_executor.h"
#include "../task.h"
#include "../common.h"

#include "error.hpp"

namespace hedgehog::async
{

    struct test_paginated_view : public ::testing::Test
    {
        void SetUp() override
        {
            constexpr uint32_t QUEUE_DEPTH = 32;

            this->_executor = std::make_shared<executor_context>(QUEUE_DEPTH);
        }

        void TearDown() override
        {
            this->_executor->shutdown();
        }

        std::shared_ptr<executor_context> _executor{};
    };

    TEST_F(test_paginated_view, test_one_page)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 64},
            this->_executor};

        auto task = view.next(64, false);

        auto maybe_vector = this->_executor->sync_submit(std::move(task));

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 64) << "Expected to read 64 bytes, but got " << maybe_vector.value().size() << " bytes";
        ASSERT_EQ(maybe_vector.value(), data);
    }

    TEST_F(test_paginated_view, test_multiple_pages)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 64},
            this->_executor};

        for(int i = 0; i < 4; i++)
        {
            auto task = view.next(16, false);

            auto maybe_vector = this->_executor->sync_submit(std::move(task));

            ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
            ASSERT_EQ(maybe_vector.value().size(), 16) << "Expected to read 64 bytes, but got " << maybe_vector.value().size() << " bytes";

            auto sub_span = std::span(data).subspan(i * 16, 16);
            auto sub_vector = std::vector<uint8_t>(sub_span.begin(), sub_span.end());

            ASSERT_EQ(maybe_vector.value(), sub_vector)
                << "Expected to read sequential bytes, but got different data at page " << i;
        }
    }

    TEST_F(test_paginated_view, test_clamp_at_end)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 64},
            this->_executor};

        auto task = view.next(1000, true); // Request more pages than available

        auto maybe_vector = this->_executor->sync_submit(std::move(task));

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 64) << "Expected to read 64 bytes, but got " << maybe_vector.value().size() << " bytes";
        ASSERT_EQ(maybe_vector.value(), data);
    }

    TEST_F(test_paginated_view, test_single_read_from_task)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 64},
            this->_executor};

        auto promise = std::promise<expected<std::vector<uint8_t>>>{};
        auto future = promise.get_future();

        auto task_lambda = [&, promise = std::move(promise)]() mutable -> task<void>
        {
            auto maybe_vector = co_await view.next(64, false);

            promise.set_value(std::move(maybe_vector));
        };

        this->_executor->submit_io_task(task_lambda());
        auto maybe_vector = future.get();

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 64) << "Expected to read 64 bytes, but got " << maybe_vector.value().size() << " bytes";
        ASSERT_EQ(maybe_vector.value(), data);
    }

    TEST_F(test_paginated_view, test_read_bytes_bug) // i'm reproducing here a bug i've found while developing... can't really give it a name to this config
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(36864); // up to 1152 elements, 9 pages

        struct key_like_t
        {
            size_t key;
            uint8_t _padding[24];
        };

        auto span = view_as<key_like_t>(data);
        for(size_t i = 0; i < 1028; i++)
            span[i].key = i;

        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 36864},
            this->_executor};

        for(int i = 0; i < 9; i++)
        {
            auto task = view.next(4096);

            auto maybe_vector = this->_executor->sync_submit(std::move(task));

            ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
            ASSERT_EQ(maybe_vector.value().size(), 4096) << "Expected to read 4096 bytes, but got " << maybe_vector.value().size() << " bytes";

            auto sub_span = std::span(data).subspan(i * 4096, 4096);
            auto sub_vector = std::vector<uint8_t>(sub_span.begin(), sub_span.end());

            ASSERT_EQ(maybe_vector.value(), sub_vector)
                << "Expected to read sequential bytes, but got different data at page " << i;

            if(i == 8)
                ASSERT_TRUE(view.is_eof()) << "Expected view to be at EOF after reading all pages";
        }

        for(int i = 0; i < 4; i++)
        {
            auto task = view.next(4096);

            auto maybe_vector = this->_executor->sync_submit(std::move(task));

            ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
            ASSERT_EQ(maybe_vector.value().size(), 0) << "Expected to read 0 bytes, but got " << maybe_vector.value().size() << " bytes";
            ASSERT_TRUE(view.is_eof()) << "Expected view to be at EOF after reading all pages";
        }
    }

    TEST_F(test_paginated_view, test_first_page_clamp)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 50},
            this->_executor};

        auto task = view.next(64, true);

        auto maybe_vector = this->_executor->sync_submit(std::move(task));

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 50) << "Expected to read 64 bytes, but got " << maybe_vector.value().size() << " bytes";

        auto sub_span = std::span(data).subspan(0, 50);
        auto sub_vector = std::vector(sub_span.begin(), sub_span.end());
        ASSERT_EQ(maybe_vector.value(), sub_vector);
    }

    TEST_F(test_paginated_view, test_last_page_clamp)
    {
        std::ofstream file("/tmp/test_file", std::ios::binary);

        ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);

        std::vector<uint8_t> data(64);
        std::iota(data.begin(), data.end(), 0); // Fill with sequential bytes
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();

        auto fd = fs::file_descriptor::from_path("/tmp/test_file", fs::file_descriptor::open_mode::read_only, false, 64);
        ASSERT_TRUE(fd.has_value()) << "Failed to open file: " << fd.error().to_string();

        auto view = file_reader{
            fd.value(),
            file_reader_config{.start_offset = 0, .end_offset = 50},
            this->_executor};

        // first page
        auto task = view.next(30, true);

        auto maybe_vector = this->_executor->sync_submit(std::move(task));

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 30) << "Expected to read 30 bytes, but got " << maybe_vector.value().size() << " bytes";

        auto sub_span = std::span(data).subspan(0, 30);
        auto sub_vector = std::vector(sub_span.begin(), sub_span.end());
        ASSERT_EQ(maybe_vector.value(), sub_vector);

        // second and last page clamped page
        task = view.next(30, true);

        maybe_vector = this->_executor->sync_submit(std::move(task));

        ASSERT_TRUE(maybe_vector.has_value()) << "Expected a valid vector, but got an error: " << maybe_vector.error().to_string();
        ASSERT_EQ(maybe_vector.value().size(), 20) << "Expected to read 20 bytes, but got " << maybe_vector.value().size() << " bytes";

        sub_span = std::span(data).subspan(30, 20);
        sub_vector = std::vector(sub_span.begin(), sub_span.end());
        ASSERT_EQ(maybe_vector.value(), sub_vector);
    }

} // namespace hedgehog::async
