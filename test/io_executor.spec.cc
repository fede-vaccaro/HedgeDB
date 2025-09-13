#include <fcntl.h>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <iostream>

#include "../io_executor.h"
#include "../task.h"

struct test_executor : public ::testing::Test
{
    void SetUp() override
    {
        constexpr uint32_t QUEUE_DEPTH = 32;

        this->_executor = std::make_unique<executor_context>(QUEUE_DEPTH);
    }

    void TearDown() override
    {
        this->_executor->shutdown();
    }

    std::unique_ptr<executor_context> _executor{};
};

TEST_F(test_executor, test_read_simple)
{
    // prepare file with some data
    std::ofstream file("/tmp/test_file", std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);
    file << "Hello, World!       ";
    file.close();

    auto fd = open("/tmp/test_file", O_RDONLY);
    ASSERT_GE(fd, 0) << "Failed to open file: " << strerror(errno);

    for(int i = 0; i < 256; ++i)
    {
        auto promise = std::promise<read_response>{};
        auto future = promise.get_future();

        auto promise_2 = std::promise<read_response>{};
        auto future_2 = promise_2.get_future();

        auto task = [&]() -> ::task<void>
        {
            auto response = co_await this->_executor->submit_request(read_request{fd, 0, 13});
            auto response_2 = co_await this->_executor->submit_request(read_request{fd, 0, 13});

            promise.set_value(std::move(response));
            promise_2.set_value(std::move(response_2));
        };

        this->_executor->submit_io_task(task());

        auto response = future.get();
        auto response_2 = future_2.get();

        ASSERT_EQ(response.error_code, 0) << "Read error: " << strerror(-response.error_code);
        ASSERT_EQ(response.bytes_read, 13) << "Read response size mismatch";
        ASSERT_EQ(std::string(response.data.get(), response.data.get() + response.bytes_read), "Hello, World!") << "Read content mismatch";

        ASSERT_EQ(response_2.error_code, 0) << "Read error: " << strerror(-response_2.error_code);
        ASSERT_EQ(response_2.bytes_read, 13) << "Read response size mismatch";
        ASSERT_EQ(std::string(response_2.data.get(), response_2.data.get() + response_2.bytes_read), "Hello, World!") << "Read content mismatch";
    }

    close(fd);
}

TEST_F(test_executor, test_write_simple)
{
    auto fd = open("/tmp/test_file", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_NE(fd, -1) << "Failed to open file: " << strerror(errno);

    std::string input_data = "Hello, World!";
    std::vector<uint8_t> buffer(input_data.begin(), input_data.end());

    auto fallocate_res = fallocate(fd, 0, 0, buffer.size());
    ASSERT_EQ(fallocate_res, 0) << "Failed to preallocate file space: " << strerror(errno);

    auto promise = std::promise<void>{};
    auto future = promise.get_future();

    auto task = [&]() -> ::task<void>
    {
        auto request = write_request{fd, std::move(buffer), 0};

        auto response = co_await this->_executor->submit_request(std::move(request));

        promise.set_value();
    };

    this->_executor->submit_io_task(task());

    future.wait();

    // read the file with ifstream and check the content
    std::ifstream file("/tmp/test_file", std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading: " << strerror(errno);
    std::string file_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    ASSERT_EQ(file_content, input_data) << "File content does not match expected data";

    close(fd);
    std::cout << "Test completed successfully, file written and verified." << std::endl;
}

TEST_F(test_executor, test_multi_read)
{
    // prepare file with some data
    std::ofstream file("/tmp/test_file", std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);
    file << "Hello, World!";
    file.close();

    auto fd = open("/tmp/test_file", O_RDONLY);
    ASSERT_GE(fd, 0) << "Failed to open file: " << strerror(errno);

    auto promise = std::promise<multi_read_response>{};
    auto future = promise.get_future();

    auto task = [&]() -> ::task<void>
    {
        auto request = multi_read_request{
            .requests = {
                read_request{fd, 0, 5}, // Read first 5 bytes
                read_request{fd, 5, 8}  // Read next 8 bytes
            }};

        auto response = co_await this->_executor->submit_request(std::move(request));

        promise.set_value(std::move(response));
    };

    this->_executor->submit_io_task(task());

    auto response = future.get();

    ASSERT_EQ(response.responses.size(), 2) << "Expected 2 read responses";

    ASSERT_EQ(response.responses[0].bytes_read, 5) << "First read response size mismatch";
    ASSERT_EQ(std::string(response.responses[0].data.get(), response.responses[0].data.get() + response.responses[0].bytes_read), "Hello") << "First read content mismatch";

    ASSERT_EQ(response.responses[1].bytes_read, 8) << "Second read response size mismatch";
    ASSERT_EQ(std::string(response.responses[1].data.get(), response.responses[1].data.get() + response.responses[1].bytes_read), ", World!") << "Second read content mismatch";
}