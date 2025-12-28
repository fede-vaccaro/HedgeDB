#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "async/io_executor.h"
#include "async/task.h"

using namespace hedge::async;

struct test_executor : public ::testing::Test
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

constexpr size_t PAGE_SIZE = 4096;

std::unique_ptr<uint8_t> aligned_alloc(size_t size)
{
    uint8_t* ptr = nullptr;
    if(posix_memalign((void**)&ptr, PAGE_SIZE, size) != 0) // todo: preallocate some memory for 4 KB pages
    {
        perror("posix_memalign failed");
        throw std::runtime_error("Failed to allocate aligned memory for buffers");
    }

    return std::unique_ptr<uint8_t>(ptr);
}

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

        auto ptr_0 = aligned_alloc(13);
        auto ptr_1 = aligned_alloc(13);

        auto task = [&]() -> ::task<void>
        {
            auto response = co_await this->_executor->submit_request(read_request{fd, ptr_0.get(), 13, 0});
            auto response_2 = co_await this->_executor->submit_request(read_request{fd, ptr_1.get(), 13, 0});

            promise.set_value(std::move(response));
            promise_2.set_value(std::move(response_2));
        };

        this->_executor->submit_io_task(task());

        auto response = future.get();
        auto response_2 = future_2.get();

        ASSERT_EQ(response.error_code, 0) << "Read error: " << strerror(-response.error_code);
        ASSERT_EQ(response.bytes_read, 13) << "Read response size mismatch";
        ASSERT_EQ(std::string(reinterpret_cast<char*>(ptr_0.get()), reinterpret_cast<char*>(ptr_0.get()) + response.bytes_read), "Hello, World!") << "Read content mismatch";

        ASSERT_EQ(response_2.error_code, 0) << "Read error: " << strerror(-response_2.error_code);
        ASSERT_EQ(response_2.bytes_read, 13) << "Read response size mismatch";
        ASSERT_EQ(std::string(reinterpret_cast<char*>(ptr_1.get()), reinterpret_cast<char*>(ptr_1.get()) + response_2.bytes_read), "Hello, World!") << "Read content mismatch";
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
        auto request = write_request{fd, buffer.data(), buffer.size(), 0};

        [[maybe_unused]] auto response = co_await this->_executor->submit_request(std::move(request));

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

TEST_F(test_executor, test_open_fallocate_write)
{
    std::filesystem::remove("/tmp/test_file"); // to be sure

    auto fd = openat(AT_FDCWD, "/tmp/test_file", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << "Normal open() failed: " << strerror(errno);
    [[maybe_unused]] auto res = write(fd, "Hello, World!", 13);
    close(fd);
    SUCCEED();

    auto promise = std::promise<int32_t>{};
    auto future = promise.get_future();

    auto task = [&]() -> ::task<void>
    {
        auto request = open_request{"/tmp/test_file", O_WRONLY | O_CREAT | O_TRUNC, 0644};

        auto response = co_await this->_executor->submit_request(std::move(request));

        if(response.error_code < 0)
        {
            promise.set_value(response.error_code);
            co_return;
        }

        std::string input_data = "Hello, World!";

        auto fallocate_response = co_await this->_executor->submit_request(fallocate_request{response.file_descriptor, 0, 0, input_data.size()});

        if(fallocate_response.error_code < 0)
        {
            promise.set_value(fallocate_response.error_code);
            co_return;
        }

        uint8_t* input_data_ptr = reinterpret_cast<uint8_t*>(input_data.data());

        auto write_response = co_await this->_executor->submit_request(write_request{response.file_descriptor, input_data_ptr, input_data.size(), 0});

        if(write_response.error_code < 0)
        {
            promise.set_value(write_response.error_code);
            co_return;
        }

        promise.set_value(0);
    };

    this->_executor->submit_io_task(task());

    auto response = future.get();

    ASSERT_EQ(response, 0) << "Failed to open, fallocate or write to file: " << strerror(-response);

    std::ifstream file("/tmp/test_file", std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open file for reading: " << strerror(errno);
    std::string file_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    ASSERT_EQ(file_content, "Hello, World!") << "File content does not match expected data";
}

TEST_F(test_executor, text_file_info_file_does_not_exist)
{
    std::filesystem::remove("/tmp/test_file"); // to be sure

    auto promise = std::promise<file_info_response>{};
    auto future = promise.get_future();

    auto task = [&]() -> ::task<void>
    {
        auto request = file_info_request{"/tmp/test_file"};

        auto response = co_await this->_executor->submit_request(std::move(request));

        promise.set_value(std::move(response));
    };

    this->_executor->submit_io_task(task());

    auto response = future.get();

    ASSERT_FALSE(response.exists) << "File does not exist";
}
TEST_F(test_executor, test_file_info_file_exists)
{
    // prepare file with some data
    std::string test_data = "Hello, World!";
    std::ofstream file("/tmp/test_file", std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << strerror(errno);
    file << test_data;
    file.close();

    auto promise = std::promise<file_info_response>{};
    auto future = promise.get_future();

    auto task = [&]() -> ::task<void>
    {
        auto request = file_info_request{"/tmp/test_file"};

        auto response = co_await this->_executor->submit_request(std::move(request));

        promise.set_value(response);
    };

    this->_executor->submit_io_task(task());

    auto response = future.get();

    ASSERT_TRUE(response.exists) << "File should exist";
    ASSERT_EQ(response.file_size, test_data.size()) << "File size should be greater than 0";
}