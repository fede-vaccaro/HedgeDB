// THIS FILE WAS WRITTEN FROM AN LLM AND HUMAN INSPECTED

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "fs/file_reader2.h"
#include "fs/fs.hpp"
#include "io/io_executor.h"
#include "io/io_requests.hpp"
#include "perf_counter.h"
#include "tmc/sync.hpp"
#include "types.h"

namespace hedge::fs
{
    static const std::string TEST_DIR = "/tmp/hh_fr2";
    static const std::string TEST_FILE = TEST_DIR + "/file_reader2_test";

    class FileReader2Test : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            if(std::filesystem::exists(TEST_DIR))
            {
                std::filesystem::remove_all(TEST_DIR);
            }
            std::filesystem::create_directories(TEST_DIR);

            _executor = std::make_unique<hedge::io::io_executor>(
                hedge::io::executor_config{
                    .queue_depth = 32,
                    .n_threads = 1,
                    .auto_detect = false,
                });
        }

        void TearDown() override
        {
            _executor.reset();
        }

        std::unique_ptr<hedge::io::io_executor> _executor;

        void CreateTestFile(size_t size)
        {
            std::ofstream ofs(TEST_FILE, std::ios::binary);
            std::vector<std::byte> buffer(PAGE_SIZE_IN_BYTES);

            size_t written = 0;
            while(written < size)
            {
                size_t remaining = size - written;
                size_t chunk = std::min(PAGE_SIZE_IN_BYTES, remaining);

                for(size_t j = 0; j < PAGE_SIZE_IN_BYTES; ++j)
                {
                    buffer[j] = (j < chunk) ? static_cast<std::byte>((written + j) % 256) : std::byte{0};
                }

                ofs.write(reinterpret_cast<const char*>(buffer.data()), PAGE_SIZE_IN_BYTES);
                written += chunk;
                if(chunk < PAGE_SIZE_IN_BYTES)
                    written += (PAGE_SIZE_IN_BYTES - chunk);
            }
            ofs.close();
        }

        tmc::task<std::string> VerifyReaderSequence(
            hedge::fs::file& file_obj,
            size_t start_offset,
            size_t end_offset,
            size_t read_ahead,
            [[maybe_unused]] size_t logical_file_size)
        {
            file_reader2_config config{
                .start_offset = start_offset,
                .end_offset = end_offset,
                .read_ahead_size = read_ahead};

            file_reader2 reader(file_obj, config);
            size_t current_offset = start_offset;
            std::string error_msg;

            while(!reader.is_eof())
            {
                auto maybe_read = reader.next();

                if(!maybe_read.has_value())
                    break;

                auto& req = maybe_read.value();
                auto result = co_await std::move(req.awaitable);

                if(result < 0)
                {
                    if(error_msg.empty())
                        error_msg = "Read failed at " + std::to_string(current_offset) + ": " + strerror(-result);
                    continue;
                }

                std::span<const std::byte> data_span(req.buffer.data(), static_cast<size_t>(result));

                for(size_t i = 0; i < data_span.size(); ++i)
                {
                    size_t file_abs_offset = current_offset + i;

                    if(file_abs_offset >= end_offset)
                    {
                        // Padding verification
                        if(data_span[i] != std::byte{0})
                        {
                            error_msg = "Non-zero padding at " + std::to_string(file_abs_offset);
                            break;
                        }
                    }
                    else
                    {
                        std::byte expected = static_cast<std::byte>(file_abs_offset % 256);

                        if(data_span[i] != expected)
                        {
                            error_msg = "Data mismatch at " + std::to_string(file_abs_offset) +
                                        " Exp: " + std::to_string(static_cast<unsigned>(expected)) +
                                        " Got: " + std::to_string(static_cast<unsigned>(data_span[i]));
                            break;
                        }
                    }
                }

                if(!error_msg.empty())
                    co_return error_msg;

                if(current_offset < end_offset)
                {
                    size_t remaining = end_offset - current_offset;
                    current_offset += std::min(data_span.size(), remaining);
                }
            }

            if(current_offset != end_offset)
                co_return "Short read. Got " + std::to_string(current_offset) + " Exp " + std::to_string(end_offset);

            co_return "";
        }
    };

    TEST_F(FileReader2Test, SequenceNotCachedAtAll)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
            try {
                co_return co_await VerifyReaderSequence(file_obj, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SequenceFullyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
             try {
                co_return co_await VerifyReaderSequence(file_obj, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SequencePartiallyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
            try {
                co_return co_await VerifyReaderSequence(file_obj, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SecondLastCached_LastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
            try {
                co_return co_await VerifyReaderSequence(file_obj, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, LastCached_SecondLastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
            try {
                co_return co_await VerifyReaderSequence(file_obj, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, Coalescing_LastPage_UnalignedFile)
    {
        size_t extra = 100;
        size_t size = 3 * PAGE_SIZE_IN_BYTES + extra;

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();

        auto error = tmc::post_waitable(*_executor, [&]() -> tmc::task<std::string>
                                                                             {
            try {
                co_return co_await VerifyReaderSequence(file_obj, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }()).get();

        EXPECT_EQ(error, "");
    }
} // namespace hedge::fs
