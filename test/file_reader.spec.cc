// THIS FILE WAS WRITTEN FROM AN LLM AND HUMAN INSPECTED

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <variant>
#include <vector>
#include <string>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "cache.h"
#include "fs/file_reader.h"
#include "fs/fs.hpp"
#include "types.h"

namespace hedge::fs
{
    using namespace hedge::db;
    using namespace hedge::async;

    static const std::string TEST_DIR = "/tmp/hh";
    static const std::string TEST_FILE = TEST_DIR + "/file_reader_test";

    class FileReaderTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            if(std::filesystem::exists(TEST_DIR))
            {
                std::filesystem::remove_all(TEST_DIR);
            }
            std::filesystem::create_directories(TEST_DIR);

            try
            {
                executor_pool::init_static_pool(1, 32);
            }
            catch(...)
            {
            }
        }

        void CreateTestFile(size_t size)
        {
            std::ofstream ofs(TEST_FILE, std::ios::binary);
            std::vector<uint8_t> buffer(PAGE_SIZE_IN_BYTES);

            size_t written = 0;
            while(written < size)
            {
                size_t remaining = size - written;
                size_t chunk = std::min(PAGE_SIZE_IN_BYTES, remaining);

                for(size_t j = 0; j < PAGE_SIZE_IN_BYTES; ++j)
                {
                    buffer[j] = (j < chunk) ? static_cast<uint8_t>((written + j) % 256) : 0;
                }

                ofs.write(reinterpret_cast<const char*>(buffer.data()), PAGE_SIZE_IN_BYTES);
                written += chunk;
                if(chunk < PAGE_SIZE_IN_BYTES) written += (PAGE_SIZE_IN_BYTES - chunk);
            }
            ofs.close();
        }

        task<void> PopulateCache(std::shared_ptr<shared_page_cache> cache, int fd, size_t start_page_idx, const std::vector<uint8_t>& blueprint)
        {
            for(size_t i = 0; i < blueprint.size(); ++i)
            {
                if(blueprint[i])
                {
                    size_t page_idx = start_page_idx + i;
                    auto page_tag = hedge::db::to_page_tag(fd, page_idx * PAGE_SIZE_IN_BYTES);
                    auto page_guard = cache->get_write_slot(page_tag);
                    uint8_t* data_ptr = page_guard.data + page_guard.idx;

                    auto read_response = co_await async::this_thread_executor()->submit_request(async::read_request{
                        .fd = fd,
                        .data = data_ptr,
                        .offset = page_idx * PAGE_SIZE_IN_BYTES,
                        .size = PAGE_SIZE_IN_BYTES,
                    });

                    std::cout << "Populated cache for page " << page_idx << std::endl;

                    if(read_response.error_code != 0)
                        throw std::runtime_error("Failed to populate cache at page " + std::to_string(page_idx) + ": " + strerror(read_response.error_code));

                }
            }
            co_return;
        }

        task<std::string> VerifyReaderSequence(
            hedge::fs::file& file_obj,
            std::shared_ptr<shared_page_cache> cache,
            size_t start_offset,
            size_t end_offset,
            size_t read_ahead,
            size_t logical_file_size)
        {
            file_reader_config config{
                .start_offset = start_offset,
                .end_offset = end_offset,
                .read_ahead_size = read_ahead};

            file_reader reader(file_obj, config);
            size_t current_offset = start_offset;

            while(!reader.is_eof())
            {
                auto batch = reader.next(cache);
                std::cout << "Batch size: " << batch.size() << std::endl;

                if(batch.empty()) break;

                for(auto& item : batch)
                {
                    std::cout << "Processing item at offset " << current_offset << std::endl;

                    std::span<uint8_t> data_span;
                    if(std::holds_alternative<file_reader::awaitable_read_request_t>(item))
                    {
                        auto& req = std::get<file_reader::awaitable_read_request_t>(item);
                        auto result = co_await req.first;
                        if(result.error_code != 0)
                        {
                            co_return "Read failed at " + std::to_string(current_offset) + ": " + strerror(result.error_code);
                        }
                        data_span = req.second;
                        std::cout << "Span size (read request): " << data_span.size() << std::endl;
                    }
                    else
                    {
                        auto& pg_awaitable = std::get<file_reader::awaitable_page_guard_t>(item);
                        auto pg = co_await pg_awaitable.first;
                        data_span = pg_awaitable.second;
                        std::cout << "Span size (page guard): " << data_span.size() << std::endl;
                    }

                    for(size_t i = 0; i < data_span.size(); ++i)
                    {
                        size_t file_abs_offset = current_offset + i;
                        uint8_t expected = (file_abs_offset >= logical_file_size) ? 0 : static_cast<uint8_t>(file_abs_offset % 256);

                        if(data_span[i] != expected)
                        {
                            co_return "Data mismatch at " + std::to_string(file_abs_offset) + 
                                     " Exp: " + std::to_string(expected) + " Got: " + std::to_string(data_span[i]);
                        }
                    }
                    current_offset += data_span.size();
                }
            }

            if(current_offset != end_offset)
                co_return "Short read. Got " + std::to_string(current_offset) + " Exp " + std::to_string(end_offset);

            co_return "";
        }
    };

    TEST_F(FileReaderTest, SequenceNotCachedAtAll)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint(pages, 0);

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, SequenceFullyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint(pages, 1);

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, SequencePartiallyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint = {1, 0, 1, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, SecondLastCached_LastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;
        std::vector<uint8_t> blueprint = {0, 1, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, LastCached_SecondLastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;
        std::vector<uint8_t> blueprint = {0, 0, 1};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, Coalescing_LastPage_UnalignedFile)
    {
        size_t extra = 100;
        size_t size = 3 * PAGE_SIZE_IN_BYTES + extra;
        std::vector<uint8_t> blueprint = {1, 0, 0, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
        }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReaderTest, VerifyCoalescingBehavior)
    {
        size_t size = 4 * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint = {0, 1, 0, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<shared_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string> {
            co_await PopulateCache(cache, file_obj.id(), 0, blueprint);
            file_reader_config config{0, size, size};
            file_reader reader(file_obj, config);

            auto batch = reader.next(cache);
            if(batch.size() != 3) co_return "Expected batch size 3, got " + std::to_string(batch.size());
            if(!std::holds_alternative<file_reader::awaitable_read_request_t>(batch[0])) co_return "Item 0 type err";
            if(!std::holds_alternative<file_reader::awaitable_page_guard_t>(batch[1])) co_return "Item 1 type err";
            if(!std::holds_alternative<file_reader::awaitable_read_request_t>(batch[2])) co_return "Item 2 type err";

            for(auto& item : batch)
            {
                if(std::holds_alternative<file_reader::awaitable_read_request_t>(item)) co_await std::get<file_reader::awaitable_read_request_t>(item).first;
                else co_await std::get<file_reader::awaitable_page_guard_t>(item).first;
            }
            co_return "";
        }());

        EXPECT_EQ(error, "");
    }
} // namespace hedge::fs