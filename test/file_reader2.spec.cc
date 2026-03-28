// THIS FILE WAS WRITTEN FROM AN LLM AND HUMAN INSPECTED

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "async/io_executor.h"
#include "async/wait_group.h"
#include "cache.h"
#include "fs/file_reader2.h"
#include "fs/fs.hpp"
#include "perf_counter.h"
#include "types.h"

namespace hedge::fs
{
    using namespace hedge::db;
    using namespace hedge::async;

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
                if(chunk < PAGE_SIZE_IN_BYTES)
                    written += (PAGE_SIZE_IN_BYTES - chunk);
            }
            ofs.close();
        }

        task<void> PopulateCache(std::shared_ptr<sharded_page_cache> cache, hedge::fs::file& file, size_t start_page_idx, const std::vector<uint8_t>& blueprint)
        {
            for(size_t i = 0; i < blueprint.size(); ++i)
            {
                if(blueprint[i])
                {
                    size_t page_idx = start_page_idx + i;
                    auto page_tag = hedge::db::to_page_tag(file.id(), page_idx * PAGE_SIZE_IN_BYTES);
                    auto page_guard = cache->get_write_slot(page_tag);
                    uint8_t* data_ptr = page_guard.data + page_guard.idx;

                    auto read_response = co_await async::this_thread_executor()->submit_request(async::read_request{
                        .fd = file.fd(),
                        .data = data_ptr,
                        .off = page_idx * PAGE_SIZE_IN_BYTES,
                        .len = PAGE_SIZE_IN_BYTES,
                    });

                    if(read_response.error_code != 0)
                        throw std::runtime_error("Failed to populate cache at page " + std::to_string(page_idx) + ": " + strerror(-read_response.error_code));
                }
            }
            co_return;
        }

        task<std::string> VerifyReaderSequence(
            hedge::fs::file& file_obj,
            std::shared_ptr<sharded_page_cache> cache,
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
                auto batch = reader.next(cache);

                if(batch.empty())
                    break;

                for(auto& item : batch)
                {
                    if(!error_msg.empty())
                    {
                        // Drain remaining awaitables in this batch to ensure cleanup
                        if(std::holds_alternative<file_reader2<>::awaitable_read_request_t>(item))
                        {
                            auto& req = std::get<file_reader2<>::awaitable_read_request_t>(item);
                            co_await req.awaitable;
                        }
                        else
                        {
                            auto& pg_awaitable = std::get<file_reader2<>::awaitable_page_guard_t>(item);
                            co_await pg_awaitable;
                        }
                        continue;
                    }

                    std::span<const uint8_t> data_span;

                    if(std::holds_alternative<file_reader2<>::awaitable_read_request_t>(item))
                    {
                        auto& req = std::get<file_reader2<>::awaitable_read_request_t>(item);
                        auto result = co_await req.awaitable;
                        if(result.error_code != 0)
                        {
                            if(error_msg.empty())
                                error_msg = "Read failed at " + std::to_string(current_offset) + ": " + strerror(-result.error_code);
                        }
                        else
                        {
                            data_span = std::span<const uint8_t>(req.buffer.data(), result.bytes_read);
                        }
                    }
                    else
                    {
                        auto& pg_awaitable = std::get<file_reader2<>::awaitable_page_guard_t>(item);
                        auto pg = co_await pg_awaitable;

                        data_span = std::span<const uint8_t>(pg.begin(), PAGE_SIZE_IN_BYTES);
                    }

                    if(!error_msg.empty())
                        continue;

                    for(size_t i = 0; i < data_span.size(); ++i)
                    {
                        size_t file_abs_offset = current_offset + i;

                        if(file_abs_offset >= end_offset)
                        {
                            // Padding verification
                            if(data_span[i] != 0)
                            {
                                error_msg = "Non-zero padding at " + std::to_string(file_abs_offset);
                                break;
                            }
                        }
                        else
                        {
                            uint8_t expected = static_cast<uint8_t>(file_abs_offset % 256);

                            if(data_span[i] != expected)
                            {
                                error_msg = "Data mismatch at " + std::to_string(file_abs_offset) +
                                            " Exp: " + std::to_string(expected) + " Got: " + std::to_string(data_span[i]);
                                break;
                            }
                        }
                    }

                    if(current_offset < end_offset)
                    {
                        size_t remaining = end_offset - current_offset;
                        current_offset += std::min(data_span.size(), remaining);
                    }
                }

                if(!error_msg.empty())
                    co_return error_msg;
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
        std::vector<uint8_t> blueprint(pages, 0);

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SequenceFullyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint(pages, 1);

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
             try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SequencePartiallyCached)
    {
        size_t pages = 4;
        size_t size = pages * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint = {1, 0, 1, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, SecondLastCached_LastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;
        std::vector<uint8_t> blueprint = {0, 1, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, LastCached_SecondLastNot_Unaligned)
    {
        size_t extra = 500;
        size_t size = (2 * PAGE_SIZE_IN_BYTES) + extra;
        std::vector<uint8_t> blueprint = {0, 0, 1};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, PAGE_SIZE_IN_BYTES, size, 2 * PAGE_SIZE_IN_BYTES, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, Coalescing_LastPage_UnalignedFile)
    {
        size_t extra = 100;
        size_t size = 3 * PAGE_SIZE_IN_BYTES + extra;
        std::vector<uint8_t> blueprint = {1, 0, 0, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                co_return co_await VerifyReaderSequence(file_obj, cache, 0, size, size, size);
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }

    TEST_F(FileReader2Test, VerifyCoalescingBehavior)
    {
        size_t size = 4 * PAGE_SIZE_IN_BYTES;
        std::vector<uint8_t> blueprint = {0, 1, 0, 0};

        CreateTestFile(size);
        auto file_res = hedge::fs::file::from_path(TEST_FILE, fs::file::open_mode::read_only, true);
        ASSERT_TRUE(file_res.has_value());
        auto& file_obj = file_res.value();
        auto cache = std::make_shared<sharded_page_cache>(10 * PAGE_SIZE_IN_BYTES, 1);

        auto error = executor_pool::executor_from_static_pool()->sync_submit([&]() -> task<std::string>
                                                                             {
            try {
                co_await PopulateCache(cache, file_obj, 0, blueprint);
                file_reader2_config config{
                    .start_offset = 0,
                    .end_offset = size,
                    .read_ahead_size = size};
                
                file_reader2 reader(file_obj, config);

                auto batch = reader.next(cache);
                if(batch.size() != 3) co_return "Expected batch size 3, got " + std::to_string(batch.size());
                if(!std::holds_alternative<file_reader2<>::awaitable_read_request_t>(batch[0])) co_return "Item 0 type err";
                if(!std::holds_alternative<file_reader2<>::awaitable_page_guard_t>(batch[1])) co_return "Item 1 type err";
                if(!std::holds_alternative<file_reader2<>::awaitable_read_request_t>(batch[2])) co_return "Item 2 type err";

                for(auto& item : batch)
                {
                    if(std::holds_alternative<file_reader2<>::awaitable_read_request_t>(item)) 
                        co_await std::get<file_reader2<>::awaitable_read_request_t>(item).awaitable;
                    else 
                        co_await std::get<file_reader2<>::awaitable_page_guard_t>(item);
                }
                co_return "";
            } catch (const std::exception& e) {
                 co_return std::string("Exception: ") + e.what();
            } }());

        EXPECT_EQ(error, "");
    }
} // namespace hedge::fs
