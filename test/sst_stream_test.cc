#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <span>
#include <vector>

#include "db/block.h"
#include "db/merge/sst_stream.h"
#include "fs/fs.hpp"
#include "io/io_executor.h"
#include "types.h"

using namespace hedge::db;
using namespace hedge::fs;
using namespace hedge;

struct SstStreamTest : public ::testing::TestWithParam<size_t>
{
    std::filesystem::path temp_file_path;
    std::unique_ptr<hedge::io::io_executor> executor;

    void SetUp() override
    {
        temp_file_path = std::filesystem::temp_directory_path() / "sst_stream_test.sst";
        if(std::filesystem::exists(temp_file_path))
            std::filesystem::remove(temp_file_path);

        executor = std::make_unique<hedge::io::io_executor>(
            hedge::io::executor_config{
                .queue_depth = 32,
                .n_threads = 1,
                .auto_detect = false,
            });
    }

    void TearDown() override
    {
        executor.reset();
        if(std::filesystem::exists(temp_file_path))
            std::filesystem::remove(temp_file_path);
    }
};

TEST_P(SstStreamTest, WriteAndReadBack)
{
    size_t read_ahead_size = GetParam();
    size_t N_ITEMS = 5000;

    // 1. Generate Data
    std::vector<std::pair<std::string, value_ptr_t>> data;
    data.reserve(N_ITEMS);

    std::mt19937 gen(42);
    std::uniform_int_distribution<uint64_t> dist_u64;
    std::uniform_int_distribution<uint32_t> dist_u32;
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_int_distribution<int> len_dist(10, 50);

    for(size_t i = 0; i < N_ITEMS; ++i)
    {
        int len = len_dist(gen);
        std::string key;
        key.reserve(len);
        for(int j = 0; j < len; ++j)
            key.push_back(static_cast<char>(char_dist(gen)));

        value_ptr_t val(dist_u64(gen), dist_u32(gen), dist_u32(gen));

        data.emplace_back(std::move(key), val);
    }

    std::sort(data.begin(), data.end(), [](const auto& a, const auto& b)
              { return a.first < b.first; });

    // 2. Write to file
    {
        std::ofstream out(temp_file_path, std::ios::binary);
        ASSERT_TRUE(out.is_open());

        constexpr size_t BUFFER_SIZE = PAGE_SIZE_IN_BYTES * 4;
        std::vector<std::byte> buffer(BUFFER_SIZE);

        block_buffer_writer writer(buffer.data(), buffer.data() + buffer.size());

        for(const auto& [key_str, val] : data)
        {
            std::span<const std::byte> key_span(reinterpret_cast<const std::byte*>(key_str.data()), key_str.size());
            std::span<const std::byte> val_span(reinterpret_cast<const std::byte*>(&val), sizeof(val));

            auto push_status = writer.push(key_span, val_span, [](std::span<const std::byte>) {});

            if(!push_status && push_status.error().code() == errc::BUFFER_FULL)
            {
                out.write(reinterpret_cast<char*>(buffer.data()), writer.bytes_written());
                writer.reset();

                push_status = writer.push(key_span, val_span, [](std::span<const std::byte>) {});
                ASSERT_TRUE(push_status) << "Failed to push even after reset";
            }
            else
            {
                ASSERT_TRUE(push_status) << "Push failed with error: " << push_status.error().to_string();
            }
        }

        if(!writer.empty())
        {
            writer.force_commit();
            out.write(reinterpret_cast<char*>(buffer.data()), writer.bytes_written());
        }
        out.close();
    }

    size_t file_size = std::filesystem::file_size(temp_file_path);
    ASSERT_GT(file_size, 0);

    // 3. Read back
    auto file_res = fs::file::from_path(temp_file_path, fs::file::open_mode::read_only);
    ASSERT_TRUE(file_res) << "Failed to open file: " << file_res.error().to_string();
    fs::file file = std::move(file_res.value());

    fs::file_reader2_config config;
    config.start_offset = 0;
    config.end_offset = file_size;
    config.read_ahead_size = read_ahead_size;

    auto read_task = [&]() -> tmc::task<size_t>
    {
        sst_stream rb(file, config, config.read_ahead_size);
        size_t idx = 0;

        auto status = co_await rb.refresh();
        if(!status)
        {
            std::cerr << "Initial refresh failed: " << status.error().to_string() << std::endl;
            co_return idx;
        }

        while(true)
        {
            if(rb.buffer_empty())
            {
                if(rb.is_eof())
                    break;

                status = co_await rb.refresh();
                if(!status)
                {
                    std::cerr << "Refresh failed: " << status.error().to_string() << std::endl;
                    break;
                }
                if(rb.buffer_empty())
                    break;
            }

            const auto& it = rb.front();
            auto read_key = it.key();
            auto read_val = it.value();

            if(idx >= data.size())
            {
                EXPECT_LT(idx, data.size()) << "Read more items than expected";
                break;
            }

            const auto& [exp_key_str, exp_val] = data[idx];
            std::span<const std::byte> exp_key_span(reinterpret_cast<const std::byte*>(exp_key_str.data()), exp_key_str.size());
            std::span<const std::byte> exp_val_span(reinterpret_cast<const std::byte*>(&exp_val), sizeof(exp_val));

            auto spans_equal = [](std::span<const std::byte> a, std::span<const std::byte> b)
            {
                return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0;
            };

            if(!spans_equal(read_key, exp_key_span))
            {
                std::string rk(reinterpret_cast<const char*>(read_key.data()), read_key.size());
                EXPECT_EQ(rk, exp_key_str) << "Keys mismatch at index " << idx;
            }

            if(!spans_equal(read_val, exp_val_span))
            {
                EXPECT_TRUE(false) << "Values mismatch at index " << idx;
            }

            if(auto s = rb.pop_front(); !s)
            {
                EXPECT_TRUE(false) << "Error on pop_front: " << s.error().to_string();
            }

            idx++;
        }

        co_return idx;
    };

    size_t items_read = tmc::post_waitable(*executor, read_task(), 0).get();
    EXPECT_EQ(items_read, data.size()) << "Read item count differs from written";
}

INSTANTIATE_TEST_SUITE_P(
    ReadAheadSizes,
    SstStreamTest,
    ::testing::Values(
        PAGE_SIZE_IN_BYTES,
        PAGE_SIZE_IN_BYTES * 2,
        PAGE_SIZE_IN_BYTES * 4,
        PAGE_SIZE_IN_BYTES * 8,
        PAGE_SIZE_IN_BYTES * 16),
    [](const testing::TestParamInfo<SstStreamTest::ParamType>& info)
    {
        return "ReadAhead_" + std::to_string(info.param);
    });
