// Tests for hedge::fs::stream_reader (src/fs/stream_reader.{h,cc}).
//
// The reader task acquires `sem` before every read and relies on the consumer
// releasing it (per chunk in next(), once more on teardown). The multi-chunk
// tests are the regression guard for that flow control: with a missing release
// they would deadlock instead of completing.

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "error.hpp"
#include "fs/fs.hpp"
#include "fs/stream_reader.h"
#include "io/io_executor.h"
#include "tmc/sync.hpp"
#include "types.h"

namespace hedge::fs
{
    static const std::string TEST_DIR = "/tmp/hh_stream_reader";
    static const std::string TEST_FILE = TEST_DIR + "/stream_reader_test";

    static constexpr size_t PAGE = PAGE_SIZE_IN_BYTES;

    class StreamReaderTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            if(std::filesystem::exists(TEST_DIR))
                std::filesystem::remove_all(TEST_DIR);
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

        // Writes `size` bytes where byte at absolute offset i == i % 256.
        static void create_test_file(size_t size)
        {
            std::ofstream ofs(TEST_FILE, std::ios::binary | std::ios::trunc);
            std::vector<char> buf(size);
            for(size_t i = 0; i < size; ++i)
                buf[i] = static_cast<char>(i % 256);
            ofs.write(buf.data(), static_cast<std::streamsize>(size));
            ofs.close();
        }

        // Runs on an executor thread: drains [start, end) through the stream
        // reader, then verifies the first (end - start) collected bytes match
        // the file pattern. Returns "" on success, otherwise an error message.
        // `buffers_out` (optional) receives the number of chunks delivered.
        tmc::task<std::string> stream_and_verify(
            file& f,
            size_t start,
            size_t end,
            size_t read_ahead,
            size_t* buffers_out)
        {
            control_block cb = start_stream(
                this->_executor->type_erased(),
                f,
                stream_reader_config{
                    .start_offset = start,
                    .end_offset = end,
                    .read_ahead_size = read_ahead});

            std::vector<std::byte> collected;
            size_t buffers = 0;

            while(true)
            {
                auto r = co_await cb.next();

                if(!r.has_value())
                {
                    if(r.error().code() == errc::END_OF_SCAN)
                        break;
                    co_return "stream error: " + r.error().to_string();
                }

                ++buffers;
                auto& chunk = r.value();
                collected.insert(collected.end(), chunk.data(), chunk.data() + chunk.size());
            }

            if(buffers_out != nullptr)
                *buffers_out = buffers;

            const size_t logical = end - start;
            if(collected.size() < logical)
                co_return "short stream: got " + std::to_string(collected.size()) +
                          " want >= " + std::to_string(logical);

            for(size_t i = 0; i < logical; ++i)
            {
                auto expected = static_cast<std::byte>((start + i) % 256);
                if(collected[i] != expected)
                    co_return "mismatch at offset " + std::to_string(start + i) +
                              " exp " + std::to_string(static_cast<unsigned>(expected)) +
                              " got " + std::to_string(static_cast<unsigned>(collected[i]));
            }

            co_return "";
        }

        std::string run(size_t start, size_t end, size_t read_ahead, size_t* buffers_out = nullptr)
        {
            auto fr = file::from_path(TEST_FILE, file::open_mode::read_only, true);
            if(!fr.has_value())
                return "open failed: " + fr.error().to_string();
            auto& f = fr.value();

            auto fut = tmc::post_waitable(*this->_executor, [&]() -> tmc::task<std::string>
                                          {
                try { co_return co_await this->stream_and_verify(f, start, end, read_ahead, buffers_out); }
                catch(const std::exception& e) { co_return std::string("exception: ") + e.what(); } }());

            return fut.get();
        }
    };

    TEST_F(StreamReaderTest, whole_file_in_single_chunk)
    {
        const size_t size = 4 * PAGE;
        create_test_file(size);

        size_t buffers = 0;
        EXPECT_EQ(run(0, size, size, &buffers), "");
        EXPECT_EQ(buffers, 1U);
    }

    TEST_F(StreamReaderTest, multiple_page_sized_chunks)
    {
        const size_t size = 4 * PAGE;
        create_test_file(size);

        size_t buffers = 0;
        EXPECT_EQ(run(0, size, PAGE, &buffers), "");
        EXPECT_EQ(buffers, 4U);
    }

    TEST_F(StreamReaderTest, read_ahead_rounded_up_to_page)
    {
        const size_t size = 4 * PAGE;
        create_test_file(size);

        // read_ahead < a page must be ceil-aligned to one page per read.
        size_t buffers = 0;
        EXPECT_EQ(run(0, size, 100, &buffers), "");
        EXPECT_EQ(buffers, 4U);
    }

    TEST_F(StreamReaderTest, multi_page_read_ahead)
    {
        const size_t size = 8 * PAGE;
        create_test_file(size);

        // 3-page read-ahead over an 8-page file -> 3 + 3 + 2 pages.
        size_t buffers = 0;
        EXPECT_EQ(run(0, size, 3 * PAGE, &buffers), "");
        EXPECT_EQ(buffers, 3U);
    }

    TEST_F(StreamReaderTest, sub_range)
    {
        const size_t size = 8 * PAGE;
        create_test_file(size);

        EXPECT_EQ(run(2 * PAGE, 6 * PAGE, 2 * PAGE), "");
    }

    TEST_F(StreamReaderTest, empty_range_yields_no_chunks)
    {
        const size_t size = 4 * PAGE;
        create_test_file(size);

        size_t buffers = 99;
        EXPECT_EQ(run(2 * PAGE, 2 * PAGE, PAGE, &buffers), "");
        EXPECT_EQ(buffers, 0U);
    }

    // Repeated start/drain cycles on one executor: catches a leaked or stuck
    // reader task from a previous stream blocking the next one.
    TEST_F(StreamReaderTest, repeated_streams_reuse_executor)
    {
        const size_t size = 4 * PAGE;
        create_test_file(size);

        for(int i = 0; i < 5; ++i)
            EXPECT_EQ(run(0, size, PAGE), "") << "iteration " << i;
    }
} // namespace hedge::fs
