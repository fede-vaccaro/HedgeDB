#include "io/io_requests.hpp"
#include "io/static_pool.h"

#include "tmc/channel.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"

#include "fs/fs.hpp"
#include "utils.h"

#include <error.hpp>

constexpr size_t NUM_FILES = 8;
constexpr size_t INPUT_SIZE = 1ULL * GiB;
constexpr size_t BUFFER_SIZE = 1 * MiB;
constexpr size_t MULTIPLIER = 1000;

struct control_block
{
    std::shared_ptr<tmc::semaphore> max_buffered_items;
    tmc::chan_tok<hedge::buffer_t> output;
};

tmc::task<void> make_stream_reader(
    std::string path,
    std::shared_ptr<tmc::semaphore> max_buffered_items,
    tmc::chan_tok<hedge::buffer_t> output)
{
    auto file = co_await hedge::fs::file::from_path_async(path, hedge::fs::file::open_mode::read_only, true);

    if(!file)
    {
        std::cerr << "Failed to open file: " << file.error().to_string() << "\n";
        co_return;
    }

    for(size_t offs = 0; offs < file.value().file_size() * MULTIPLIER; offs += BUFFER_SIZE)
    {
        co_await *max_buffered_items;

        hedge::buffer_t buffer = hedge::make_aligned_buffer(BUFFER_SIZE);

        auto res = co_await hedge::io::read(file.value().fd(), buffer.get(), BUFFER_SIZE, offs % INPUT_SIZE);
        if(res < 0)
        {
            std::cerr << "Failed to read from file: " << std::string(strerror(-res)) << "\n";
            co_return;
        }

        output.post(std::move(buffer));
    }

    co_return;
}

tmc::task<void> make_stream_writer(std::string path, std::vector<control_block> reader_channels)
{
    auto file = co_await hedge::fs::file::from_path_async(path, hedge::fs::file::open_mode::write_new, true, NUM_FILES * INPUT_SIZE);
    if(!file)
    {
        std::cerr << "Failed to open file for writing: " << file.error().to_string() << "\n";
        co_return;
    }

    size_t bytes_written = 0;
    constexpr auto OUTPUT_FILE_SIZE = INPUT_SIZE * NUM_FILES;

    while(bytes_written < OUTPUT_FILE_SIZE * MULTIPLIER)
    {
        for(auto& cb : reader_channels)
        {
            auto maybe_buf = co_await cb.output.pull();
            assert(maybe_buf);
            auto buf = std::move(maybe_buf.value());
            cb.max_buffered_items->release();
            auto n = co_await hedge::io::write(file.value().fd(), buf.get(), BUFFER_SIZE, bytes_written % OUTPUT_FILE_SIZE);
            if(n < 0)
            {
                std::cerr << "Failed to write to file: " << std::string(strerror(-n)) << "\n";
                co_return;
            }
            if(static_cast<size_t>(n) != BUFFER_SIZE)
            {
                std::cerr << "Partial write: expected " << BUFFER_SIZE << " bytes, wrote " << n << " bytes\n";
                co_return;
            }

            bytes_written += BUFFER_SIZE;
        }
    }
}

int main()
{
    hedge::io::static_pool::instance()->init(
        hedge::io::executor_config{
            .name = "bench_pool",
            .queue_depth = 64,
            .type = hedge::io::executor_type::GENERAL_PURPOSE,
            .n_threads = 1,
            .auto_detect = true,
        });

    std::vector<control_block> control_blocks;
    control_blocks.reserve(NUM_FILES);

    for(size_t i = 0; i < NUM_FILES; i++)
    {
        auto ch = tmc::make_channel<hedge::buffer_t>();
        auto sem = std::make_shared<tmc::semaphore>(4);
        auto filename = std::format("file_{}", i);

        control_blocks.emplace_back(sem, ch.new_token());
        tmc::post(
            *hedge::io::static_pool::instance(),
            make_stream_reader(filename, sem, std::move(ch)));
    }

    tmc::post_waitable(
        *hedge::io::static_pool::instance(),
        make_stream_writer("outfile", control_blocks)
    ).get();

    return 0;
}
