#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <sched.h>

#include <perf_counter.h>

#include "db/sst.h"
#include "fs/fs.hpp"
#include "io/io_requests.hpp"
#include "page_aligned_buffer.h"
#include "utils.h"

#include "stream_reader.h"

namespace hedge::fs
{

    tmc::task<void> reader_task(int32_t fd, stream_reader_config cfg, control_block cb)
    {
        const size_t read_ahead_size = hedge::ceil_page_align(cfg.read_ahead_size);
        size_t current_offset = cfg.start_offset;

        while(true)
        {
            if(current_offset >= cfg.end_offset)
            {
                cb.chan_tok.close();
                co_return;
            }

            size_t bytes_to_read = read_ahead_size;

            // Clip to end offset
            if(current_offset + read_ahead_size > cfg.end_offset)
                bytes_to_read = cfg.end_offset - current_offset;

            // Page align the read size
            bytes_to_read = hedge::ceil_page_align(bytes_to_read);

            co_await *cb.sem;

            page_aligned_buffer<std::byte> buffer;
            buffer.grow_uninitialized(bytes_to_read);

            auto res = co_await io::read(
                fd,
                buffer.data(),
                bytes_to_read,
                current_offset);

            if(res < 0)
            {
                // Error during read, send error through channel and exit
                cb.chan_tok.post(hedge::error(std::string("Read error: ") + std::strerror(-res)));
                cb.chan_tok.close();
                co_return;
            }

            bool ok = cb.chan_tok.post(std::move(buffer));
            if(!ok) // Channel closed, exit
                co_return;

            current_offset += bytes_to_read;
        }
    };

    control_block start_stream(
        tmc::ex_any* executor,
        const fs::file& fd,
        const stream_reader_config& config)
    {
        control_block cb{
            std::make_shared<tmc::semaphore>(1),
            tmc::make_channel<hedge::expected<hedge::page_aligned_buffer<std::byte>>>()};

        tmc::post(
            executor,
            reader_task(fd.fd(), config, control_block{cb.sem, cb.chan_tok.new_token()}));

        return cb;
    }

    tmc::task<hedge::expected<hedge::page_aligned_buffer<std::byte>>> control_block::next()
    {
        auto res = co_await this->chan_tok.pull();
        this->sem->release(1);

        if(!res.has_value())
            co_return hedge::error("stream eof", errc::END_OF_SCAN);

        auto maybe_buffer = std::move(res.value());

        if(!maybe_buffer.has_value())
            co_return hedge::error(res.value().error());

        auto buf = std::move(maybe_buffer.value());

        co_return std::move(buf);
    }

} // namespace hedge::fs
