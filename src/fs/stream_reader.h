#pragma once

#include <error.hpp>
#include <sys/types.h>

#include <tmc/channel.hpp>
#include <tmc/ex_any.hpp>
#include <tmc/semaphore.hpp>

#include "fs.hpp"
#include "page_aligned_buffer.h"

/*
    HedgeFS Stream File Reader
    Provides an asynchronous file reader abstraction for streaming chunks of data from files using HedgeFS's async I/O executor.
*/
namespace hedge::fs
{

    struct control_block
    {
        std::shared_ptr<tmc::semaphore> sem;
        tmc::chan_tok<hedge::expected<hedge::page_aligned_buffer<std::byte>>> chan_tok;

        control_block(
            std::shared_ptr<tmc::semaphore> sem,
            tmc::chan_tok<hedge::expected<hedge::page_aligned_buffer<std::byte>>> chan_tok)
            : sem(std::move(sem)), chan_tok(std::move(chan_tok))
        {
        }

        // Movable, not copyable: copying would let a temporary's destructor
        // close the shared channel out from under the live owner. A moved-from
        // block has a null `sem` and its destructor becomes a no-op.
        control_block(control_block&&) = default;
        control_block& operator=(control_block&&) = default;
        control_block(const control_block&) = delete;
        control_block& operator=(const control_block&) = delete;

        tmc::task<hedge::expected<hedge::page_aligned_buffer<std::byte>>> next();

        ~control_block()
        {
            if(this->sem)
            {
                this->chan_tok.close();
                this->sem->release();
            }
        }
    };

    struct stream_reader_config
    {
        size_t start_offset{};
        size_t end_offset{};
        size_t read_ahead_size;
    };

    control_block start_stream(
        tmc::ex_any* executor,
        const fs::file& fd,
        const stream_reader_config& config);

} // namespace hedge::fs
