#pragma once

#include <cstdlib>
#include <new>

#include <error.hpp>

#include "db/sst.h"
#include "db/block.h"
#include "fs/stream_reader.h"

namespace hedge::db
{

    class sst_stream_reader
    {
        // Stream reader control block
        fs::control_block _reader_cb;
        std::deque<page_aligned_buffer<std::byte>> _buffers;
        std::deque<page_aligned_buffer<std::byte>>::iterator _cur_buffer{};

        // Reader
        block_buffer_reader _block_reader = block_buffer_reader{true};

        // Reference SST
        const db::sst* _sst{};

        // EOF flag
        bool _eof{false};

    public:
        sst_stream_reader(const db::sst& file,
                          tmc::ex_any* executor,
                          fs::stream_reader_config reader_cfg) : _reader_cb(fs::start_stream(executor, file, reader_cfg)),
                                                                 _sst(&file)
        {
        }

        // Warning: might be unsafe
        [[nodiscard]] const db::sst& index() const
        {
            return *this->_sst;
        }

        [[nodiscard]] hedge::status _push(page_aligned_buffer<std::byte>&& buffer)
        {
            this->_buffers.emplace_back(std::move(buffer));

            if(this->_buffers.size() == 1)
            {
                this->_cur_buffer = this->_buffers.begin();
                return this->_start_consuming_buffer(this->_cur_buffer);
            }

            return hedge::ok();
        }

        [[nodiscard]] hedge::status _start_consuming_buffer(const std::deque<page_aligned_buffer<std::byte>>::iterator& buffer_it)
        {
            return this->_block_reader.reset(buffer_it->begin(),
                                             buffer_it->end());
        }

        // refresh is idempotent if the readers are EOF
        tmc::task<status> refresh()
        {
            auto read_request = co_await this->_reader_cb.next();

            if(!read_request.has_value() && read_request.error().code() == errc::END_OF_SCAN)
            {
                this->_eof = true;
                co_return hedge::ok();
            }

            if(!read_request.has_value())
                co_return hedge::error("Failed to read from stream: " + read_request.error().to_string());

            auto ok = this->_push(std::move(read_request.value()));
            if(!ok)
                co_return ok.error();

            co_return hedge::ok();
        }

        [[nodiscard]] hedge::status pop_front()
        {
            // Try to move to the next entry in the current block
            auto status = this->_block_reader.next();
            if(!status) [[unlikely]]
                return status;

            if(this->_block_reader != block_buffer_reader::block_buffer_end_sentinel{}) [[likely]]
                return hedge::ok();

            // The current buffer is fully consumed, move to the next one
            this->_buffers.pop_front();

            // Move to next buffer (if any)
            this->_cur_buffer = this->_buffers.begin();

            if(this->_cur_buffer != this->_buffers.end())
            {
                auto ok = this->_start_consuming_buffer(this->_cur_buffer);
                if(!ok)
                    return ok.error();
            }
            else
            {
                this->_cur_buffer = {}; // No more buffers to consume
            }
            return hedge::ok();
        }

        const auto& front()
        {
            return this->_block_reader.it();
        }

        size_t size()
        {
            size_t size = 0;
            for(const auto& buffer : this->_buffers)
                size += (buffer.end() - buffer.begin());

            return size;
        }

        [[nodiscard]] bool buffer_empty() const
        {
            return this->_buffers.empty();
        }

        [[nodiscard]] bool is_eof() const
        {
            return this->_eof && this->buffer_empty();
        }

        [[nodiscard]] uint64_t epoch() const { return this->index().epoch(); }
    };

    struct sst_stream_reader_set
    {
        sst_stream_reader* _data{nullptr};
        size_t _capacity{0};
        size_t _size{0};

        sst_stream_reader_set(size_t capacity) : _capacity(capacity)
        {
            if(capacity > 0)
            {
                size_t bytes = sizeof(sst_stream_reader) * capacity;
                size_t alignment = alignof(sst_stream_reader);
                // Ensure size is a multiple of alignment for aligned_alloc
                if(bytes % alignment != 0)
                {
                    bytes += alignment - (bytes % alignment);
                }
                this->_data = static_cast<sst_stream_reader*>(std::aligned_alloc(alignment, bytes));
                if(this->_data == nullptr)
                    throw std::bad_alloc();
            }
        }

        ~sst_stream_reader_set()
        {
            if(this->_data != nullptr)
            {
                for(size_t i = 0; i < _size; ++i)
                    this->_data[i].~sst_stream_reader();

                std::free(this->_data);
            }
        }

        template <typename... Args>
        sst_stream_reader& emplace_back(Args&&... args)
        {
            if(this->_size >= this->_capacity)
                throw std::length_error("sst_stream_reader_set full");

            sst_stream_reader* ptr = this->_data + this->_size;
            ::new(ptr) sst_stream_reader(std::forward<Args>(args)...);
            this->_size++;
            return *ptr;
        }

        [[nodiscard]] sst_stream_reader* begin() const { return this->_data; }
        [[nodiscard]] sst_stream_reader* end() const { return this->_data + _size; }

        // Disable copy
        sst_stream_reader_set(const sst_stream_reader_set&) = delete;
        sst_stream_reader_set& operator=(const sst_stream_reader_set&) = delete;
    };
} // namespace hedge::db
