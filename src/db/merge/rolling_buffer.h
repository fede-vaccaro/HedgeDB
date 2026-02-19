#pragma once

#include "db/block.h"
#include "db/sst.h"
#include "fs/file_reader2.h"
#include <cstdlib>
#include <memory>
#include <new>

namespace hedge::db
{
    using file_reader = fs::file_reader2<fs::file>;
    using cached_frame = db::page_cache::read_page_guard;
    using disk_frame = page_aligned_buffer<uint8_t>;

    class rolling_buffer2
    {
        using buffered_page_t = std::variant<cached_frame, disk_frame>;

        std::deque<buffered_page_t> _buffers;
        [[maybe_unused]] size_t _read_ahead_size;
        file_reader _reader;
        std::vector<file_reader::awaitable_from_cache_or_fs_t> _pending_reads;

        std::deque<buffered_page_t>::iterator _cur_buffer{};

        // Reader
        buffer_decoder _block_reader;

        const uint8_t* _end_of_buffer(const buffered_page_t& buffer)
        {
            const auto* ptr = std::visit([](const auto& buffer)
                                         { return buffer.end(); }, buffer);
            assert(hedge::is_page_aligned((uint64_t)ptr));
            return ptr;
        }

        const uint8_t* _begin_of_buffer(const buffered_page_t& buffer)
        {
            const auto* ptr = std::visit([](const auto& buffer)
                                         { return buffer.begin(); }, buffer);
            assert(hedge::is_page_aligned((uint64_t)ptr));
            return ptr;
        }

    public:
        rolling_buffer2(const fs::file& file,
                        fs::file_reader2_config reader_cfg,
                        size_t read_ahead_size,
                        const std::shared_ptr<db::sharded_page_cache>& cache) : _read_ahead_size(read_ahead_size),
                                                                                _reader(file, reader_cfg)
        {
            this->_pending_reads = this->_reader.next(cache);
        }

        // JUST FOR DEBUGGING
        [[nodiscard]] const db::sst& index() const
        {
            // cast to sst
            return static_cast<const db::sst&>(this->_reader.file());
        }

        void _push(buffered_page_t&& t)
        {
            this->_buffers.emplace_back(std::move(t));

            if(this->_buffers.size() == 1)
            {
                this->_cur_buffer = this->_buffers.begin();
                this->_start_consuming_buffer(this->_cur_buffer);
            }
        }

        void _start_consuming_buffer(const std::deque<buffered_page_t>::iterator& buffer_it)
        {
            this->_block_reader.reset(this->_begin_of_buffer(*buffer_it),
                                      this->_end_of_buffer(*buffer_it));
        }

        // refresh is idempotent if the readers are EOF
        async::task<status> refresh(const std::shared_ptr<db::sharded_page_cache>& cache)
        {
            auto unpack_read_result = hedge::overloaded{
                [](file_reader::awaitable_page_guard_t& awaitable) -> async::task<expected<buffered_page_t>>
                {
                    co_return buffered_page_t{db::page_cache::read_page_guard(co_await awaitable)};
                },
                [](file_reader::awaitable_read_request_t& read_request) -> async::task<expected<buffered_page_t>>
                {
                    auto response = co_await read_request.awaitable;

                    if(response.error_code != 0)
                        co_return hedge::error("Read request failed with error code: " + std::string(strerror(-response.error_code)));

                    auto buffer = std::move(read_request.buffer);

                    co_return buffered_page_t{disk_frame{std::move(buffer)}};
                },
            };

            for(auto& read_request : this->_pending_reads)
            {
                auto maybe_read = co_await std::visit(unpack_read_result, read_request);
                if(!maybe_read)
                    co_return hedge::error("Failed to read from LHS during buffer refresh: " + maybe_read.error().to_string());

                this->_push(std::move(maybe_read.value()));
            }
            this->_pending_reads.clear();

            // Submit new requests
            if(!this->_reader.is_eof())
                this->_pending_reads = this->_reader.next(cache);

            co_return hedge::ok();
        }

        void pop_front()
        {
            // prof::counter_guard guard(prof::get<"rolling_buffer::pop_front">());

            // Try to move to the next entry in the current block
            if(++this->_block_reader != buffer_decoder::block_buffer_end_sentinel{}) [[likely]]
                return;

            // The current buffer is fully consumed, move to the next one
            // Erase current buffer
            this->_buffers.pop_front();

            // Move to next buffer (if any)
            this->_cur_buffer = this->_buffers.begin();

            if(this->_cur_buffer != this->_buffers.end())
                this->_start_consuming_buffer(this->_cur_buffer);
            else
                this->_cur_buffer = {}; // No more buffers to consume
        }

        const auto& front()
        {
            return this->_block_reader.it();
        }

        size_t size()
        {
            size_t size = 0;
            for(const auto& buffer : this->_buffers)
                size += (this->_end_of_buffer(buffer) - this->_begin_of_buffer(buffer));

            return size;
        }

        [[nodiscard]] bool buffer_empty() const
        {
            return this->_buffers.empty();
        }

        [[nodiscard]] bool is_eof() const
        {
            return this->_reader.is_eof() && this->buffer_empty() && this->_pending_reads.empty();
        }
    };

    struct rolling_buffer_set
    {
        rolling_buffer2* _data{nullptr};
        size_t _capacity{0};
        size_t _size{0};

        rolling_buffer_set(size_t capacity) : _capacity(capacity)
        {
            if(capacity > 0)
            {
                size_t bytes = sizeof(rolling_buffer2) * capacity;
                size_t alignment = alignof(rolling_buffer2);
                // Ensure size is a multiple of alignment for aligned_alloc
                if(bytes % alignment != 0)
                {
                    bytes += alignment - (bytes % alignment);
                }
                this->_data = static_cast<rolling_buffer2*>(std::aligned_alloc(alignment, bytes));
                if(this->_data == nullptr)
                    throw std::bad_alloc();
            }
        }

        ~rolling_buffer_set()
        {
            if(this->_data != nullptr)
            {
                for(size_t i = 0; i < _size; ++i)
                    this->_data[i].~rolling_buffer2();

                std::free(this->_data);
            }
        }

        template <typename... Args>
        rolling_buffer2& emplace_back(Args&&... args)
        {
            if(this->_size >= this->_capacity)
                throw std::length_error("rolling_buffer_set full");

            rolling_buffer2* ptr = this->_data + this->_size;
            ::new(ptr) rolling_buffer2(std::forward<Args>(args)...);
            this->_size++;
            return *ptr;
        }

        [[nodiscard]] rolling_buffer2* begin() const { return this->_data; }
        [[nodiscard]] rolling_buffer2* end() const { return this->_data + _size; }

        // Disable copy
        rolling_buffer_set(const rolling_buffer_set&) = delete;
        rolling_buffer_set& operator=(const rolling_buffer_set&) = delete;
    };
} // namespace hedge::db