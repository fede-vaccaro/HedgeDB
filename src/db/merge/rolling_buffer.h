#pragma once

#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <optional>

#include "db/block.h"
#include "db/memtable.h"
#include "db/skiplist.h"
#include "db/sst.h"
#include "error.hpp"
#include "fs/file_reader2.h"

namespace hedge::db
{
    using file_reader = fs::file_reader2<fs::file>;
    using cached_frame = db::page_cache::read_page_guard;
    using disk_frame = page_aligned_buffer<uint8_t>;

    class sst_stream
    {
        using buffered_page_t = std::variant<cached_frame, disk_frame>;

        std::deque<buffered_page_t> _buffers;
        [[maybe_unused]] size_t _read_ahead_size;
        file_reader _reader;
        std::vector<file_reader::awaitable_from_cache_or_fs_t> _pending_reads;

        std::deque<buffered_page_t>::iterator _cur_buffer{};

        // Reader
        block_buffer_reader _block_reader = block_buffer_reader{true};

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
        sst_stream(const fs::file& file,
                   fs::file_reader2_config reader_cfg,
                   size_t read_ahead_size,
                   const std::shared_ptr<db::sharded_page_cache>& cache) : _read_ahead_size(read_ahead_size),
                                                                           _reader(file, reader_cfg)
        {
            this->_pending_reads = this->_reader.next(cache);
        }

        // Warning: unsafe
        [[nodiscard]] const db::sst& index() const
        {
            // cast to sst
            return static_cast<const db::sst&>(this->_reader.file());
        }

        [[nodiscard]] hedge::status _push(buffered_page_t&& t)
        {
            this->_buffers.emplace_back(std::move(t));

            if(this->_buffers.size() == 1)
            {
                this->_cur_buffer = this->_buffers.begin();
                return this->_start_consuming_buffer(this->_cur_buffer);
            }

            return hedge::ok();
        }

        [[nodiscard]] hedge::status _start_consuming_buffer(const std::deque<buffered_page_t>::iterator& buffer_it)
        {
            return this->_block_reader.reset(this->_begin_of_buffer(*buffer_it),
                                             this->_end_of_buffer(*buffer_it));
        }

        // refresh is idempotent if the readers are EOF
        tmc::task<status> refresh(const std::shared_ptr<db::sharded_page_cache>& cache)
        {
            auto unpack_read_result = hedge::overloaded{
                [](file_reader::awaitable_page_guard_t& awaitable) -> tmc::task<expected<buffered_page_t>>
                {
                    co_return buffered_page_t{db::page_cache::read_page_guard(co_await awaitable)};
                },
                [](file_reader::awaitable_read_request_t& read_request) -> tmc::task<expected<buffered_page_t>>
                {
                    auto response = co_await std::move(read_request.awaitable);

                    if(response < 0)
                        co_return hedge::error("Read request failed with error code: " + std::string(strerror(-response)));

                    auto buffer = std::move(read_request.buffer);

                    co_return buffered_page_t{disk_frame{std::move(buffer)}};
                },
            };

            for(auto& read_request : this->_pending_reads)
            {
                auto maybe_read = co_await std::visit(unpack_read_result, read_request);
                if(!maybe_read)
                    co_return hedge::error("Failed to read from LHS during buffer refresh: " + maybe_read.error().to_string());

                auto ok = this->_push(std::move(maybe_read.value()));
                if(!ok)
                    co_return ok.error();
            }
            this->_pending_reads.clear();

            // Submit new requests
            if(!this->_reader.is_eof())
                this->_pending_reads = this->_reader.next(cache);

            co_return hedge::ok();
        }

        [[nodiscard]] hedge::status pop_front()
        {
            // prof::counter_guard guard(prof::get<"rolling_buffer::pop_front">());

            // Try to move to the next entry in the current block
            auto status = this->_block_reader.next_block();
            if(!status) [[unlikely]]
                return status;

            if(this->_block_reader != block_buffer_reader::block_buffer_end_sentinel{}) [[likely]]
                return hedge::ok();

            // The current buffer is fully consumed, move to the next one
            // Erase current buffer
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

        [[nodiscard]] uint64_t epoch() const { return this->index().epoch(); }
    };

    struct sst_stream_set
    {
        sst_stream* _data{nullptr};
        size_t _capacity{0};
        size_t _size{0};

        sst_stream_set(size_t capacity) : _capacity(capacity)
        {
            if(capacity > 0)
            {
                size_t bytes = sizeof(sst_stream) * capacity;
                size_t alignment = alignof(sst_stream);
                // Ensure size is a multiple of alignment for aligned_alloc
                if(bytes % alignment != 0)
                {
                    bytes += alignment - (bytes % alignment);
                }
                this->_data = static_cast<sst_stream*>(std::aligned_alloc(alignment, bytes));
                if(this->_data == nullptr)
                    throw std::bad_alloc();
            }
        }

        ~sst_stream_set()
        {
            if(this->_data != nullptr)
            {
                for(size_t i = 0; i < _size; ++i)
                    this->_data[i].~sst_stream();

                std::free(this->_data);
            }
        }

        template <typename... Args>
        sst_stream& emplace_back(Args&&... args)
        {
            if(this->_size >= this->_capacity)
                throw std::length_error("sst_stream_set full");

            sst_stream* ptr = this->_data + this->_size;
            ::new(ptr) sst_stream(std::forward<Args>(args)...);
            this->_size++;
            return *ptr;
        }

        [[nodiscard]] sst_stream* begin() const { return this->_data; }
        [[nodiscard]] sst_stream* end() const { return this->_data + _size; }

        // Disable copy
        sst_stream_set(const sst_stream_set&) = delete;
        sst_stream_set& operator=(const sst_stream_set&) = delete;
    };

    class memtable_cursor
    {
        skiplist_t::Accessor _accessor;
        skiplist_t::iterator _it;
        skiplist_t::iterator _end;
        uint64_t _max_seq;
        uint64_t _epoch;

        void _skip_invisible()
        {
            while(this->_it != this->_end && this->_it->seq > this->_max_seq)
                ++this->_it;
        }

        void _skip_duplicate_keys(const key_t& key)
        {
            while(this->_it != this->_end && this->_it->_key == key)
                ++this->_it;
        }

    public:
        memtable_cursor(const memtable_impl3_t& mem,
                        const std::optional<key_t>& lower,
                        const std::optional<key_t>& upper,
                        uint64_t epoch,
                        uint64_t max_seq = std::numeric_limits<uint64_t>::max())
            : _accessor(const_cast<memtable_impl3_t*>(&mem)),
              _max_seq(max_seq),
              _epoch(epoch)
        {
            this->_it = lower ? this->_accessor.lower_bound(memtable_entry{*lower, std::numeric_limits<uint64_t>::max(), {}})
                              : this->_accessor.begin();
            this->_end = upper ? this->_accessor.lower_bound(memtable_entry{*upper, std::numeric_limits<uint64_t>::max(), {}})
                               : this->_accessor.end();
            this->_skip_invisible();
        }

        [[nodiscard]] bool is_eof() const { return this->_it == this->_end; }
        [[nodiscard]] bool buffer_empty() const { return this->is_eof(); }
        [[nodiscard]] const memtable_entry& front() const { return *this->_it; }

        hedge::status pop_front()
        {
            auto key = this->_it->_key;
            ++this->_it;
            this->_skip_invisible();
            this->_skip_duplicate_keys(key);
            return hedge::ok();
        }

        [[nodiscard]] uint64_t epoch() const { return this->_epoch; }
    };
} // namespace hedge::db