#pragma once

#include <cstdint>
#include <optional>

#include <error.hpp>

#include "common.h"
#include "fs/file_reader.h"

namespace hedge::db
{

    // rolling_buffer
    //
    // Currently needed as a helper class for the secondary memory merge sort.
    //
    // This is a helper class needed for mantaining a rolling buffer while
    // loading pages of subsequent index_key_t from a sorted index file
    // assume that we loaded a page, and we are iterating over it through
    // iterator_t& rolling_buffer::iterator(). This is the buffer and the
    // object at which the iterator points is highlited by '[]'
    //
    // BUFFER: {000, 001, 010, 011, [0aa], 0bb, 0f0, 0ff}
    //
    // When loading a new page of, let's say 8 entries, the buffer
    // and the iterator are updated as following:
    //
    // BUFFER: {[aa], bb, f0, ff, 001, 003, 020, 021, 1aa, 1bb, 1f0, 2ff}
    //
    // As you can see, the read section is discarded, the newly read buffer
    // is appended and the iterator is updated
    //
    // It's important to note that for convenience the iterator "views" the
    // buffer as an array of `index_key_t`, while the underlying type is
    // uint8_t.
    class rolling_buffer
    {
        using byte_buffer_t = std::vector<uint8_t>;
        using span_t = std::span<index_key_t>;
        using iterator_t = std::span<index_key_t>::iterator;
        using buffer_iterator_t = byte_buffer_t::iterator;

        fs::file_reader _reader;

        byte_buffer_t _buffer;
        span_t _view;
        iterator_t _it;

    public:
        rolling_buffer(fs::file_reader&& reader) : _reader(std::move(reader))
        {
            this->_init();
        }

        iterator_t& it()
        {
            return this->_it;
        }

        buffer_iterator_t buffer_it()
        {
            return buffer_iterator_t(reinterpret_cast<uint8_t*>(this->_it.base()));
        }

        iterator_t end()
        {
            return this->_view.end();
        }

        bool eof()
        {
            return this->_buffer.empty();
        }

        const byte_buffer_t& buffer()
        {
            return this->_buffer;
        }

        async::task<hedge::status> next(size_t bytes_to_read)
        {
            co_return co_await this->_next(bytes_to_read);
        }

    private:
        async::task<hedge::expected<std::vector<uint8_t>>> _read_from_io(size_t read_ahead_size)
        {
            auto maybe_buffer = co_await this->_reader.next(read_ahead_size);
            if(!maybe_buffer.has_value())
                co_return hedge::error("Failed to read from io: " + maybe_buffer.error().to_string());

            co_return std::move(maybe_buffer.value());
        };

        async::task<hedge::status> _next(size_t bytes_to_read)
        {
            auto maybe_new_read = co_await this->_read_from_io(bytes_to_read);

            if(!maybe_new_read)
                co_return hedge::error("Failed to read from index view: " + maybe_new_read.error().to_string()); // todo: more meaningful error

            this->_consume_and_push(std::move(maybe_new_read.value()));

            co_return hedge::ok();
        }

        void _consume_and_push(byte_buffer_t&& new_buffer)
        {
            this->_buffer.erase(this->_buffer.begin(), this->buffer_it());
            this->_buffer.insert(this->_buffer.end(), new_buffer.begin(), new_buffer.end());
            this->_view = view_as<index_key_t>(this->_buffer);
            this->_it = this->_view.begin();
        }

        void _init()
        {
            this->_view = view_as<index_key_t>(this->_buffer);
            this->_it = this->_view.begin();
        }
    };

    class unique_buffer
    {
        std::optional<index_key_t> _buffered_item{};

        std::optional<index_key_t> _ready_item{};

    public:
        unique_buffer() = default;

        void push(index_key_t new_item)
        {
            if(!this->_buffered_item)
                this->_buffered_item = new_item;
            else if(this->_buffered_item->key != new_item.key)
                this->_ready_item = std::exchange(this->_buffered_item, new_item);
            else if(new_item.value_ptr < this->_buffered_item->value_ptr)
                this->_buffered_item = new_item;
        }

        bool ready()
        {
            return this->_ready_item.has_value();
        }

        index_key_t force_pop()
        {
            if(this->_ready_item.has_value())
                throw std::runtime_error("Ready item still present, cannot pop last");

            if(!this->_buffered_item.has_value())
                throw std::runtime_error("No item to pop");

            return this->_buffered_item.value();
        }

        index_key_t pop()
        {
            if(this->_ready_item.has_value())
                return std::exchange(this->_ready_item, std::nullopt).value();

            throw std::runtime_error("No ready item to pop");
        }
    };

} // namespace hedge::db