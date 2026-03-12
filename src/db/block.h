#pragma once

/*

This Block format employes prefix compaction with restart points.

*********************************************************
************** DYNAMIC PREFIX BLOCK FORMAT **************
*********************************************************

Within the block, every key is sorted.
A restart group, is a ordered set of key-value pairs.
At the beginning of a restart group, a full key is stored.
The following 15 keys (or different, depending on configuration)
are stored using prefix and delta encoding. The prefix is (totally or partially)
shared with the previous key in the group; the remainder (the delta) and its size is
stored without any encoding or compression. Then, the value (uncompressed and unencoded)
and its size are written.

To allow a faster lookup, the restart point offsets (first key of the restart
group) key-value are kept in order and stored at the end of the block, before
the Footer (8 bytes) and before the restart count (4 bytes).

Block format:
[ Entry  0: Full Key Size + Full Key + Value Size + Value ] <--- Restart Point 0 (Offset: 0)
[ Entry  1: Shared Prefix Size + Delta Key Size + Delta Key + Value Size + Value ]
[ ... ]
[ Entry 16: Full Key Size + Full Key + Value Size + Value ] <--- Restart Point 1 (Offset: XXX)
[ ... ]
[ Entry  N: Shared Prefix size + Delta Key + Value Size + Value ]
[ ... FREE SPACE (could not be used) ... ]
[ Restart Offset 0 (2B) ] <--- Trailer starts here
[ Restart Offset 1 (2B) ]
[ ... ]
[ Restart Offset M (2B) ]
[ Restart Count (2B)    ]
[ Footer Metadata (8B)  ] <--- Page Type, Checksum, etc. (TBD)

*********************************************************

*/

#include <cstdint>
#include <error.hpp>

#include "stack_allocator.h"
#include "types.h"

namespace hedge::db
{

    struct block_config
    {
        size_t block_size_in_bytes{PAGE_SIZE_IN_BYTES};
        size_t restart_group_size{4}; // Use power of 2
    };

    struct block_footer
    {
        uint16_t kv_count{};
        std::array<uint8_t, 6> _footer_bytes; // Padding
    };

    template <typename T>
        requires std::is_unsigned_v<T>
    struct inline_vector : std::vector<T, stack_allocator<T, MAX_KEY_LEN * 2>>
    {
        inline_vector() : std::vector<T, stack_allocator<T, MAX_KEY_LEN * 2>>()
        {
            this->reserve(MAX_KEY_LEN * 2);
        }

        inline_vector(auto begin, auto end)
        {
            this->reserve(MAX_KEY_LEN * 2);
            this->assign(begin, end);
        }
        
    };

    class block_encoder
    {
        friend struct BlockTest;
        static constexpr size_t FOOTER_SIZE = 8;

        block_config _cfg{};

        // Builder state
        uint8_t* _base{};
        uint8_t* _head{};
        inline_vector<uint8_t> _last_key{}; // TODO: try std::pmr::vector
        inline_vector<uint16_t> _offsets{}; // TODO: try std::pmr::vector
        size_t _bytes_written{};
        uint32_t _kvs_count{};
        uint32_t _restart_keys_written{};
        bool _committed{false};

        block_footer _footer{}; // currently unused

    public:
        static constexpr size_t MIN_KEY_lEN = 1;
        static constexpr size_t MAX_KEY_LEN = 256;

        block_encoder(uint8_t* base, const block_config& cfg = {});

        hedge::status push(std::span<const uint8_t> key, std::span<const uint8_t> value); // Returns error code BUFFER_FULL if could not push the key-value

        void reset(uint8_t* new_base);

        // Write Footer and restart offsets
        // Otherwise those are lazily written if the block is full
        void commit()
        {
            this->_commit();
        }

        [[nodiscard]] std::span<const uint8_t> last_pushed_key() const
        {
            return this->_last_key;
        }

        [[nodiscard]] bool committed() const
        {
            return this->_committed;
        }

        [[nodiscard]] size_t kv_count() const
        {
            return this->_kvs_count;
        }

    private:
        void _commit();
        hedge::status _push_as_restart_key(std::span<const uint8_t> key, std::span<const uint8_t> value);
        hedge::status _push_as_delta_key(std::span<const uint8_t> key, std::span<const uint8_t> value);

        static uint32_t _compute_shared_prefix_length(std::span<const uint8_t> prev, std::span<const uint8_t> curr)
        {
            auto prev_it = prev.begin();
            auto curr_it = curr.begin();

            uint32_t count{0};

            while(prev_it != prev.end() &&
                  curr_it != curr.end() &&
                  *curr_it++ == *prev_it++)
                ++count;

            return count;
        }
    };

    class block_iterator_sentinel
    {
    };

    class block_iterator_restart_group_sentinel
    {
    };

    class block_iterator
    {
        const uint8_t* _head{nullptr};

        uint32_t _kv_idx{};
        uint32_t _starting_kv_idx{};
        uint32_t _kvs_in_block_count{};
        uint32_t _restart_group_size{};

        inline_vector<uint8_t> _key{};
        std::span<const uint8_t> _value_ref{};

    public:
        block_iterator(const uint8_t* base,
                       uint32_t starting_kv_idx,
                       uint32_t _kvs_in_block,
                       uint32_t restart_group_size);
        block_iterator() = default;

        block_iterator(block_iterator&& other) noexcept = default;
        block_iterator& operator=(block_iterator&& other) noexcept = default;

        block_iterator(const block_iterator& other) = default;
        block_iterator& operator=(const block_iterator& other) = default;

        block_iterator& operator++();

        [[nodiscard]] std::span<const uint8_t> key() const;
        [[nodiscard]] std::span<const uint8_t> value() const;

        bool operator==(block_iterator_sentinel) const;
        bool operator==(block_iterator_restart_group_sentinel) const;

    private:
        void _read_next();
        void _read_restart_key();
        void _read_delta_key();
    };

    class block_decoder
    {
        block_config _cfg{};
        const uint8_t* _base{};
        block_footer _footer{};

    public:
        block_decoder(const block_config& cfg = {}) : _cfg(cfg){};
        block_decoder(const uint8_t* base, const block_config& cfg = {});
        std::span<const uint8_t> find(std::span<const uint8_t> key);
        void reset(const uint8_t* new_base);
        void _read_footer();

        [[nodiscard]] block_iterator begin() const;
        [[nodiscard]] block_iterator_sentinel end() const;
    };

    // buffer_encoder encodes the pushed key values in blocks into the configured buffer
    // Respect to the block_encoder, it automatically switch to the next block when the current has no space left
    class block_buffer_writer
    {
        uint8_t* const _begin{};
        uint8_t* _cur{};
        uint8_t* const _end{};
        block_encoder _block_encoder;

    public:
        block_buffer_writer(uint8_t* begin, uint8_t* end)
            : _begin(begin),
              _cur(begin),
              _end(end),
              _block_encoder(begin)
        {
        }

        const block_encoder& encoder()
        {
            return this->_block_encoder;
        }

        template <typename CALLABLE>
        hedge::status push(std::span<const uint8_t> key, std::span<const uint8_t> value, CALLABLE&& on_end_of_block)
        {
            auto s = this->_block_encoder.push(key, value);

            if(!s)
            {
                assert(s.error().code() == errc::BUFFER_FULL);
                assert(this->_block_encoder.committed());

                on_end_of_block(this->_block_encoder.last_pushed_key());

                if(this->_cur + PAGE_SIZE_IN_BYTES == this->_end)
                    return s;

                this->_cur += PAGE_SIZE_IN_BYTES;
                this->_block_encoder.reset(this->_cur);
                s = this->_block_encoder.push(key, value);
            }

            return s;
        }

        void reset()
        {
            this->_cur = this->_begin;
            this->_block_encoder.reset(this->_cur);
        }

        [[nodiscard]] bool empty() const
        {
            return this->_cur == this->_begin && this->_block_encoder.kv_count() == 0;
        }

        void force_commit()
        {
            this->_block_encoder.commit();
        }

        size_t bytes_written()
        {
            // Always include the current block
            return std::distance(this->_begin, this->_cur) + PAGE_SIZE_IN_BYTES;
        }
    };

    // buffer_decoder decodes the key values encoded in blocks from the configured buffer
    // Respect to the block_decoder it automatically switch to the next block when the current has been fully decoded
    class buffer_decoder
    {
        const uint8_t* _next{};
        const uint8_t* _end{};
        block_decoder _block_decoder;
        block_iterator _block_it;

    public:
        buffer_decoder() = default;

        buffer_decoder(const uint8_t* begin, const uint8_t* end)
            : _next(begin + PAGE_SIZE_IN_BYTES),
              _end(end),
              _block_decoder(begin),
              _block_it(_block_decoder.begin())
        {
        }

        void reset(const uint8_t* begin, const uint8_t* end)
        {
            this->_next = begin + PAGE_SIZE_IN_BYTES;
            this->_end = end;
            this->_block_decoder.reset(begin);
            this->_block_it = this->_block_decoder.begin();
        }

        const block_decoder& decoder()
        {
            return this->_block_decoder;
        }

        buffer_decoder& operator++()
        {
            if(++this->_block_it != this->_block_decoder.end()) // There are KV left -> continue
                return *this;

            // Block fully read, move to the next block if any
            if(this->_next != this->_end)
            {
                const auto* cur = this->_next;
                this->_next += PAGE_SIZE_IN_BYTES;
                this->_block_decoder.reset(cur);
                this->_block_it = this->_block_decoder.begin();
            }
            return *this;
        }

        [[nodiscard]] const auto& it() const
        {
            return this->_block_it;
        }

        struct block_buffer_end_sentinel
        {
        };

        bool operator==(block_buffer_end_sentinel /* sentinel */)
        {
            return this->_next == this->_end &&                   // If there is no block left to read
                   this->_block_it == this->_block_decoder.end(); // And if we fully read the block we're -> end
        }
    };
} // namespace hedge::db