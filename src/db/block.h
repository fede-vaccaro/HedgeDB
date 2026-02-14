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
[ Entry  1: Shared Prefix Size + Delta Key Size + Value Size + Value ]
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

#include "types.h"

namespace hedge::db
{

    struct block_config
    {
        size_t block_size_in_bytes{PAGE_SIZE_IN_BYTES};
        size_t restart_group_size{16}; // Use power of 2
    };

    struct block_footer
    {
        uint16_t item_count{};
        std::array<uint8_t, 6> _footer_bytes; // Padding
    };

    class block_builder
    {
        friend struct BlockTest;

        static constexpr size_t MIN_KEY_SIZE_BYTES = 1;
        static constexpr size_t MAX_KEY_SIZE_BYTES = 256;
        static constexpr size_t FOOTER_SIZE = 8;

        block_config _cfg{};

        // Builder state
        uint8_t* _base{};
        uint8_t* _head{};
        std::vector<uint8_t> _last_key{};
        std::vector<uint16_t> _offsets;
        size_t _bytes_written{};
        uint32_t _item_written_count{};
        uint32_t _restart_keys_written{};

        block_footer _footer{}; // currently unused

    public:
        block_builder(const block_config& cfg, uint8_t* base);

        hedge::status push(std::span<const uint8_t> key, std::span<const uint8_t> value); // Returns error code BUFFER_FULL if could not push the key-value

        void reset(uint8_t* new_base);

        void finish()
        {
            this->_commit();
        }

    private:
        void _commit();
        hedge::status _push_as_restart_key(std::span<const uint8_t> key, std::span<const uint8_t> value);
        hedge::status _push_as_delta_key(std::span<const uint8_t> key, std::span<const uint8_t> value);

        static uint32_t _shared_prefix_length(std::span<const uint8_t> prev, std::span<const uint8_t> curr)
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
        friend class block_iterator;
        friend class block_reader;
        uint32_t _items_in_block_count{};
        block_iterator_sentinel(uint32_t items_in_block_count) : _items_in_block_count(items_in_block_count){};
    };

    class block_iterator_restart_group_sentinel
    {
        friend class block_iterator;
        friend class block_reader;
        uint32_t _items_in_block_count{};
        block_iterator_restart_group_sentinel(uint32_t _items_in_block_count) : _items_in_block_count(_items_in_block_count){};
    };

    class block_iterator
    {
        uint8_t* _head{nullptr};
        size_t _block_size{};
        uint32_t _restart_group_size{};

        std::vector<uint8_t> _key{};
        std::span<const uint8_t> _value_ref{};
        uint32_t _curr_item_idx{};

    public:
        block_iterator(uint8_t* base, size_t block_size, uint32_t restart_group_size);
        block_iterator() = default;

        block_iterator(block_iterator&& other) noexcept = default;
        block_iterator& operator=(block_iterator&& other) noexcept = default;

        block_iterator(const block_iterator& other) = default;
        block_iterator& operator=(const block_iterator& other) = default;

        block_iterator& operator++();

        std::span<const uint8_t> key();
        std::span<const uint8_t> value();

        bool operator==(const block_iterator_sentinel&) const;
        bool operator==(const block_iterator_restart_group_sentinel&) const;

    private:
        void _read_next();
        void _read_restart_key();
        void _read_delta_key();
    };

    class block_reader
    {
        block_config _cfg{};
        uint8_t* _base{};
        block_footer _footer{};

    public:
        block_reader(const block_config& cfg, uint8_t* base);
        std::vector<uint8_t> find(std::span<const uint8_t> key);

        [[nodiscard]] block_iterator begin() const;
        [[nodiscard]] block_iterator_sentinel end() const;
    };

} // namespace hedge::db