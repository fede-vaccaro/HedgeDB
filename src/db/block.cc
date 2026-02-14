#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/types.h>
#include <type_traits>

#include "block.h"
#include "db/varint.h"
#include "error.hpp"

namespace hedge::db
{

    block_builder::block_builder(const block_config& cfg, uint8_t* base)
        : _cfg(cfg),
          _base(base),
          _head(base),
          _bytes_written(sizeof(block_footer))
    {
        constexpr size_t INITIAL_OFFSETS_CAPACITY = 16;
        this->_offsets.reserve(INITIAL_OFFSETS_CAPACITY);
    }

    hedge::status block_builder::push(std::span<const uint8_t> key, std::span<const uint8_t> value)
    {
        if(key.size() > block_builder::MAX_KEY_SIZE_BYTES || key.size() == 0)
            return hedge::error("invalid key size");

        if((this->_item_written_count & (this->_cfg.restart_group_size - 1)) != 0) [[likely]]
            return this->_push_as_delta_key(key, value);

        return this->_push_as_restart_key(key, value);
    }

    uint8_t _encode_key_size(size_t key_size)
    {
        return static_cast<uint8_t>(key_size - 1);
    }

    size_t _decode_key_size(uint8_t encoded_key_size)
    {
        return static_cast<size_t>(encoded_key_size) + 1;
    }

    template <typename T>
        requires std::is_unsigned_v<T>
    void _write_unsafe(T v, uint8_t** ptr)
    {
        **ptr = v;
        *ptr += sizeof(T);
    }

    void _write_unsafe(std::span<const uint8_t> v, uint8_t** ptr)
    {
        std::ranges::copy(v, *ptr);
        *ptr += v.size();
    }

    template <typename T>
        requires std::is_unsigned_v<T>
    void _write_as_varint_unsafe(T v, uint8_t** ptr)
    {
        auto* updated = unsafe_varint(v, *ptr);
        *ptr = updated;
    }

    void _write_unsafe(uint8_t* src, size_t count, uint8_t** dest)
    {
        std::memcpy(*dest, src, count);
        *dest += count;
    }

    hedge::status block_builder::_push_as_restart_key(std::span<const uint8_t> key, std::span<const uint8_t> value)
    {
        // Compute space needed
        const size_t space_needed =
            sizeof(uint8_t) +             // Key length field
            key.size() +                  // Actual key
            varint_length(value.size()) + // Value length field
            value.size();                 // Actual value

        constexpr size_t SIZEOF_OFFSET = sizeof(uint16_t);

        if(this->_bytes_written + space_needed + SIZEOF_OFFSET > this->_cfg.block_size_in_bytes)
        {
            this->_commit();
            return hedge::error("", errc::BUFFER_FULL);
        }

        // Update state
        this->_last_key.assign(key.begin(), key.end());
        this->_restart_keys_written++;
        this->_item_written_count++;

        // The offset will be written at the end of the block, but we need to account for it now to check if we have space.
        this->_bytes_written += space_needed + SIZEOF_OFFSET;

        [[maybe_unused]] auto* start_ptr = this->_head;

        this->_offsets.emplace_back(static_cast<uint16_t>(std::distance(this->_base, this->_head)));

        // Write Key length size
        _write_unsafe(_encode_key_size(key.size()), &this->_head);

        // Write Key
        _write_unsafe(key, &this->_head);

        // Write value length
        _write_as_varint_unsafe(value.size(), &this->_head);

        // Write value
        _write_unsafe(value, &this->_head);

        assert(static_cast<size_t>(this->_head - start_ptr) == space_needed);

        return hedge::ok();
    }

    hedge::status block_builder::_push_as_delta_key(std::span<const uint8_t> key, std::span<const uint8_t> value)
    {
        uint32_t shared_prefix_length = block_builder::_shared_prefix_length(this->_last_key, key);

        // Compute space needed
        const size_t space_needed =
            sizeof(uint8_t) +                   // Prefix length
            sizeof(uint8_t) +                   // Delta length
            key.size() - shared_prefix_length + // Delta Key
            varint_length(value.size()) +       // Value length field
            value.size();                       // Actual value

        if(this->_bytes_written + space_needed > this->_cfg.block_size_in_bytes)
        {
            this->_commit();
            return hedge::error("", errc::BUFFER_FULL);
        }

        this->_last_key.assign(key.begin(), key.end());
        this->_item_written_count++;
        this->_bytes_written += space_needed;

        [[maybe_unused]] auto* start_ptr = this->_head;

        // Shared Prefix length
        _write_unsafe<uint8_t>((uint8_t)shared_prefix_length, &this->_head);

        // Delta length
        _write_unsafe<uint8_t>((uint8_t)(key.size() - shared_prefix_length), &this->_head);

        // Write Key
        std::span<const uint8_t> suffix{key.begin() + shared_prefix_length, key.end()};
        _write_unsafe(suffix, &this->_head);

        // Write value length
        _write_as_varint_unsafe(value.size(), &this->_head);

        // Write value
        _write_unsafe(value, &this->_head);

        assert(static_cast<size_t>(this->_head - start_ptr) == space_needed);

        return hedge::ok();
    }

    // Write the lookup-section and footer
    void block_builder::_commit()
    {
        // Layout at the end is
        // [ Restart Offset 1 (2B) ]
        // [ Restart Offset 0 (2B) ]
        // [ Restart Offset   (2B) ]
        // [ Restart Offset M (2B) ]
        // [ Restart Count    (2B) ]
        // [ Footer Metadata  (8B) ]

        // this->_bytes_written count won't be increased as this space
        // has already been taken in account during insertion

        this->_footer.item_count = this->_item_written_count;

        // Write restart point offsets
        auto* end = this->_base + this->_cfg.block_size_in_bytes;
        this->_head = end -
                      (this->_offsets.size() * sizeof(uint16_t)) - // Restart offsets
                      sizeof(uint16_t) -                           // Restart count
                      sizeof(block_footer);                        // Footer

        // Copy restart offsets
        _write_unsafe(reinterpret_cast<uint8_t*>(this->_offsets.data()), this->_offsets.size() * sizeof(uint16_t), &this->_head);

        // Copy restart count
        _write_unsafe<uint16_t>(this->_offsets.size(), &this->_head);

        // Copy footer
        _write_unsafe(reinterpret_cast<uint8_t*>(&this->_footer), sizeof(block_footer), &this->_head);

        assert(this->_head == this->_base + this->_cfg.block_size_in_bytes);
    }

    void block_builder::reset(uint8_t* new_base)
    {
        this->_base = this->_head = new_base;
        this->_bytes_written = block_builder::FOOTER_SIZE;
        this->_last_key.clear();
        this->_offsets.clear();
        this->_item_written_count = 0;
        this->_restart_keys_written = 0;
        this->_footer = {};
    }

    block_iterator::block_iterator(uint8_t* base, size_t block_size, uint32_t restart_group_size)
        : _head(base),
          _block_size(block_size),
          _restart_group_size(restart_group_size)
    {
        this->_read_next();
    }

    block_iterator& block_iterator::operator++()
    {
        this->_read_next();

        return *this;
    }

    void block_iterator::_read_next()
    {
        if((this->_curr_item_idx & (this->_restart_group_size - 1)) == 0) [[unlikely]]
            this->_read_restart_key();
        else
            this->_read_delta_key();

        this->_curr_item_idx++;
    }

    std::span<const uint8_t> block_iterator::key()
    {
        return this->_key;
    }

    std::span<const uint8_t> block_iterator::value()
    {
        return this->_value_ref;
    }

    void block_iterator::_read_restart_key()
    {
        // Check key whole size
        size_t key_size = _decode_key_size(*this->_head);
        this->_head += 1;

        // Read key
        this->_key.assign(this->_head, this->_head + key_size);
        this->_head += key_size;

        // Decode value size
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value_size =
            try_decode_varint(std::span<const uint8_t>(this->_head, MAX_VARINT_LENGTH_32));
        assert(decoded_value_size.has_value()); // Expected to be ok

        auto& [value_size, size_of_varint_value_size] = decoded_value_size.value();
        this->_head += size_of_varint_value_size;

        // Assign valure ref
        this->_value_ref = std::span<const uint8_t>(this->_head, value_size);
        this->_head += value_size;
    }

    void block_iterator::_read_delta_key()
    {
        // Check prefix length
        size_t shared_prefix_length = *this->_head;
        this->_head += 1;

        // Check delta key length
        size_t delta_key_length = *this->_head;
        this->_head += 1;

        // Read delta key
        this->_key.resize(shared_prefix_length);
        this->_key.insert(this->_key.end(), this->_head, this->_head + delta_key_length);
        this->_head += delta_key_length;

        // Decode value size
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value_size =
            try_decode_varint(std::span<const uint8_t>(this->_head, MAX_VARINT_LENGTH_32));
        assert(decoded_value_size.has_value()); // Expected to be ok

        auto& [value_size, size_of_varint_value_size] = decoded_value_size.value();
        this->_head += size_of_varint_value_size;

        // Assign valure ref
        this->_value_ref = std::span<const uint8_t>(this->_head, value_size);
        this->_head += value_size;
    }

    bool block_iterator::operator==(const block_iterator_sentinel& sentinel) const
    {
        return this->_curr_item_idx > sentinel._items_in_block_count;
    }

    bool block_iterator::operator==(const block_iterator_restart_group_sentinel& sentinel) const
    {
        return this->_curr_item_idx > this->_restart_group_size ||
               this->_curr_item_idx > sentinel._items_in_block_count;
    }

    block_reader::block_reader(const block_config& cfg, uint8_t* base)
        : _cfg(cfg),
          _base(base)
    {
        // read footer
        std::memcpy(&this->_footer, base + cfg.block_size_in_bytes - sizeof(block_footer), sizeof(block_footer));
    }

    bool key_comparator(std::span<const uint8_t> a, std::span<const uint8_t> b)
    {
        return std::ranges::lexicographical_compare(a, b);
    }

    std::vector<uint8_t> block_reader::find(std::span<const uint8_t> key)
    {
        // std::cout << "Finding key: " << std::string(key.begin(), key.end()) << std::endl;

        // Load offsets
        auto* end = this->_base + this->_cfg.block_size_in_bytes;
        auto* restart_count_ptr = end - sizeof(block_footer) - sizeof(uint16_t);
        size_t restart_count = *reinterpret_cast<uint16_t*>(restart_count_ptr);
        auto* offsets_base = restart_count_ptr - (restart_count * sizeof(uint16_t));

        std::vector<uint16_t> offsets;
        offsets.assign(reinterpret_cast<uint16_t*>(offsets_base), reinterpret_cast<uint16_t*>(restart_count_ptr));

        // Find the right Restart Point with binary search

        std::vector<uint8_t> key_buffer;

        auto offset_it = std::upper_bound( // NOLINT
            offsets.begin(),
            offsets.end(),
            key,
            [&](std::span<const uint8_t> key, uint16_t offset)
            {
                // Read key at offset
                size_t key_size = _decode_key_size(*(this->_base + offset));
                key_buffer.assign(this->_base + offset + 1, this->_base + offset + 1 + key_size);

                // std::cout << "Comparing with restart key: " << std::string(key_buffer.begin(), key_buffer.end()) << std::endl;

                return key_comparator(key, key_buffer);
            });

        // Key is greater than all restart keys, so it can't be in the block
        if(offset_it == offsets.begin())
            return {};

        offset_it--; // Move back to the correct restart point
        uint16_t offset = *offset_it;

        block_iterator it(this->_base + offset, this->_cfg.block_size_in_bytes - offset, this->_cfg.restart_group_size);

        size_t skipped_items = std::distance(offsets.begin(), offset_it) * this->_cfg.restart_group_size;
        block_iterator_restart_group_sentinel restart_group_sentinel(this->_footer.item_count - skipped_items);

        for(; it != restart_group_sentinel; ++it)
        {
            // std::cout << "Comparing with key: " << std::string(it.key().begin(), it.key().end()) << std::endl;

            if(std::ranges::equal(it.key(), key))
                return {it.value().begin(), it.value().end()};
        }

        return {};
    }

    block_iterator block_reader::begin() const
    {
        return {this->_base,
                this->_cfg.block_size_in_bytes,
                static_cast<uint32_t>(this->_cfg.restart_group_size)};
    }

    block_iterator_sentinel block_reader::end() const
    {
        block_iterator_sentinel sentinel{static_cast<uint32_t>(this->_footer.item_count)};
        return sentinel;
    }

} // namespace hedge::db