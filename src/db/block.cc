#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <sys/types.h>
#include <type_traits>

#include <xxh64.hpp>

#include "block.h"
#include "db/varint.h"
#include "error.hpp"
namespace hedge::db
{

    block_encoder::block_encoder(std::byte* base, const block_config& cfg)
        : _cfg(cfg),
          _base(base),
          _head(base),
          _bytes_written(sizeof(block_footer) + sizeof(uint16_t)) // Reserve space for footer and offsets count
    {
        constexpr size_t INITIAL_OFFSETS_CAPACITY = 16;
        this->_offsets.reserve(INITIAL_OFFSETS_CAPACITY);
    }

    hedge::status block_encoder::push(std::span<const std::byte> key, std::span<const std::byte> value)
    {
        if(key.size() > block_encoder::MAX_KEY_LEN || key.size() == 0)
            return hedge::error("invalid key size");

        if((this->_kvs_count & (this->_cfg.restart_group_size - 1)) != 0) [[likely]]
            return this->_push_as_delta_key(key, value);

        return this->_push_as_restart_key(key, value);
    }

    template <typename T>
        requires std::is_unsigned_v<T>
    void _write_unsafe(T v, std::byte** ptr)
    {
        // Check alignment
        assert((uint64_t)&v % std::alignment_of<T>::value == 0);
        *reinterpret_cast<T*>(*ptr) = v;
        *ptr += sizeof(T);
    }

    void _write_unsafe(std::span<const std::byte> v, std::byte** ptr)
    {
        std::ranges::copy(v, *ptr);
        *ptr += v.size();
    }

    template <typename T>
        requires std::is_unsigned_v<T>
    void _write_as_varint_unsafe(T v, std::byte** ptr)
    {
        auto* updated = unsafe_varint(v, *ptr);
        *ptr = updated;
    }

    void _write_unsafe(const std::byte* src, size_t count, std::byte** dest)
    {
        std::memcpy(*dest, src, count);
        *dest += count;
    }

    uint32_t _checksum(const std::byte* start, size_t n)
    {
        uint64_t checksum64 = xxh64::hash(reinterpret_cast<const char*>(start), n, BLOCK_CHECKSUM_SEED);
        constexpr uint64_t UINT32_MASK = std::numeric_limits<uint32_t>::max(); // Keep the bottom 32 bits
        return static_cast<uint32_t>(checksum64 & UINT32_MASK);
    }

    hedge::status block_encoder::_push_as_restart_key(std::span<const std::byte> key, std::span<const std::byte> value)
    {
        // Compute space needed
        const size_t space_needed =
            sizeof(std::byte) +           // Key length field
            key.size() +                  // Actual key
            varint_length(value.size()) + // Value length field
            value.size();                 // Actual value

        constexpr size_t SIZE_OF_OFFSET = sizeof(uint16_t);

        if(this->_bytes_written + space_needed + SIZE_OF_OFFSET > this->_cfg.block_size_in_bytes)
        {
            this->_commit();
            return hedge::error("", errc::BUFFER_FULL);
        }

        // Update state
        this->_last_key.assign(key.begin(), key.end());
        this->_restart_keys_written++;
        this->_kvs_count++;

        // The offset will be written at the end of the block, but we need to account for it now to check if we have space.
        this->_bytes_written += space_needed + SIZE_OF_OFFSET;

        [[maybe_unused]] auto* start_ptr = this->_head;

        this->_offsets.emplace_back(static_cast<uint16_t>(std::distance(this->_base, this->_head)));

        // Write Key length size
        _write_unsafe(encode_key_size(key.size()), &this->_head);

        // Write Key
        _write_unsafe(key, &this->_head);

        // Write value length
        _write_as_varint_unsafe(value.size(), &this->_head);

        // Write value
        _write_unsafe(value, &this->_head);

        assert(static_cast<size_t>(this->_head - start_ptr) == space_needed);

        return hedge::ok();
    }

    hedge::status block_encoder::_push_as_delta_key(std::span<const std::byte> key, std::span<const std::byte> value)
    {
        uint32_t shared_prefix_length = block_encoder::_compute_shared_prefix_length(this->_last_key, key);

        // Compute space needed
        const size_t space_needed =
            sizeof(std::byte) +                 // Prefix length
            sizeof(std::byte) +                 // Delta length
            key.size() - shared_prefix_length + // Delta Key
            varint_length(value.size()) +       // Value length field
            value.size();                       // Actual value

        if(this->_bytes_written + space_needed > this->_cfg.block_size_in_bytes)
        {
            this->_commit();
            return hedge::error("", errc::BUFFER_FULL);
        }

        this->_last_key.assign(key.begin(), key.end());
        this->_kvs_count++;
        this->_bytes_written += space_needed;

        [[maybe_unused]] auto* start_ptr = this->_head;

        // Shared Prefix length
        _write_unsafe<uint8_t>(static_cast<uint8_t>(shared_prefix_length), &this->_head);

        // Delta length
        _write_unsafe<uint8_t>(static_cast<uint8_t>(key.size() - shared_prefix_length), &this->_head);

        // Write Key
        std::span<const std::byte> suffix{key.begin() + shared_prefix_length, key.end()};
        _write_unsafe(suffix, &this->_head);

        // Write value length
        _write_as_varint_unsafe(value.size(), &this->_head);

        // Write value
        _write_unsafe(value, &this->_head);

        assert(static_cast<size_t>(this->_head - start_ptr) == space_needed);

        return hedge::ok();
    }

    // Write the lookup-section and footer
    void block_encoder::_commit()
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

        // Write restart point offsets
        auto* end = this->_base + this->_cfg.block_size_in_bytes;
        this->_head = end -
                      (this->_offsets.size() * sizeof(uint16_t)) - // Restart offsets
                      sizeof(uint16_t) -                           // Restart count
                      sizeof(block_footer);                        // Footer

        // Copy restart offsets
        _write_unsafe(reinterpret_cast<std::byte*>(this->_offsets.data()), this->_offsets.size() * sizeof(uint16_t), &this->_head);

        // Copy restart count
        _write_unsafe<uint16_t>(this->_offsets.size(), &this->_head);

        // Set footer
        this->_footer.kv_count = this->_kvs_count;

        // Copy footer (checksum escluded)
        _write_unsafe(reinterpret_cast<std::byte*>(&this->_footer), sizeof(block_footer) - sizeof(block_footer::checksum), &this->_head);

        // Compute Checksum
        const uint32_t checksum = _checksum(
            this->_base,
            this->_cfg.block_size_in_bytes - sizeof(block_footer::checksum));

        // Write checksum
        _write_unsafe(reinterpret_cast<const std::byte*>(&checksum), sizeof(uint32_t), &this->_head);

        this->_committed = true;
        assert(this->_head == this->_base + this->_cfg.block_size_in_bytes);
    }

    void block_encoder::reset(std::byte* new_base)
    {
        this->_base = new_base;
        this->_head = new_base;
        this->_bytes_written = block_encoder::FOOTER_SIZE + sizeof(uint16_t); // Reserve space for footer and offsets count
        this->_last_key.clear();
        this->_offsets.clear();
        this->_kvs_count = 0;
        this->_restart_keys_written = 0;
        this->_footer = {};
        this->_committed = false;
    }

    block_iterator::block_iterator(const std::byte* base,
                                   uint32_t starting_kv_idx,
                                   uint32_t _kvs_in_block,
                                   uint32_t restart_group_size)
        : _head(base),
          _kv_idx(starting_kv_idx),
          _starting_kv_idx(starting_kv_idx),
          _kvs_in_block_count(_kvs_in_block),
          _restart_group_size(restart_group_size)
    {
        this->_key.reserve(block_encoder::MAX_KEY_LEN * 2);
        this->_read_next();
    }

    block_iterator& block_iterator::operator++()
    {
        this->_read_next();

        return *this;
    }

    void block_iterator::_read_next()
    {
        size_t curr_idx = this->_kv_idx++;

        if(curr_idx >= this->_kvs_in_block_count)
            return;

        if((curr_idx & (this->_restart_group_size - 1)) == 0) [[unlikely]]
            this->_read_restart_key();
        else
            this->_read_delta_key();
    }

    std::span<const std::byte> block_iterator::key() const
    {
        return this->_key;
    }

    std::span<const std::byte> block_iterator::value() const
    {
        return this->_value_ref;
    }

    void block_iterator::_read_restart_key()
    {
        // Check key whole size
        size_t key_size = decode_key_size(*this->_head);
        this->_head += 1;

        // Read key
        this->_key.assign(this->_head, this->_head + key_size);
        this->_head += key_size;

        // Decode value size
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value_size =
            try_decode_varint(std::span<const std::byte>(this->_head, MAX_VARINT_LENGTH_32));
        assert(decoded_value_size.has_value()); // Expected to be ok

        auto& [value_size, size_of_varint_value_size] = decoded_value_size.value();
        this->_head += size_of_varint_value_size;

        // Assign valure ref
        this->_value_ref = std::span<const std::byte>(this->_head, value_size);
        this->_head += value_size;

        // std::cout << "Read restart key: " << to_hex_string(this->_key) << std::endl;
    }

    void block_iterator::_read_delta_key()
    {
        // Check prefix length
        size_t shared_prefix_length = static_cast<size_t>(*this->_head);
        this->_head += 1;

        // Check delta key length
        size_t delta_key_length = static_cast<size_t>(*this->_head);
        this->_head += 1;

        // Read delta key
        this->_key.resize(shared_prefix_length);
        this->_key.insert(this->_key.end(), this->_head, this->_head + delta_key_length);
        this->_head += delta_key_length;

        // Decode value size
        hedge::expected<std::pair<uint64_t, size_t>> decoded_value_size =
            try_decode_varint(std::span<const std::byte>(this->_head, MAX_VARINT_LENGTH_32));
        assert(decoded_value_size.has_value()); // Expected to be ok

        auto& [value_size, size_of_varint_value_size] = decoded_value_size.value();
        this->_head += size_of_varint_value_size;

        // Assign valure ref
        this->_value_ref = std::span<const std::byte>(this->_head, value_size);
        this->_head += value_size;

        // std::cout << "Read delta key: " << to_hex_string(this->_key) << std::endl;
    }

    bool block_iterator::operator==(block_iterator_sentinel /* sentinel */) const
    {
        return this->_kv_idx > this->_kvs_in_block_count;
    }

    bool block_iterator::operator==(block_iterator_restart_group_sentinel /* sentinel */) const
    {
        return this->_kv_idx > (this->_starting_kv_idx + this->_restart_group_size) ||
               this->_kv_idx > this->_kvs_in_block_count;
    }

    block_decoder::block_decoder(const std::byte* base, const block_config& cfg)
        : _cfg(cfg),
          _base(base)
    {
        this->_read_footer();
    }

    void block_decoder::reset(const std::byte* new_base)
    {
        this->_base = new_base;
        this->_read_footer();
    }

    hedge::status block_decoder::sanity_check() const
    {
        const uint32_t checksum = _checksum(
            this->_base,
            this->_cfg.block_size_in_bytes - sizeof(block_footer::checksum));

        if(checksum != this->_footer.checksum)
            return hedge::error("checksum mismatch sum: " + std::to_string(checksum) + " footer: " + std::to_string(this->_footer.checksum));

        return hedge::ok();
    }

    void block_decoder::_read_footer()
    {
        if(this->_base == nullptr)
        {
            this->_footer = {};
            return;
        }

        std::memcpy(&this->_footer, this->_base + this->_cfg.block_size_in_bytes - sizeof(block_footer), sizeof(block_footer));
    }

    bool key_comparator(std::span<const std::byte> a, std::span<const std::byte> b)
    {
        return std::ranges::lexicographical_compare(a, b);
    }

    std::span<const std::byte> block_decoder::find(std::span<const std::byte> key)
    {
        // std::cout << "Finding key: " << to_hex_string(key) << std::endl;

        // Load offsets
        const auto* end = this->_base + this->_cfg.block_size_in_bytes;
        const auto* restart_count_ptr = end - sizeof(block_footer) - sizeof(uint16_t);
        size_t restart_count = *reinterpret_cast<const uint16_t*>(restart_count_ptr);
        const auto* offsets_base = restart_count_ptr - (restart_count * sizeof(uint16_t));

        inline_vector<uint16_t> offsets;
        offsets.assign(reinterpret_cast<const uint16_t*>(offsets_base), reinterpret_cast<const uint16_t*>(restart_count_ptr));

        // Find the right Restart Point with binary search

        inline_vector<std::byte> key_buffer; // TODO: std::vector should be backed from a stack allocated buffer

        auto offset_it = std::upper_bound( // NOLINT
            offsets.begin(),
            offsets.end(),
            key,
            [&](std::span<const std::byte> key, uint16_t offset)
            {
                // Read key at offset
                const std::byte encoded_key_size = *(this->_base + offset);
                size_t key_size = decode_key_size(encoded_key_size);
                key_buffer.assign(this->_base + offset + 1, this->_base + offset + 1 + key_size);

                // std::cout << "Comparing with restart key: " << to_hex_string(key_buffer) << std::endl;

                return key_comparator(key, key_buffer);
            });

        // Key is greater than all restart keys, so it can't be in the block
        if(offset_it == offsets.begin())
            return {};

        offset_it--; // Move back to the correct restart point

        uint16_t offset = *offset_it;
        size_t skipped_kvs = std::distance(offsets.begin(), offset_it) * this->_cfg.restart_group_size;

        block_iterator it(
            this->_base + offset,
            skipped_kvs,
            this->_footer.kv_count,
            this->_cfg.restart_group_size);

        block_iterator_restart_group_sentinel restart_group_sentinel{};

        for(; it != restart_group_sentinel; ++it)
        {
            // std::cout << "Comparing with key: " << std::string(it.key().begin(), it.key().end()) << std::endl;

            if(std::ranges::equal(it.key(), key))
                return {it.value().begin(), it.value().end()};
        }

        return {};
    }

    block_iterator block_decoder::begin() const
    {
        return {this->_base,
                0,
                this->_footer.kv_count,
                static_cast<uint32_t>(this->_cfg.restart_group_size)};
    }

    block_iterator_sentinel block_decoder::end() const
    {
        return {};
    }

} // namespace hedge::db