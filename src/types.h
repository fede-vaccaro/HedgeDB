#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <sys/types.h>
#include <uuid.h>

#include "error.hpp"
#include "key.h"
#include "overloaded.h"
#include "size_literals.h"
namespace hedge
{
    // key_t is a type performing short-string optimization up to 31 byte characters regardless of the STL implementation
    // Used in HedgeDB for representing keys
    using key_t = hedge::key<>;

    constexpr size_t MAX_KEY_LEN = 256; // TODO: copied, refactor code
    constexpr size_t MIN_KEY_LEN = 1;

    struct value_ptr_t
    {
    private:
        // Note: The most significant bit (MSB) of _offset is used as a 'deleted' flag.
        uint64_t _offset{};   ///< Byte offset within the value table file. MSB indicates a deletion operation.
        uint32_t _size{};     ///< Size of the value data in bytes (including any header).
        uint32_t _table_id{}; ///< Identifier of the value_table file containing the value.

    public:
        value_ptr_t() = default;
        value_ptr_t(const value_ptr_t&) = default;
        value_ptr_t(value_ptr_t&&) = default;

        static std::optional<value_ptr_t> try_from_span(std::span<const uint8_t> span)
        {
            if(span.size() != sizeof(value_ptr_t))
                return std::nullopt;

            value_ptr_t vp;
            std::memcpy(&vp, span.data(), sizeof(value_ptr_t));
            return vp;
        }

        value_ptr_t(uint64_t offset, uint32_t size, uint32_t table_id) : _offset(offset), _size(size), _table_id(table_id) {}

        value_ptr_t& operator=(const value_ptr_t&) = default;
        value_ptr_t& operator=(value_ptr_t&&) = default;

        ~value_ptr_t() = default;

        operator std::span<const uint8_t>() const
        {
            return {reinterpret_cast<const uint8_t*>(this), sizeof(value_ptr_t)};
        }

        [[nodiscard]] bool is_deleted() const
        {
            return (this->_offset >> 63) == 1;
        }

        [[nodiscard]] uint64_t offset() const
        {
            // Define a mask where only the MSB (bit 63) is set to 1.
            constexpr uint64_t deleted_mask = (1ULL << 63);
            // Use bitwise AND with the inverted mask (~deleted_mask has MSB=0, others=1)
            // to clear the MSB, effectively removing the flag.
            return this->_offset & ~deleted_mask;
        }

        [[nodiscard]] uint32_t size() const
        {
            return this->_size;
        }

        [[nodiscard]] uint32_t table_id() const
        {
            return this->_table_id;
        }

        static value_ptr_t apply_delete(value_ptr_t value_ptr)
        {
            constexpr uint64_t deleted_mask = (1ULL << 63);
            value_ptr._offset |= deleted_mask;
            return value_ptr;
        }

        bool operator<(const value_ptr_t& other) const
        {
            // Higher table_id means newer, thus higher priority (lower value according to operator<)
            if(this->table_id() > other.table_id())
                return true;

            // If table_ids are the same, higher offset means newer, thus higher priority
            if(this->offset() > other.offset())
                return true;

            // If table_id and offset are the same, a tombstone (deleted) has higher priority
            // over a non-deleted entry. `is_deleted()` returns true (1) for deleted, false (0) for non-deleted.
            // `true > false`, so `this->is_deleted() > other.is_deleted()` means `this` is deleted and `other` is not.
            // We want the deleted entry to have higher priority, so '<' should return true if `this` is deleted and `other` is not.
            return this->is_deleted() && !other.is_deleted();
        }

        bool operator==(const value_ptr_t& other) const = default;
    };

    struct tombstone_t
    {
    };

    using value_t = std::variant<value_ptr_t, std::vector<uint8_t>, tombstone_t>;

    static constexpr auto sizeof_value_t = sizeof(value_t);

    enum class value_type : uint8_t
    {
        VALUE_PTR = 0,
        IN_PLACE_VALUE = 1,
        TOMBSTONE = 2,
        UNDEFINED = 255,
    };

    inline hedge::expected<value_t> value_from_span(std::span<const uint8_t> span)
    {
        const auto type = static_cast<value_type>(*span.data());
        switch(type)
        {
            case value_type::VALUE_PTR:
            {
                if(span.size() != 1 + sizeof(value_ptr_t))
                    return hedge::error("Invalid span size for value_ptr_t");
                value_ptr_t vp;
                std::memcpy(&vp, span.data() + 1, sizeof(value_ptr_t));
                return vp;
            }
            case value_type::IN_PLACE_VALUE:
            {
                return std::vector<uint8_t>(span.data() + 1, span.data() + span.size());
            }
            case value_type::TOMBSTONE:
            {
                if(span.size() != 1)
                    return hedge::error("Invalid span size for tombstone_t");
                return tombstone_t{};
            }
            default:
                return hedge::error("Invalid value type");
        }
    }

    inline std::span<const uint8_t> value_to_span(const value_t& value)
    {
        return std::visit(
            overloaded{[](const value_ptr_t& vp) -> std::span<const uint8_t>
                       {
                           return std::span<const uint8_t>{reinterpret_cast<const uint8_t*>(&vp), sizeof(value_ptr_t)};
                       },
                       [](const std::vector<uint8_t>& vec) -> std::span<const uint8_t>
                       {
                           return std::span<const uint8_t>{vec};
                       },
                       [](const tombstone_t&) -> std::span<const uint8_t>
                       {
                           static const auto tombstone_marker = static_cast<uint8_t>(value_type::TOMBSTONE);
                           return std::span<const uint8_t>{&tombstone_marker, 1};
                       }},
            value);
    }

    // Standard page size used for I/O operations (4 KiB).
    constexpr size_t PAGE_SIZE_IN_BYTES = 4 * KiB;

} // namespace hedge