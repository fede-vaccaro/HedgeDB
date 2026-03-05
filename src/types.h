#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <sys/types.h>
#include <uuid.h>

#include "error.hpp"
#include "key.h"
#include "overloaded.h"
namespace hedge
{

    /**
     * @brief Defines the type used for keys in the database.
     * Currently uses universally unique identifiers (UUIDs).
     * Will move to raw bytes in future versions for portability.
     */
    using uuid_t = uuids::uuid;

    using key_t = hedge::key<>;

    constexpr size_t MAX_KEY_LEN = 256; // TODO: copied, refactor code
    constexpr size_t MIN_KEY_LEN = 1;

    /**
     * @brief Represents a pointer to a value stored in a value_table file.
     * @details This compact struct holds the necessary information to locate
     * and retrieve a value: the offset within a specific value table file,
     * the size of the value, and the ID of the value table file.
     * It also encodes a "deleted" flag within the offset field for space efficiency.
     * Size is 16 bytes total.
     */
    struct value_ptr_t
    {
    private:
        // Note: The most significant bit (MSB) of _offset is used as a 'deleted' flag.
        uint64_t _offset{};   ///< Byte offset within the value table file. MSB indicates a deletion operation.
        uint32_t _size{};     ///< Size of the value data in bytes (including any header).
        uint32_t _table_id{}; ///< Identifier of the value_table file containing the value.

    public:
        /** @brief Default constructor. Initializes to zero/default values. */
        value_ptr_t() = default;
        /** @brief Copy constructor. */
        value_ptr_t(const value_ptr_t&) = default;
        /** @brief Move constructor. */
        value_ptr_t(value_ptr_t&&) = default;

        static std::optional<value_ptr_t> try_from_span(std::span<const uint8_t> span)
        {
            if(span.size() != sizeof(value_ptr_t))
                return std::nullopt;

            value_ptr_t vp;
            std::memcpy(&vp, span.data(), sizeof(value_ptr_t));
            return vp;
        }

        /**
         * @brief Constructs a value_ptr_t with specified offset, size, and table ID.
         * @param offset The byte offset within the value table.
         * @param size The size of the value data in bytes.
         * @param table_id The ID of the value table.
         */
        value_ptr_t(uint64_t offset, uint32_t size, uint32_t table_id) : _offset(offset), _size(size), _table_id(table_id) {}

        /** @brief Copy assignment operator. */
        value_ptr_t& operator=(const value_ptr_t&) = default;
        /** @brief Move assignment operator. */
        value_ptr_t& operator=(value_ptr_t&&) = default;

        /** @brief Default destructor. */
        ~value_ptr_t() = default;

        operator std::span<const uint8_t>() const
        {
            return {reinterpret_cast<const uint8_t*>(this), sizeof(value_ptr_t)};
        }

        /**
         * @brief Checks if the value pointer is marked as deleted.
         * @details This function checks the most significant bit (MSB) of the `_offset` field.
         * A '1' in the MSB indicates that the entry associated with this pointer is considered deleted (tombstone).
         * The implementation uses preprocessor directives to handle potential endianness differences,
         * @return `true` if the entry is marked as deleted, `false` otherwise.
         */
        [[nodiscard]] bool is_deleted() const
        {
            return (this->_offset >> 63) == 1;
        }

        /**
         * @brief Gets the actual byte offset of the value, excluding the deleted flag.
         * @details This function masks out the most significant bit (MSB) which is used
         * as the deleted flag, returning only the valid offset bits (0-62).
         * @return The 63-bit byte offset within the value table file.
         */
        [[nodiscard]] uint64_t offset() const
        {
            // Define a mask where only the MSB (bit 63) is set to 1.
            constexpr uint64_t deleted_mask = (1ULL << 63);
            // Use bitwise AND with the inverted mask (~deleted_mask has MSB=0, others=1)
            // to clear the MSB, effectively removing the flag.
            return this->_offset & ~deleted_mask;
        }

        /**
         * @brief Gets the size of the value data in bytes.
         * @return The 32-bit size.
         */
        [[nodiscard]] uint32_t size() const
        {
            return this->_size;
        }

        /**
         * @brief Gets the ID of the value table file where the value is stored.
         * @return The 32-bit table ID.
         */
        [[nodiscard]] uint32_t table_id() const
        {
            return this->_table_id;
        }

        /**
         * @brief Creates a new value_ptr_t instance with the deleted flag set.
         * @details Takes an existing value_ptr_t and sets its most significant bit (MSB)
         * in the offset field to mark it as deleted.
         * @param value_ptr The original value_ptr_t.
         * @return A new value_ptr_t instance with the deleted flag set.
         */
        static value_ptr_t apply_delete(value_ptr_t value_ptr)
        {
            constexpr uint64_t deleted_mask = (1ULL << 63);
            value_ptr._offset |= deleted_mask;
            return value_ptr;
        }

        /**
         * @brief Defines the comparison operator used for determining precedence during merges.
         * @details This operator is crucial for compaction/merging. It defines which `value_ptr_t`
         * (and therefore which version of a key-value pair) should be kept when duplicates are encountered.
         * "Lower" means higher priority (i.e., the one that should be kept).
         * Priority order:
         * 1. Higher `table_id` (newer file) has higher priority, because table_id is always monotonically increasing.
         * 2. If `table_id` is the same, higher `offset` (written later within the same file) has higher priority.
         * 3. If `table_id` and `offset` are the same, a deleted entry (`is_deleted() == true`) has higher priority
         * than a non-deleted one (ensuring deletes override existing values).
         * Also, if a value is re-inserted after deletion, the new entry will have a higher offset,
         * thus correctly taking precedence over the tombstone.
         * @param other The other value_ptr_t to compare against.
         * @return `true` if `this` pointer has higher priority (should be kept) than `other`, `false` otherwise.
         */
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

    /**
     * @brief Represents a single entry in an index file (mem_index or sorted_index).
     * @details Pairs a key (`key_t`) with a pointer (`value_ptr_t`) to its corresponding
     * value's location in a value table.
     */
    struct index_entry_t
    {
        uuid_t key{};            ///< The key (UUID).
        value_ptr_t value_ptr{}; ///< The pointer to the value's location and status.

        /**
         * @brief Comparison operator based solely on the key.
         * @details Used for sorting index entries within memtables and sorted_index files.
         * @param other The other index_entry_t to compare against.
         * @return `true` if `this->key` is less than `other.key`, `false` otherwise.
         */
        bool operator<(const index_entry_t& other) const
        {
            return key < other.key;
        }

        // Default comparison for equality (needed for some algorithms if used)
        bool operator==(const index_entry_t& other) const = default;
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

    struct index_entry2_t
    {

        key_t key{};                      ///< The key (UUID).
        std::span<const uint8_t> value{}; ///< The pointer to the value's location and status.

        ~index_entry2_t() = default;

        /**
         * @brief Comparison operator based solely on the key.
         * @details Used for sorting index entries within memtables and sorted_index files.
         * @param other The other index_entry_t to compare against.
         * @return `true` if `this->key` is less than `other.key`, `false` otherwise.
         */
        bool operator<(const index_entry2_t& other) const
        {
            return key < other.key;
        }

        // Default comparison for equality (needed for some algorithms if used)
        bool operator==(const index_entry2_t& other) const
        {
            return key == other.key && std::ranges::equal(value, other.value);
        }
    };

    /** @brief Standard page size used for I/O operations (typically 4 KiB). */
    constexpr size_t PAGE_SIZE_IN_BYTES = 4096;

    /**
     * @brief Number of index entries that fit exactly into one standard page.
     * @details Calculated based on `PAGE_SIZE_IN_BYTES` and the size of `index_entry_t`.
     * This determines the granularity of the meta-index.
     * With a 4 KiB page and the current size of `index_entry_t` (32 bytes),
     * this results in 128 entries per page.
     */
    constexpr size_t INDEX_PAGE_NUM_ENTRIES = PAGE_SIZE_IN_BYTES / sizeof(index_entry_t);

} // namespace hedge