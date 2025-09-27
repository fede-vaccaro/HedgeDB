#pragma once

#pragma once

#include <cassert>
#include <cstdint>
#include <uuid.h>

namespace hedge
{
    using key_t = uuids::uuid;

    struct value_ptr_t
    {
    private:
        uint64_t _offset{};
        uint32_t _size{};
        uint32_t _table_id{};

    public:
        value_ptr_t() = default;
        value_ptr_t(const value_ptr_t&) = default;
        value_ptr_t(value_ptr_t&&) = default;

        value_ptr_t(uint64_t offset, uint32_t size, uint32_t table_id) : _offset(offset), _size(size), _table_id(table_id) {}

        value_ptr_t& operator=(const value_ptr_t&) = default;
        value_ptr_t& operator=(value_ptr_t&&) = default;

        ~value_ptr_t() = default;

        [[nodiscard]] bool is_deleted() const
        {
#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            return (this->_offset >> 63) == 1;
#else
            return (this->_offset & 1) == 1;
#endif
#else
#error "Byte order not defined. Please define __BYTE_ORDER__."
#endif
        }

        [[nodiscard]] uint64_t offset() const
        {
            constexpr uint64_t deleted_mask = (1ULL << 63);
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
#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            constexpr uint64_t deleted_mask = (1ULL << 63);
#else
            constexpr uint64_t deleted_mask = 1UL;
#endif
#else
#error "Byte order not defined. Please define __BYTE_ORDER__."
#endif
            value_ptr._offset |= deleted_mask;

            return value_ptr;
        }

        /**
            Lower means highest "priority".
         */
        bool operator<(const value_ptr_t& other) const
        {
            return this->table_id() > other.table_id() || offset() > other.offset() || (this->is_deleted() && !other.is_deleted());
        }
    };

    struct index_entry_t
    {
        key_t key{};
        value_ptr_t value_ptr{};

        bool operator<(const index_entry_t& other) const
        {
            return key < other.key;
        }
    };

    constexpr size_t PAGE_SIZE_IN_BYTES = 4096;
    constexpr size_t INDEX_PAGE_NUM_ENTRIES = PAGE_SIZE_IN_BYTES / sizeof(index_entry_t);
} // namespace hedge