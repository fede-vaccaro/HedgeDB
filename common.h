#pragma once

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <type_traits>
#include <uuid.h>

namespace hedgehog
{
    using key_t = uuids::uuid;

    struct value_ptr_t
    {
    private:
        uint64_t _offset{};
        uint32_t _size{};
        uint32_t _table_id{};

    public:
        [[nodiscard]] bool is_deleted() const
        {
#ifdef __BYTE_ORDER__
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            return (_offset >> 63) == 1;
#else
            return (offset & 1) == 1;
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
            /**
            This might looks weird but it follows a design choice on how to implement updates and deletion.

            Every time we apply some change to a key-value, we put into the mem_index a new entry
            At the current time, the only allowed updates are key/value deletion and the update is possible when a value
            is moved to a new table when garbage-collection is occurring. However, through this way, we still anticipate the
            value update.

            Keep in mind that the table_id is strictly monotonic increasing by design. This implies that:

            - every time a value is moved to a table, the destination table has higher index (it checks the highest priority definition)
            - if we update the key/value, it might get pushed to the same table but necessarily with higher offset (it checks the highest priority definition)
            - if we delete a key, an index_key_t is pushed with "make_deleted_value_ptr" (it checks the highest priority definition)
            - but if we make an insert-after-delete, the table_id will be necessarily higher.
              ...actually, this is not true, because we might be pushing the new value to the same original table.
              this needs to be handled ASAP because insert-after-delete IS ALLOWED.
              I'm not sure this case can handled. FUCK.

              I could use a bit from the offset or the size and call it the "deleted bit".

              todo: update the code that use ::offset

              It looks that with this change, it fits all of the cases
            */

            return this->table_id() > other.table_id() || offset() > other.offset() || (this->is_deleted() && !other.is_deleted());
        }

        value_ptr_t() = default;
        value_ptr_t(uint64_t offset, uint32_t size, uint32_t table_id) : _offset(offset), _size(size), _table_id(table_id) {}
    };

    struct index_key_t
    {
        key_t key{};
        value_ptr_t value_ptr{};

        bool operator<(const index_key_t& other) const
        {
            return key < other.key || (key == other.key && value_ptr.table_id() < other.value_ptr.table_id());
        }
    };

    constexpr size_t PAGE_SIZE_IN_BYTES = 4096;
    constexpr size_t INDEX_PAGE_NUM_ENTRIES = PAGE_SIZE_IN_BYTES / sizeof(index_key_t);

    inline std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext)
    {
        return path.string() + ext.data();
    }

    template <typename T>
    inline std::span<T> view_as(std::vector<uint8_t>& vector)
    {
        auto start_ptr = typename std::vector<T>::iterator(reinterpret_cast<T*>(vector.data()));
        auto end_ptr = typename std::vector<T>::iterator(reinterpret_cast<T*>(vector.data() + vector.size()));

        return std::span<T>(start_ptr, end_ptr);
    }

    template <typename T>
    auto& byte_varray_view(T& value)
    {
        using U = std::remove_reference_t<T>;

        return reinterpret_cast<const std::array<uint8_t, sizeof(U)>&>(value);
    }

    template <typename T>
    inline T ceil(T value, T divisor)
    {
        return (value + divisor - 1) / divisor;
    }

    inline uint16_t extract_prefix(const uuids::uuid& key)
    {
        const auto& array_view = byte_varray_view(key);

        size_t prefix = 0;

        // little endian!
        prefix |= static_cast<uint16_t>(array_view[1]);
        prefix |= static_cast<uint16_t>(array_view[0]) << 8;

        return prefix;
    }

    /*
        a partition is identified by its (included) upper bound
        e.g. 00fa is the upper bound of the partition [0000, 00ff]
        and might include elements up to 00fa ffff ffff ffff
        element starting with 01fb will be in the subsequent partition
     */
    inline size_t find_partition_prefix_for_key(key_t key, size_t partition_size)
    {
        const auto& K = extract_prefix(key);
        const auto& P = partition_size;

        return ((K + P) / P * P) - 1;
    };

    [[nodiscard]] std::pair<std::string, std::string> format_prefix(uint16_t prefix);

    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions);

} // namespace hedgehog