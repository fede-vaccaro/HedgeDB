#pragma once

#include "types.h"

namespace hedge::db
{
    /**
     * @brief Represents the in-memory index (often called a memtable or write buffer).
     * @details This class holds the most recent key-value pointer associations in memory
     * before they are flushed to disk as an immutable `sorted_index` file. It uses an
     * `std::unordered_map` for fast O(1) average time complexity for insertions (`put`)
     * and lookups (`get`). When the `mem_index` reaches a certain size threshold, its
     * contents are flushed to disk. Lookups always check the `mem_index` first, as it
     * contains the newest data, potentially overriding older data present in on-disk indices.
     */
    class mem_index
    {
        /**
         * @brief Underlying data structure mapping keys to value pointers.
         * `std::unordered_map` provides fast average-case lookups and insertions.
         */
        using index_t = std::unordered_map<key_t, value_ptr_t>;

        /**
         * @brief Grants `index_ops` access to the private `_index` member.
         * This is necessary for `index_ops` functions (like `flush_mem_index`)
         * to read and potentially clear the contents of the `mem_index` when
         * creating a `sorted_index` file.
         */
        friend struct index_ops;

        index_t _index; ///< The hash map storing the key -> value_ptr_t mappings.

    public:
        /**
         * @brief Default constructor. Creates an empty mem_index.
         */
        mem_index() = default;

        /**
         * @brief Move constructor. Allows efficient transfer of ownership of the internal map.
         */
        mem_index(mem_index&& other) noexcept = default;

        /**
         * @brief Move assignment operator. Allows efficient transfer of ownership.
         */
        mem_index& operator=(mem_index&& other) noexcept = default;

        // Delete copy constructor and copy assignment operator to prevent
        // accidental expensive copies of potentially large in-memory maps.
        // `mem_index` ownership should typically be transferred via move semantics.
        mem_index(const mem_index&) = delete;
        mem_index& operator=(const mem_index&) = delete;

        /**
         * @brief Removes all entries from the mem_index.
         */
        void clear()
        {
            this->_index.clear();
        }

        /**
         * @brief Pre-allocates memory for the underlying hash map.
         * @details This can improve performance by reducing the number of potential
         * rehashes when inserting a known number of elements. Should be called
         * before inserting a large batch of items if the approximate size is known.
         * @param size The number of elements to reserve space for.
         */
        void reserve(size_t size)
        {
            this->_index.reserve(size);
        }

        /**
         * @brief Inserts or updates a key-value pointer association in the mem_index.
         * @details If the key does not exist, it is inserted. If the key already exists,
         * its associated `value_ptr_t` is overwritten with the new `value`. This ensures
         * that the `mem_index` always holds the most recent update for any given key.
         * @param key The key (`key_t`) to insert or update.
         * @param value The `value_ptr_t` associated with the key.
         * @return Always returns `true` (consistent with `std::map::insert_or_assign` which returns an iterator+bool).
         * The boolean indicates if an insertion happened, but this implementation simplifies it.
         */
        bool put(const key_t& key, const value_ptr_t& value)
        {
            auto [it, inserted] = this->_index.emplace(key, value);

            if(!inserted)
                it->second = value;

            return true;
        }

        /**
         * @brief Retrieves the value pointer associated with a given key.
         * @details Performs a lookup in the underlying hash map.
         * @param key The key (`key_t`) to search for.
         * @return An `std::optional<value_ptr_t>` containing the associated value pointer
         * if the key is found, otherwise `std::nullopt`.
         */
        std::optional<value_ptr_t> get(const key_t& key) const
        {
            auto it = _index.find(key);
            if(it != _index.end())
                return it->second;
            return std::nullopt; // Key not found
        }

        /**
         * @brief Returns the current number of entries stored in the mem_index.
         * @return The number of key-value pointer pairs.
         */
        size_t size() const
        {
            return this->_index.size();
        };
    };
} // namespace hedge::db