#pragma once

#include "spinlock.h"
#include "types.h"
#include <mutex>
#include <shared_mutex>
#include <tsl/robin_map.h>
#include <tsl/sparse_map.h>

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
        using inner_index_t = tsl::robin_map<key_t, value_ptr_t>;

        friend struct index_ops;

        struct alignas(64) _inner_index : public inner_index_t
        {
        };

        using index_t = std::vector<_inner_index>;

        index_t _index;

        struct alignas(64) _spinlock : public async::rw_spinlock
        {
        };

        std::unique_ptr<_spinlock[]> _locks;
        // std::unique_ptr<std::shared_mutex[]> _locks;

    public:
        mem_index() = default;

        mem_index(mem_index&& other) noexcept = default;
        mem_index& operator=(mem_index&& other) noexcept = default;

        // Delete copy constructor and copy assignment operator to prevent
        // accidental expensive copies of potentially large in-memory maps.
        // `mem_index` ownership should typically be transferred via move semantics.
        mem_index(const mem_index&) = delete;
        mem_index& operator=(const mem_index&) = delete;

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
        void reserve(size_t thread_count, size_t size)
        {
            this->_locks = std::unique_ptr<_spinlock[]>(new _spinlock[thread_count]);
            // for(size_t i = 0; i < thread_count; ++i)
            //     this->_locks.get()[i] = async::rw_spinlock{};

            this->_index.resize(thread_count);

            assert(reinterpret_cast<uintptr_t>(this->_locks.get()) % 64 == 0);
            assert(reinterpret_cast<uintptr_t>(this->_index.data()) % 64 == 0); 

            constexpr auto SLACK = 0.05;
            const auto size_w_slack = static_cast<size_t>(size * (1.0 + SLACK));

            for(auto& inner_map : this->_index)
                inner_map.reserve(size_w_slack / thread_count);
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
        bool put(size_t tid, const key_t& key, const value_ptr_t& value)
        {
            auto hash = std::hash<key_t>{}(key);
            tid = hash % this->_index.size();

            std::lock_guard lk(this->_locks[tid]);

            auto [it, inserted] = this->_index[tid].try_emplace(key, value);

            if(!inserted)
                it.value() = value;

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
            auto hash = std::hash<key_t>{}(key);
            size_t tid = hash % this->_index.size();

            std::optional<value_ptr_t> result = std::nullopt;

            {
                std::shared_lock lk(this->_locks[tid]);

                auto it = this->_index[tid].find(key);
                if(it != this->_index[tid].end())
                {
                    result = it->second;
                }
            }

            // for(size_t i = 0; i < this->_index.size(); ++i)
            // {
            //     std::shared_lock lk(this->_locks[i]);

            //     auto it = this->_index[i].find(key);
            //     if(it != this->_index[i].end())
            //     {
            //         if(result && it->second < *result)
            //             result = it->second;
            //         else if(!result)
            //             result = it->second;
            //     }
            // }

            return result; // Key not found
        }

        /**
         * @brief Returns the current number of entries stored in the mem_index.
         * @return The number of key-value pointer pairs.
         */
        size_t size() const
        {
            return std::accumulate(this->_index.begin(), this->_index.end(), size_t{0},
                                   [](size_t acc, const inner_index_t& map)
                                   { return acc + map.size(); });
        };

        ~mem_index() {}
    };
} // namespace hedge::db