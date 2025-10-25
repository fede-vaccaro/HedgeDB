#pragma once

#include "types.h"

namespace hedge::db
{
    class mem_index
    {
        using index_t = std::unordered_map<key_t, value_ptr_t>;

        friend struct index_ops;

        index_t _index;

    public:
        mem_index() = default;

        mem_index(mem_index&& other) noexcept = default;
        mem_index& operator=(mem_index&& other) noexcept = default;

        mem_index(const mem_index&) = delete;
        mem_index& operator=(const mem_index&) = delete;

        void clear()
        {
            this->_index.clear();
        }

        void reserve(size_t size)
        {
            this->_index.reserve(size);
        }

        bool put(const key_t& key, const value_ptr_t& value)
        {
            auto [it, inserted] = this->_index.emplace(key, value);

            if(!inserted)
                it->second = value;

            return true;
        }

        std::optional<value_ptr_t> get(const key_t& key) const
        {
            auto it = _index.find(key);
            if(it != _index.end())
                return it->second;
            return std::nullopt; // Key not found
        }

        size_t size() const
        {
            return this->_index.size();
        };
    };
} // namespace hedge::db