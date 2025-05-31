#pragma once

#include <filesystem>

#include <sys/types.h>
#include <uuid.h>

#include <error.hpp>
namespace hedgehog::db
{
    using key_t = uuids::uuid;

    struct value_ptr_t
    {
        uint64_t offset{};
        uint32_t size{};
        uint32_t table_id{};
    };

    struct index_key_t
    {
        key_t key{};
        value_ptr_t value_ptr{};

        bool operator<(const index_key_t& other) const
        {
            return key < other.key;
        }
    };

    inline std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext)
    {
        return path.string() + ext.data();
    }

    class mem_index;

    struct index_ops{
        static hedgehog::status flush_and_compact(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, bool multithreaded = false);

        // flush: merge indices ->
        //        split by 2 bytes prefix
        //        merge with files
    };

    class mem_index
    {
        using index_t = std::unordered_map<key_t, value_ptr_t>;
        
        friend struct index_ops;

        index_t _index;

    public:
        bool add(const key_t& key, const value_ptr_t& value){
            auto [it, inserted] = _index.emplace(key, value);

            if(!inserted)
                it->second = value; // Update existing entry

            return true;
        }

        bool erase(const key_t& key){
            return _index.erase(key) > 0;
        }

        std::optional<value_ptr_t> get(const key_t& key) const {
            auto it = _index.find(key);
            if(it != _index.end())
                return it->second;
            return std::nullopt; // Key not found
        }

        size_t size() const
        {
            return _index.size();
        };     
    };


}