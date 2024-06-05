#pragma once

#include <algorithm>
#include <array>
#include <cstdio>
#include <filesystem>
#include <format>
#include <fstream>
#include <mutex>
#include <functional>
#include <ios>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <error.hpp>
#include <uuid.h>

#include <cstdint>

#include "index.h"
#include "fs.hpp"
namespace hedgehog::db
{
    static constexpr char FILE_SEPARATOR = static_cast<char>(0);

    std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext);

    template <typename STREAM>
    size_t _push_data_to_stream(STREAM& value_ofstream, const std::vector<uint8_t>& data)
    {
        value_ofstream << FILE_SEPARATOR;

        auto offset = value_ofstream.tellp();

        value_ofstream.write(reinterpret_cast<const char*>(data.data()), static_cast<int64_t>(data.size()));

        return offset;
    }

    std::unordered_map<key_t, value_ptr_t> index_to_memtable(const std::vector<index_key_t>::iterator begin,
                                                                                         const std::vector<index_key_t>::iterator end);    
    class tombstone
    {
        std::unordered_set<key_t> _keys_cache;
        std::filesystem::path _tombstone_path;
        std::ofstream _tombstone_log;

    public:
        explicit tombstone(const std::filesystem::path& base_path, const std::string& db_name);

        tombstone(const tombstone& other) = delete;
        tombstone& operator=(tombstone& other) = delete;

        tombstone(tombstone&& other) noexcept = default;
        tombstone& operator=(tombstone&& other) noexcept = default;

        ~tombstone() = default;

        hedgehog::status add(key_t key);

        bool contains(key_t key);

        static hedgehog::expected<tombstone> recovery_tombstone(const std::filesystem::path& base_path, const std::string& db_name);

        static hedgehog::expected<std::unordered_map<uint32_t, tombstone>> split_based_on_prefix(tombstone&& tombstone);

        void clear();

    private:
        tombstone() = default;
    };

    template <typename CONTAINER>
    hedgehog::expected<CONTAINER> load_memtable_from(const std::filesystem::path& path)
    {
        CONTAINER memtable;

        if(!std::filesystem::exists(path))
            return hedgehog::error("Index file does not exist: " + path.string());

        auto tmp_index = std::ifstream(path, std::ios::binary);

        if(!tmp_index.good())
            return hedgehog::error("Failed to open index file: " + path.string());

        index_key_t index_key{};

        auto file_size = std::filesystem::file_size(path);

        if(size_t modulo = file_size % sizeof(index_key_t); modulo != 0)
            return hedgehog::error(std::format("Invalid index file size for index: {} extra {} bytes", path.string(), modulo));

        constexpr bool container_is_map = std::is_same_v<CONTAINER, std::map<key_t, value_ptr_t>> || std::is_same_v<CONTAINER, std::unordered_map<key_t, value_ptr_t>>;

        if constexpr(std::is_same_v<CONTAINER, std::vector<index_key_t>>)
        {
            memtable.resize(file_size / sizeof(index_key_t));

            tmp_index.read(reinterpret_cast<char*>(memtable.data()), static_cast<std::streamsize>(file_size));
        }
        else if constexpr(container_is_map)
        {
            memtable.reserve(file_size / sizeof(index_key_t));

            while(tmp_index.read(reinterpret_cast<char*>(&index_key), sizeof(index_key_t))) // what if the file size is not a multiple of sizeof(index_key_t)
                memtable.insert({index_key.key, index_key.value_ptr});
        }
        else
            static_assert(false, "Unsupported container type");

        if(tmp_index.bad())
            return hedgehog::error("Failed to read index file");

        std::cout << "Loaded " << memtable.size() << " keys from index" << std::endl;

        return memtable;
    }

    struct db_attrs
    {
        std::filesystem::path _base_path;
        std::string _db_name;
        std::filesystem::path _value_log_path;
        std::filesystem::path _index_path;

        static db_attrs make(const std::filesystem::path& base_path, const std::string& db_name);
    };

    std::ostream& operator<<(std::ostream& ost, const db_attrs& attrs);

    class sortedstring_db;
    class memtable_db;

    class flusher
    {
        static hedgehog::expected<std::unordered_map<uint32_t, sortedstring_db>> flush_memtable_db(memtable_db&& db_, bool cache_memtable = true);
    };

    class memtable_db : public db_attrs
    {
        tombstone _tombstone;

        std::unordered_map<key_t, value_ptr_t> _memtable;

        std::ofstream _value_log_out;
        std::ofstream _tmp_index;

        friend flusher;

        void _init();
        void _close_files();

        std::optional<uint32_t> _table_id;

    public:
        explicit memtable_db() = delete;

        static memtable_db make_new(const std::filesystem::path& base_path, const std::string& db_name, uint32_t table_id);

        static hedgehog::expected<memtable_db> existing_from_path(const std::filesystem::path& base_path, const std::string& db_name);

        hedgehog::status insert(key_t key, const std::vector<uint8_t>& data);

        hedgehog::expected<std::vector<uint8_t>> get(key_t key);

        hedgehog::status del(key_t key);

        const std::unordered_map<key_t, value_ptr_t>& get_memtable() const;

    private:
        explicit memtable_db(db_attrs attrs, tombstone init_tombstone, uint32_t table_id);

        explicit memtable_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone init_tombstone, uint32_t table_id);
    };

    class sortedstring_db : public db_attrs
    {
        tombstone _tombstone;
        std::ifstream _value_log_ifs;
        
        fs::mmap_wrapper _mmap;
        std::optional<std::unordered_map<key_t, value_ptr_t>> _cached_index;

    public:
        sortedstring_db() = delete;

        hedgehog::expected<std::vector<uint8_t>> get(key_t key);

        hedgehog::expected<std::optional<value_ptr_t>> get_value_ptr(key_t key);

        hedgehog::status compact();

        hedgehog::status del(key_t key);

        void drop_cache_index();

        hedgehog::status cache_index();

        static hedgehog::expected<sortedstring_db> existing_from_path(const std::filesystem::path& base_path, const std::string& db_name);

    private:
        friend flusher;

        hedgehog::status _init();

        explicit sortedstring_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone tombstone, std::optional<std::unordered_map<key_t, value_ptr_t>> index = std::nullopt);

        explicit sortedstring_db(db_attrs attrs, tombstone tombstone, std::optional<std::unordered_map<key_t, value_ptr_t>> index = std::nullopt);
    };

} // namespace hedgehog::db
