#pragma once

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <format>
#include <fstream>
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

#include <uuid.h>

#include <cstdint>

#include <include/error.hpp>

// todo list
// test: index recovery from crash before flush
// test: "broken" index file (e.g. not multiple of sizeof(index_key_t))

// test compactation with inconsistent index
// implement thread safety
// use faster unordered_map implementation (e.g. abseil::flat_hash_map)

// design idea: the actual database is a manager over several db objects
// design idea: a db object is a handle over a index, a tombston and a value file
// todo: shutdown operations on critical fs errors
// todo: tombstone object
// todo HIGH PRIORITY: sanitization: truncate db.index.tmp and db.tombstone if length is invalid (size(index) % sizeof(index_key_t) != 0)
// todo idea: recover index from values file - i could use this format : [4/8 bytes magic number, key, size, actual file] - adds overhead especially for small files but might be worth it
// todo : try_emplace when inserting into the index
// todo: implement bloom filter and use murmur hash
// todo: check if offset and offset + size is valid within the file size - at least test it. maybe is okay to check whether ifstream is good?
// implement cumulative size
// todo: index & tombstone checksum
// todo: improve compact by using prefetch/paged mmap

namespace hedgehog::db
{
    using key_t = uuids::uuid;

    struct value_ptr_t
    {
        uint64_t offset{};
        uint64_t size{};
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

    namespace hashed_index
    {
        constexpr static size_t NUM_BUCKETS = 16UL * 1024UL;
        constexpr static size_t BUCKET_SIZE = 127;

        struct bucket // 4096 bytes single page sized bucket
        {
            using bucket_value_t = std::array<index_key_t, BUCKET_SIZE>;

            size_t elements_count{};
            std::array<size_t, 3> _padding{};
            bucket_value_t elements{};
        };

        inline bool push_item(bucket* buckets, const index_key_t& item)
        {
            auto& bucket = buckets[std::hash<db::key_t>{}(item.key) % NUM_BUCKETS]; // NOLINT

            if(bucket.elements_count >= BUCKET_SIZE)
                return false;

            bucket.elements[bucket.elements_count++] = item; // NOLINT

            return true;
        }

        inline std::optional<value_ptr_t> get_item(bucket* buckets, const key_t& key)
        {
            auto& bucket = buckets[std::hash<db::key_t>{}(key) % NUM_BUCKETS]; // NOLINT

            for(size_t i = 0; i < bucket.elements_count; ++i)
            {
                if(bucket.elements[i].key == key)        // NOLINT
                    return bucket.elements[i].value_ptr; // NOLINT
            }

            return std::nullopt;
        }

        constexpr static size_t get_index_byte_size()
        {
            return sizeof(bucket) * NUM_BUCKETS;
        }

        inline bool erase(bucket* buckets, const key_t& key)
        {
            auto& bucket = buckets[std::hash<db::key_t>{}(key) % NUM_BUCKETS]; // NOLINT

            auto* begin_it = bucket.elements.begin();
            auto* end_it = bucket.elements.begin() + bucket.elements_count;

            auto* removed = std::remove_if(begin_it, end_it,
                                           [&](const auto& item)
                                           { return item.key == key; });

            if(removed == end_it)
                return false;

            --bucket.elements_count;

            return true;
        }

        inline hedgehog::status serialize_to(bucket* buckets, const std::filesystem::path& path)
        {
            std::ofstream ofs(path, std::ios::binary);

            if(!ofs.good())
                return hedgehog::error("Failed to open file: " + path.string());

            auto count = sizeof(bucket) * NUM_BUCKETS;

            std::cout << "Writing " << count << " bytes to file" << std::endl;
            std::cout << "Writing " << NUM_BUCKETS << " buckets to file" << std::endl;
            std::cout << "Writing " << sizeof(bucket) << " bytes per bucket" << std::endl;

            ofs.write(reinterpret_cast<const char*>(buckets), static_cast<std::streamsize>(count));

            if(!ofs.good())
                return hedgehog::error("Failed to write file: " + path.string());

            return hedgehog::ok();
        }

        inline hedgehog::status deserialize_from(bucket* buckets, const std::filesystem::path& path)
        {
            std::ifstream ifs(path, std::ios::binary);

            if(!ifs.good())
                return hedgehog::error("Failed to open file: " + path.string());

            ifs.read(reinterpret_cast<char*>(buckets), static_cast<std::streamsize>(sizeof(bucket) * NUM_BUCKETS));

            if(!ifs.good())
                return hedgehog::error("Failed to read file: " + path.string());

            return hedgehog::ok();
        }

    } // namespace hashed_index

    struct hashed_index_managed
    {
        std::vector<hashed_index::bucket> _buckets = std::vector<hashed_index::bucket>(hashed_index::NUM_BUCKETS);

        bool push_item(const index_key_t& item)
        {
            return hashed_index::push_item(this->_buckets.data(), item);
        }

        std::optional<value_ptr_t> get_item(const key_t& key)
        {
            return hashed_index::get_item(this->_buckets.data(), key);
        }

        constexpr static size_t index_byte_size()
        {
            return hashed_index::get_index_byte_size();
        }

        bool erase(const key_t& key)
        {
            return hashed_index::erase(this->_buckets.data(), key);
        }

        hedgehog::status serialize_to(const std::filesystem::path& path)
        {
            return hashed_index::serialize_to(this->_buckets.data(), path);
        }

        hedgehog::status deserialize_from(const std::filesystem::path& path)
        {
            return hashed_index::deserialize_from(this->_buckets.data(), path);
        }
    };

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

    class memtable_db : public db_attrs
    {
        tombstone _tombstone;

        hashed_index_managed _memtable{};

        std::ofstream _value_log_out;
        std::ofstream _tmp_index;

        friend hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

        void _init();

    public:
        explicit memtable_db() = delete;

        static memtable_db make_new(const std::filesystem::path& base_path, const std::string& db_name);

        static hedgehog::expected<memtable_db> existing_from_path(const std::filesystem::path& base_path, const std::string& db_name);

        hedgehog::status insert(key_t key, const std::vector<uint8_t>& data);

        hedgehog::expected<std::vector<uint8_t>> get(key_t key);

        hedgehog::status del(key_t key);

    private:
        explicit memtable_db(db_attrs attrs, tombstone init_tombstone);

        explicit memtable_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone init_tombstone);
    };

    inline std::string get_error_message_thread_safe()
    {
        constexpr size_t BUF_SIZE = 1024;
        std::array<char, BUF_SIZE> buf{};

        strerror_r(errno, buf.data(), buf.size());

        return {buf.data()};
    }

    class fd_wrapper
    {
        int _fd = -1;
        size_t _file_size{};

    public:
        [[nodiscard]] int get() const
        {
            return this->_fd;
        }

        [[nodiscard]] size_t file_size() const
        {
            return this->_file_size;
        }

        static hedgehog::expected<fd_wrapper> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
        {
            if(!std::filesystem::exists(path))
                return hedgehog::error("File does not exist: " + path.string());

            // Open the file;
            int fd = open(path.c_str(), O_RDONLY); // NOLINT

            if(fd == -1)
            {
                auto err = get_error_message_thread_safe();
                return hedgehog::error("Failed to open file descriptor " + err);
            }

            fd_wrapper fd_wrapped{};
            fd_wrapped._fd = fd;

            // Get the size of the file
            size_t file_size = std::filesystem::file_size(path);

            if(expected_size.has_value() && file_size != expected_size.value())
                return hedgehog::error("Invalid file size! " + std::to_string(file_size) + " != " + std::to_string(expected_size.value()));

            fd_wrapped._file_size = file_size;

            return std::move(fd_wrapped);
        }

        fd_wrapper() = default;

        fd_wrapper(fd_wrapper&& other) : _fd(other._fd), _file_size(other._file_size)
        {
            other._fd = -1;
            other._file_size = 0;
        }

        fd_wrapper& operator=(fd_wrapper&& other)
        {
            this->_fd = other._fd;
            this->_file_size = other._file_size;

            other._fd = -1;
            other._file_size = 0;

            return *this;
        };

        fd_wrapper(const fd_wrapper&) = delete;
        fd_wrapper& operator=(const fd_wrapper&) = delete;

        ~fd_wrapper()
        {
            if(this->_fd != 0)
                close(this->_fd);
        }
    };

    class sortedstring_db : public db_attrs
    {
        tombstone _tombstone;
        std::ifstream _value_log_mmap;
        fd_wrapper _fd;

    public:
        sortedstring_db() = delete;

        hedgehog::expected<std::vector<uint8_t>> get(key_t key);

        hedgehog::expected<std::optional<value_ptr_t>> get_offset_from_key(key_t key);

        hedgehog::status compact();

        hedgehog::status del(key_t key);

        static hedgehog::expected<sortedstring_db> existing_from_path(const std::filesystem::path& base_path, const std::string& db_name);

    private:
        friend hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

        hedgehog::status _init();

        explicit sortedstring_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone tombstone);

        explicit sortedstring_db(db_attrs attrs, tombstone tombstone);
    };

    hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

} // namespace hedgehog::db
