#pragma once

#include <algorithm>
#include <array>
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

#include <error.hpp>
#include <uuid.h>

#include <cstdint>

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

        std::unordered_map<key_t, value_ptr_t> _memtable;

        std::ofstream _value_log_out;
        std::ofstream _tmp_index;

        friend hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

        void _init();
        void _close_files();

    public:
        explicit memtable_db() = delete;

        static memtable_db make_new(const std::filesystem::path& base_path, const std::string& db_name);

        static hedgehog::expected<memtable_db> existing_from_path(const std::filesystem::path& base_path, const std::string& db_name);

        hedgehog::status insert(key_t key, const std::vector<uint8_t>& data);

        hedgehog::expected<std::vector<uint8_t>> get(key_t key);

        hedgehog::status del(key_t key);

        const std::unordered_map<key_t, value_ptr_t>& get_memtable() const;

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

    class mmap_wrapper
    {
    private:
        fd_wrapper _fd_wrapper;
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;

    public:
        static hedgehog::expected<mmap_wrapper> from_fd_wrapper(fd_wrapper&& fd_w)
        {
            if(fd_w.get() == -1)
            {
                return hedgehog::error("Cannot map an invalid file descriptor.");
            }
            if(fd_w.file_size() == 0)
            {
            }

            void* mapped_ptr = mmap(nullptr, fd_w.file_size(), PROT_READ, MAP_PRIVATE, fd_w.get(), 0);

            if(mapped_ptr == MAP_FAILED)
            {
                auto err_msg = get_error_message_thread_safe();
                return hedgehog::error("Failed to mmap file: " + err_msg);
            }

            mmap_wrapper wrapper;
            wrapper._fd_wrapper = std::move(fd_w);
            wrapper._mapped_ptr = mapped_ptr;
            wrapper._mapped_size = wrapper._fd_wrapper.file_size();

            return std::move(wrapper);
        }

        static hedgehog::expected<mmap_wrapper> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
        {
            auto fd_res = fd_wrapper::from_path(path, expected_size);
            if(!fd_res.has_value())
            {
                return hedgehog::error("Failed to get file descriptor for mmap: " + fd_res.error().to_string());
            }
            return from_fd_wrapper(std::move(fd_res.value()));
        }

        mmap_wrapper() = default;

        mmap_wrapper(mmap_wrapper&& other) noexcept
            : _fd_wrapper(std::move(other._fd_wrapper)),
              _mapped_ptr(other._mapped_ptr),
              _mapped_size(other._mapped_size)
        {
            other._mapped_ptr = nullptr;
            other._mapped_size = 0;
        }

        mmap_wrapper& operator=(mmap_wrapper&& other) noexcept
        {
            if(this != &other)
            {
                if(_mapped_ptr != nullptr && _mapped_ptr != MAP_FAILED)
                {
                    munmap(_mapped_ptr, _mapped_size);
                }

                _fd_wrapper = std::move(other._fd_wrapper);
                _mapped_ptr = other._mapped_ptr;
                _mapped_size = other._mapped_size;

                other._mapped_ptr = nullptr;
                other._mapped_size = 0;
            }
            return *this;
        }

        mmap_wrapper(const mmap_wrapper&) = delete;
        mmap_wrapper& operator=(const mmap_wrapper&) = delete;

        ~mmap_wrapper()
        {
            if(_mapped_ptr != nullptr && _mapped_ptr != MAP_FAILED)
            {
                if(munmap(_mapped_ptr, _mapped_size) == -1)
                {
                    perror("Error munmapping file in mmap_wrapper destructor");
                }
            }
        }

        [[nodiscard]] void* get_ptr() const
        {
            return _mapped_ptr;
        }

        [[nodiscard]] size_t size() const
        {
            return _mapped_size;
        }

        [[nodiscard]] int get_fd() const
        {
            return _fd_wrapper.get();
        }
    };

    class sortedstring_db : public db_attrs
    {
        tombstone _tombstone;
        std::ifstream _value_log_ifs;
        
        mmap_wrapper _mmap;
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
        friend hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

        hedgehog::status _init();

        explicit sortedstring_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone tombstone, std::optional<std::unordered_map<key_t, value_ptr_t>> index = std::nullopt);

        explicit sortedstring_db(db_attrs attrs, tombstone tombstone, std::optional<std::unordered_map<key_t, value_ptr_t>> index = std::nullopt);
    };

    hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_);

} // namespace hedgehog::db
