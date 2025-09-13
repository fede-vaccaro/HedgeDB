#include <array>
#include <fcntl.h>
#include <format>
#include <map>
#include <numeric>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "bloom_filter.h"
#include "tables.h"

namespace hedgehog::db
{
    static constexpr std::string_view INDEX_EXT = ".index.ss";
    static constexpr std::string_view VALUE_EXT = ".values.v";
    static constexpr std::string_view TOMBSTONE_EXT = ".ts";

    std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext)
    {
        return path.string() + ext.data();
    }

    tombstone::tombstone(const std::filesystem::path& base_path, const std::string& db_name)
    {
        this->_tombstone_path = base_path / with_extension(db_name, ".tombstone");
        this->_tombstone_log = std::ofstream(this->_tombstone_path, std::ios::binary | std::ios::app);
    }

    hedgehog::status tombstone::add(key_t key)
    {
        this->_keys_cache.insert(key);

        this->_tombstone_log.write(reinterpret_cast<const char*>(&key), sizeof(key_t));

        if(!this->_tombstone_log.good())
            return hedgehog::error("an error occurred with tombstone log");

        return hedgehog::ok();
    }

    bool tombstone::contains(key_t key)
    {
        return this->_keys_cache.contains(key);
    }

    hedgehog::expected<tombstone> tombstone::recovery_tombstone(const std::filesystem::path& base_path, const std::string& db_name)
    {
        auto ts_path = base_path / with_extension(db_name, TOMBSTONE_EXT);

        if(!std::filesystem::exists(ts_path))
            return hedgehog::error("error: tombstone file does not exist: " + ts_path.string());

        auto tombstone_log = std::ifstream(ts_path, std::ios::binary);

        if(!tombstone_log.good())
            return hedgehog::error("error: tombstone file exists but cannot be read from: " + ts_path.string());

        tombstone tombstone{};

        std::vector<key_t> keys(std::filesystem::file_size(ts_path) / sizeof(key_t));

        tombstone_log.read(reinterpret_cast<char*>(keys.data()), static_cast<std::streamsize>(keys.size() * sizeof(key_t)));

        for(const auto& key : keys)
            tombstone._keys_cache.insert(key);

        tombstone._tombstone_path = ts_path;
        tombstone._tombstone_log = std::ofstream(ts_path, std::ios::binary | std::ios::app);

        return tombstone;
    }

    void tombstone::clear()
    {
        this->_keys_cache.clear();

        this->_tombstone_log.close();

        std::filesystem::remove(this->_tombstone_path);

        this->_tombstone_log = std::ofstream(this->_tombstone_path, std::ios::binary | std::ios::app);
    }

    db_attrs db_attrs::make(const std::filesystem::path& base_path, const std::string& db_name)
    {
        return db_attrs{
            ._base_path = base_path,
            ._db_name = db_name,
            ._value_log_path = base_path / with_extension(db_name, VALUE_EXT),
            ._index_path = base_path / with_extension(db_name, INDEX_EXT)};
    }

    std::ostream& operator<<(std::ostream& ostr, const db_attrs& attrs)
    {
        ostr << "db_attrs{base_path: " << attrs._base_path << ", db_name: " << attrs._db_name << ", value_log_path: " << attrs._value_log_path << ", index_path: " << attrs._index_path << '}';
        return ostr;
    }

    void memtable_db::_init()
    {
        this->_value_log_out = std::ofstream(this->_value_log_path, std::ios::binary | std::ios::app);
        this->_tmp_index = std::ofstream(with_extension(this->_index_path, ".tmp"), std::ios::binary | std::ios::app);
    }

    memtable_db memtable_db::make_new(const std::filesystem::path& base_path, const std::string& db_name)
    {
        return memtable_db(db_attrs::make(base_path, db_name), tombstone{base_path, db_name});
    }

    hedgehog::expected<memtable_db> memtable_db::existing_from_path(const std::filesystem::path& base_path, const std::string& db_name)
    {
        auto maybe_tombstone = tombstone::recovery_tombstone(base_path, db_name);

        if(!maybe_tombstone)
            return hedgehog::error("Failed to recover tombstone: " + maybe_tombstone.error().to_string());

        memtable_db mem_db(base_path, db_name, std::move(maybe_tombstone.value()));

        auto maybe_memtable = load_memtable_from<decltype(mem_db._memtable)>(with_extension(mem_db._index_path, ".tmp"));

        if(!maybe_memtable)
            return hedgehog::error("Failed to load memtable: " + maybe_memtable.error().to_string());

        mem_db._memtable = std::move(maybe_memtable.value());

        return mem_db;
    }

    hedgehog::status memtable_db::insert(key_t key, const std::vector<uint8_t>& data)
    {
        // check if key already exists
        auto it = this->_memtable.find(key);
        if(it != this->_memtable.end())
            return hedgehog::error("Update non supported");

        auto size = data.size();
        auto offset = _push_data_to_stream(this->_value_log_out, data);

        if(!this->_value_log_out)
            return hedgehog::error("an error occurred with value_log file");

        auto index_key = index_key_t{key, {.offset = static_cast<uint64_t>(offset), .size = static_cast<uint64_t>(size)}};

        this->_memtable.insert({key, index_key.value_ptr});

        this->_tmp_index.write(reinterpret_cast<const char*>(&index_key), sizeof(index_key_t)); // len(index) % sizeof(index_key_t) != 0

        if(!this->_tmp_index)
            return hedgehog::error("an error occurred with the tmp index");

        return hedgehog::ok();
    }

    hedgehog::expected<std::vector<uint8_t>> memtable_db::get(key_t key)
    {
        if(this->_tombstone.contains(key))
            return std::vector<uint8_t>{};

        auto it = this->_memtable.find(key);

        value_ptr_t offset{};

        if(it == this->_memtable.end())
            return std::vector<uint8_t>{}; // Key not found

        offset = it->second;

        auto values = std::ifstream(this->_value_log_path, std::ios::binary);

        if(!values.good())
            return hedgehog::error("Failed to open value log file");

        values.seekg(static_cast<std::streamoff>(offset.offset), std::ios::beg);

        std::vector<uint8_t> data(offset.size);

        values.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(offset.size));

        if(values.fail())
        {
            values.clear();
            return hedgehog::error("Failed to read data from log: bad bit");
        }

        values.clear();
        return data;
    }

    hedgehog::status memtable_db::del(key_t key)
    {
        if(auto it = this->_memtable.find(key); it != this->_memtable.end())
            this->_memtable.erase(it);

        return this->_tombstone.add(key);
    }

    memtable_db::memtable_db(db_attrs attrs, tombstone tombstone) : db_attrs(std::move(attrs)), _tombstone(std::move(tombstone))
    {
        this->_init();
    }

    memtable_db::memtable_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone tombstone) : db_attrs(db_attrs::make(base_path, db_name)), _tombstone(std::move(tombstone))
    {
        this->_init();
    }

    const std::unordered_map<key_t, value_ptr_t>& memtable_db::get_memtable() const{
        return this->_memtable;
    }

    void memtable_db::_close_files()
    {
        if(this->_value_log_out.is_open())
            this->_value_log_out.close();

        if(this->_tmp_index.is_open())
            this->_tmp_index.close();
    }

    hedgehog::expected<sortedstring_db> sortedstring_db::existing_from_path(const std::filesystem::path& base_path, const std::string& db_name)
    {
        auto index_path = base_path / with_extension(db_name, INDEX_EXT);
        auto value_path = base_path / with_extension(db_name, VALUE_EXT);

        if(!std::filesystem::exists(index_path) || !std::filesystem::exists(value_path))
            return hedgehog::error("index or value log file does not exist");

        auto maybe_tombstone = tombstone::recovery_tombstone(base_path, db_name);

        if(!maybe_tombstone)
            return hedgehog::error("Failed to recover tombstone: " + maybe_tombstone.error().to_string());

        sortedstring_db ss_db{base_path, db_name, std::move(maybe_tombstone.value())};

        auto status = ss_db._init();
        ss_db._tombstone = tombstone::recovery_tombstone(base_path, db_name).value();

        if(!status)
            return status.error();

        return ss_db;
    }

    hedgehog::status sortedstring_db::_init()
    {
        this->_value_log_ifs = std::ifstream(this->_value_log_path, std::ios::binary);

        if(!this->_value_log_ifs.good())
            return hedgehog::error("Failed to open value log file");

        auto fd = fd_wrapper::from_path(this->_index_path);

        if(!fd)
            return hedgehog::error("Failed to open file descriptor: " + fd.error().to_string());

        auto mmap = mmap_wrapper::from_fd_wrapper(std::move(fd.value()));

        if(!mmap.has_value())
            return hedgehog::error("Failet do open mmap: " + mmap.error().to_string());

        this->_mmap = std::move(mmap.value());

        return hedgehog::ok();
    }

    hedgehog::expected<std::vector<uint8_t>> sortedstring_db::get(key_t key)
    {
        if(this->_tombstone.contains(key))
            return std::vector<uint8_t>{};

        auto offset_opt = this->get_offset_from_key(key);

        if(!offset_opt)
            return offset_opt.error();

        if(!offset_opt.value())
            return std::vector<uint8_t>{};

        value_ptr_t offset = *offset_opt.value();

        this->_value_log_ifs.seekg(static_cast<std::streamoff>(offset.offset), std::ios::beg);

        std::vector<uint8_t> data(offset.size);

        this->_value_log_ifs.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(offset.size));

        if(this->_value_log_ifs.bad())
        {
            this->_value_log_ifs.clear();

            auto file_size = std::filesystem::file_size(this->_value_log_path);
            auto current_pos = this->_value_log_ifs.tellg();
            auto requested_pos = static_cast<std::streamoff>(offset.offset);
            auto value_size = static_cast<std::streamsize>(offset.size);

            std::string error_msg = std::format("Failed to read data from log: bad bit. File size: {}, Current position: {}, Requested position: {}, Value size: {}", std::to_string(file_size), std::to_string(current_pos), std::to_string(requested_pos), std::to_string(value_size));

            return hedgehog::error(error_msg);
        }

        return data;
    }

    hedgehog::expected<std::optional<value_ptr_t>> sortedstring_db::get_offset_from_key(key_t key) // NOLINT
    {
        if(this->_tombstone.contains(key))
            return std::nullopt;

        // Memory-map the file
        void* mapped = this->_mmap.get_ptr();

        auto* ptr_start = static_cast<index_key_t*>(mapped);
        auto* ptr_end = static_cast<index_key_t*>(mapped) + this->_mmap.size() / sizeof(index_key_t);

        // Binary search
        auto* ptr = std::lower_bound(ptr_start, ptr_end, index_key_t{key, {}});

        auto item = *ptr;

        if(item.key != key)
            return std::nullopt;

        return item.value_ptr;
    }

    hedgehog::status sortedstring_db::compact()
    {
        auto maybe_index = load_memtable_from<std::vector<index_key_t>>(this->_index_path);

        if(!maybe_index)
            return hedgehog::error("Failed to load index file: " + maybe_index.error().to_string());

        auto index = std::move(maybe_index.value());

        std::sort(index.begin(), index.end(), [](const index_key_t& lhs, const index_key_t& rhs)

                  { return lhs.value_ptr.offset < rhs.value_ptr.offset; });

        std::filesystem::path cmpt_index_path = with_extension(this->_index_path, ".cmpt");
        std::filesystem::path cmpt_value_path = with_extension(this->_value_log_path, ".cmpt");

        std::ofstream index_ofs(cmpt_index_path, std::ios::binary);
        std::ofstream values_ofs(cmpt_value_path, std::ios::binary);

        if(!index_ofs.good())
            return hedgehog::error("Failed to open index file");

        if(!values_ofs.good())
            return hedgehog::error("Failed to open values file");

        for(auto& [key, offset] : index)
        {
            if(this->_tombstone.contains(key))
            {
                // std::cout << "Skipping tombstone key: " << key << std::endl;
                continue;
            }

            auto maybe_data = this->get(key);

            if(!maybe_data)
                return hedgehog::error(std::format("Cannot retrieve file with key {}", uuids::to_string(key)));

            auto data = std::move(maybe_data.value());

            if(data.empty())
                return hedgehog::error("Failed to get data for key: ");

            auto size = data.size();
            auto data_offset = _push_data_to_stream(values_ofs, data);

            if(!values_ofs.good())
                return hedgehog::error("Failed to write values");

            auto new_offset = value_ptr_t{
                .offset = data_offset,
                .size = size};

            offset = new_offset;
        }

        std::sort(index.begin(), index.end());

        for(auto& [key, offset] : index)
        {
            index_ofs.write(reinterpret_cast<const char*>(&key), sizeof(index_key_t));

            if(!index_ofs.good())
                return hedgehog::error("Failed to write index");
        }

        std::filesystem::path old_index_path = with_extension(this->_index_path, ".old");
        std::filesystem::path old_value_path = with_extension(this->_value_log_path, ".old");

        std::filesystem::rename(this->_index_path, old_index_path);
        std::filesystem::rename(this->_value_log_path, old_value_path);

        std::filesystem::rename(cmpt_index_path, this->_index_path);
        std::filesystem::rename(cmpt_value_path, this->_value_log_path);

        std::filesystem::remove(old_value_path);
        std::filesystem::remove(old_index_path);

        this->_tombstone.clear();

        return hedgehog::ok();
    }

    hedgehog::status sortedstring_db::del(key_t key)
    {
        return this->_tombstone.add(key);
    }

    sortedstring_db::sortedstring_db(const std::filesystem::path& base_path, const std::string& db_name, tombstone tombstone) : db_attrs(db_attrs::make(base_path, db_name)), _tombstone(std::move(tombstone)) {}

    sortedstring_db::sortedstring_db(db_attrs attrs, tombstone tombstone) : db_attrs(std::move(attrs)), _tombstone(std::move(tombstone)){};

    hedgehog::expected<sortedstring_db> flush_memtable_db(memtable_db&& db_)
    {
        auto mem_db = std::move(db_);
        
        mem_db._close_files();

        std::vector<index_key_t> index_sorted;

        index_sorted.reserve(mem_db._memtable.size());

        for(const auto& [key, offset] : mem_db._memtable)
            index_sorted.push_back({key, offset});

        std::sort(index_sorted.begin(), index_sorted.end());

        auto index = std::ofstream(mem_db._index_path, std::ios::binary);

        index.write(reinterpret_cast<const char*>(index_sorted.data()), static_cast<std::streamsize>(index_sorted.size() * sizeof(index_key_t)));

        if(!index.good())
            return hedgehog::error("An error occurred with index file");

        std::filesystem::remove(with_extension(mem_db._index_path, ".tmp"));

        auto& attrs = static_cast<db_attrs&>(mem_db);

        index.close();

        sortedstring_db ss_db(std::move(attrs), std::move(mem_db._tombstone));

        if(auto status = ss_db._init(); !status)
            return hedgehog::error("Failed to initialize sortedstring_db: " + status.error().to_string());

        // std::cout << "ss_db initialized" << std::endl;

        return std::move(ss_db);
    }

} // namespace hedgehog::db
