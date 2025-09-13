#include <algorithm>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>

#include <error.hpp>

#include "database.h"
#include "tables.h"

namespace hedgehog::db
{
    std::string DB::get_next_db_name()
    {
        return std::format("db.{:0>5}", this->_next_db_id);
    }

    hedgehog::status DB::load_existing_dbs()
    {
        // todo: stub
        return hedgehog::ok();
    }

    hedgehog::status DB::flush_current_memtable()
    {
        if(!this->_memtable_db.has_value())
            return hedgehog::error("No memtable to flush.");

        BloomFilter new_bloom_filter(this->BLOOM_FILTER_BITS, this->BLOOM_FILTER_HASH_FUNCTIONS);

        for(const auto& [key, _] : this->_memtable_db->get_memtable())
        {
            const uint8_t* key_data = reinterpret_cast<const uint8_t*>(key.as_bytes().data());
            std::size_t key_len = key.as_bytes().size();
            new_bloom_filter.add(key_data, key_len);
        }

        auto flushed_db_res = flush_memtable_db(std::move(this->_memtable_db.value()));
        this->_memtable_db.reset();

        if(!flushed_db_res.has_value())
            return hedgehog::error("Failed to flush memtable: " + flushed_db_res.error().to_string());

        sortedstring_db new_sorted_db = std::move(flushed_db_res.value());

        // This part requires BloomFilter to have a serialize method.
        // For demonstration purposes, assuming a `serialize` method exists.
        // std::filesystem::path bloom_path = new_sorted_db._base_path / new_sorted_db._db_name / (new_sorted_db._db_name + ".bloom");
        // auto bloom_save_status = new_bloom_filter.serialize(bloom_path);
        // if (!bloom_save_status) {
        //     return hedgehog::error("Failed to save Bloom filter for flushed DB '" + new_sorted_db._db_name + "': " + bloom_save_status.to_string());
        // }

        this->_sorted_dbs.emplace_back(std::move(new_sorted_db), std::move(new_bloom_filter));
        return hedgehog::ok();
    }

    DB::DB(const std::filesystem::path& base_path)
        : _base_path(base_path)
    {
    }

    hedgehog::expected<DB> DB::make_new(const std::filesystem::path& base_path)
    {
        // if(std::filesystem::exists(base_path))
            // return hedgehog::error("Base path already exists for new DB: " + base_path.string());

        std::filesystem::create_directories(base_path);

        DB new_db(base_path);
        new_db._memtable_db = memtable_db::make_new(new_db._base_path, new_db.get_next_db_name());

        return new_db;
    }

    hedgehog::expected<DB> DB::from_path(const std::filesystem::path& base_path)
    {
        if(!std::filesystem::exists(base_path))
            return hedgehog::error("Base path does not exist for existing DB: " + base_path.string());

        DB existing_db(base_path);
        auto status = existing_db.load_existing_dbs();

        if(!status)
            return hedgehog::error("Failed to load existing databases: " + status.error().to_string());

        existing_db._memtable_db = memtable_db::make_new(existing_db._base_path, existing_db.get_next_db_name());

        return existing_db;
    }

    DB::~DB()
    {
        if(this->_memtable_db.has_value())
        {
            std::cout << "Flushing memtable during DB shutdown..." << std::endl;

            auto flush_res = this->flush_current_memtable();

            if(!flush_res)
                std::cerr << "Error flushing memtable on shutdown: " << flush_res.error().to_string() << std::endl;
        }
    }

    hedgehog::status DB::insert(key_t key, const std::vector<uint8_t>& data)
    {
        if(!this->_memtable_db.has_value())
            return hedgehog::error("Memtable is not initialized.");

        if(this->_memtable_db->get_memtable().size() >= this->MEMTABLE_FLUSH_ENTRY_LIMIT)
        {
            auto flush_status = this->flush_current_memtable();

            if(!flush_status)
                return flush_status;

            this->_next_db_id++;
            this->_memtable_db = memtable_db::make_new(this->_base_path, this->get_next_db_name());
        }

        return this->_memtable_db->insert(key, data);
    }

    hedgehog::expected<std::optional<std::vector<uint8_t>>> DB::get(key_t key)
    {
        if(this->_memtable_db.has_value())
        {
            auto memtable_res = this->_memtable_db->get(key);

            if(memtable_res.has_value() && !memtable_res.value().empty())
                return std::optional<std::vector<uint8_t>>(std::move(memtable_res.value()));

            if(!memtable_res.has_value())
                return hedgehog::error("Error reading from memtable: " + memtable_res.error().to_string());
        }

        for(auto it = this->_sorted_dbs.rbegin(); it != this->_sorted_dbs.rend(); ++it)
        {
            auto& sorted_db = it->first;
            const auto& bloom_filter = it->second;

            const uint8_t* key_data = reinterpret_cast<const uint8_t*>(key.as_bytes().data());
            std::size_t key_len = key.as_bytes().size();

            if(bloom_filter.possiblyContains(key_data, key_len))
            {
                auto sst_res = sorted_db.get(key);
                if(sst_res.has_value() && !sst_res.value().empty())
                    return std::optional<std::vector<uint8_t>>(std::move(sst_res.value()));

                if(!sst_res)
                    return hedgehog::error("Error reading from sortedstring_db: " + sst_res.error().to_string());
            }
        }

        return std::optional<std::vector<uint8_t>>{};
    }

    hedgehog::status DB::del(key_t key)
    {
        if(!this->_memtable_db.has_value())
            return hedgehog::error("Memtable is not initialized.");

        auto memtable_del_status = this->_memtable_db->del(key);

        if(!memtable_del_status)
            return memtable_del_status;

        for(auto it = this->_sorted_dbs.rbegin(); it != this->_sorted_dbs.rend(); ++it)
        {
            auto& sorted_db = it->first;
            const auto& bloom_filter = it->second;

            const uint8_t* key_data = reinterpret_cast<const uint8_t*>(key.as_bytes().data());
            std::size_t key_len = key.as_bytes().size();

            if(bloom_filter.possiblyContains(key_data, key_len))
            {
                auto sst_del_status = sorted_db.del(key);

                if(!sst_del_status)
                    return hedgehog::error(std::format("Failed to delete key from sortedstring_db '{}': {}", sorted_db._db_name, sst_del_status.error().to_string()));
            }
        }

        return hedgehog::ok();
    }

} // namespace hedgehog::db
