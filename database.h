#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "bloom_filter.h"
#include "tables.h"

namespace hedgehog::db
{

    class DB
    {
    private:
        std::filesystem::path _base_path;
        uint64_t _next_db_id = 0;

        std::optional<memtable_db> _memtable_db;
        std::vector<std::pair<sortedstring_db, BloomFilter>> _sorted_dbs;

        static constexpr size_t MEMTABLE_FLUSH_ENTRY_LIMIT = 2097152;
        static constexpr uint64_t BLOOM_FILTER_BITS = 41943040;
        static constexpr uint8_t BLOOM_FILTER_HASH_FUNCTIONS = 11;

        DB(const std::filesystem::path& base_path); // Private constructor

        std::string get_next_db_name();
        hedgehog::status load_existing_dbs();
        hedgehog::status flush_current_memtable();

    public:
        // Static factory methods
        static hedgehog::expected<DB> make_new(const std::filesystem::path& base_path);
        static hedgehog::expected<DB> from_path(const std::filesystem::path& base_path);

        ~DB();

        hedgehog::status insert(key_t key, const std::vector<uint8_t>& data);
        hedgehog::expected<std::optional<std::vector<uint8_t>>> get(key_t key);
        hedgehog::status del(key_t key);

        DB(const DB&) = delete;
        DB& operator=(const DB&) = delete;

        DB(DB&&) = default;
        DB& operator=(DB&&) = default;
    };

} // namespace hedgehog::db
