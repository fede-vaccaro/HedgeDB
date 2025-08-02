#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>

#include <error.hpp>
#include <sys/types.h>

#include "common.h"
#include "index.h"
#include "task.h"
#include "value_table.h"
#include "worker.h"

namespace hedgehog::db
{

    struct db_config
    {
        static constexpr size_t MAX_PARTITION_EXPONENT = 16;
        static constexpr size_t MIN_KEYS_IN_MEM_BEFORE_FLUSH = 1000;

        size_t keys_in_mem_before_flush = 2'000'000;           // default number of keys to push
        size_t num_partition_exponent = 10;                    // default partition exponent
        double compactation_size_ratio = 0.2;                  // if table rhs/lhs > 0.2, trigger compactation
        size_t compactation_read_ahead_size_bytes = 16384;     // it will read from each table 16 KB at a time
        std::chrono::milliseconds compacation_timeout{120000}; // stop waiting if past this compactation
    };

    class database : public std::enable_shared_from_this<database>
    {
        // constants after initialization
        std::filesystem::path _base_path;
        std::filesystem::path _indices_path;
        std::filesystem::path _values_path;

        // configuration
        db_config _config;

        // persisted state
        using sorted_index_ptr_t = std::shared_ptr<hedgehog::db::sorted_index>;
        using sorted_indices_map_t = std::map<uint16_t, std::vector<sorted_index_ptr_t>>;

        std::mutex _sorted_index_mutex;
        sorted_indices_map_t _sorted_indices;

        std::mutex _value_tables_mutex;
        std::unordered_map<uint32_t, std::shared_ptr<value_table>> _value_tables;

        // current state
        std::shared_ptr<value_table> _current_value_table;
        mem_index _mem_index;

        // worker handling compactation and gc
        worker worker;

    public:
        using byte_buffer_t = std::vector<uint8_t>;

        async::task<expected<byte_buffer_t>> get_async(key_t key, const std::shared_ptr<async::executor_context>& executor);
        async::task<hedgehog::status> put_async(key_t key, const byte_buffer_t& value, const std::shared_ptr<async::executor_context>& executor);
        async::task<hedgehog::status> remove_async(key_t key, const std::shared_ptr<async::executor_context>& executor);

        static expected<std::shared_ptr<database>> make_new(const std::filesystem::path& base_path, const db_config& config);
        static expected<std::shared_ptr<database>> load(const std::filesystem::path& base_path);

    private:
        [[nodiscard]] size_t _find_matching_partition_for_key(const key_t& key) const;
        hedgehog::status _rotate_value_table();
        hedgehog::status _flush_mem_index();
        void _compactation_job(const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedgehog::db
