#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>

#include <error.hpp>
#include <sys/types.h>

#include "common.h"
#include "index.h"
#include "io_executor.h"
#include "logger.h"
#include "task.h"
#include "value_table.h"
#include "worker.h"

namespace hedge::db
{

    struct db_config
    {
        // todo: sanitize every config param
        static constexpr size_t MAX_PARTITION_EXPONENT = 16;
        static constexpr size_t MIN_KEYS_IN_MEM_BEFORE_FLUSH = 1000;

        size_t keys_in_mem_before_flush = 2'000'000;          // default number of keys to push
        size_t num_partition_exponent = 10;                   // default partition exponent
        double compaction_size_ratio = 0.2;                   // if during two way merge, rhs/lhs > compaction_size_ratio, a compaction job is triggered
        size_t compaction_read_ahead_size_bytes = 16384;      // it will read from each table 16 KB at a time
        std::chrono::milliseconds compaction_timeout{120000}; // stop waiting if this timeout is due
        bool auto_compaction = true;                          // compaction is automatically triggered when the memtable reaches its limit
        size_t max_pending_compactions = 16;                  // maximum number of pending compactions before the database stops accepting new writes: TODO: implement this
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
        using sorted_index_ptr_t = std::shared_ptr<hedge::db::sorted_index>;
        using sorted_indices_map_t = std::map<uint16_t, std::vector<sorted_index_ptr_t>>;

        size_t _flush_iteration{0};
        std::mutex _sorted_index_mutex;
        sorted_indices_map_t _sorted_indices;

        size_t _last_table_id{0};
        std::mutex _value_tables_mutex;
        std::unordered_map<uint32_t, std::shared_ptr<value_table>> _value_tables;

        // current state
        std::shared_ptr<value_table> _current_value_table;
        mem_index _mem_index;

        // worker handling compaction and gc
        async::worker compaction_worker;
        async::worker gc_worker;

        // logger
        logger _logger{"database"};

    public:
        using byte_buffer_t = std::vector<uint8_t>;

        async::task<expected<byte_buffer_t>> get_async(key_t key, const std::shared_ptr<async::executor_context>& executor);

        async::task<hedge::status> put_async(key_t key, const byte_buffer_t& value, const std::shared_ptr<async::executor_context>& executor);
        async::task<hedge::status> put_async(key_t key, byte_buffer_t&& value, const std::shared_ptr<async::executor_context>& executor);

        async::task<hedge::status> remove_async(key_t key, const std::shared_ptr<async::executor_context>& executor);
        std::future<hedge::status> compact_sorted_indices(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor);

        static expected<std::shared_ptr<database>> make_new(const std::filesystem::path& base_path, const db_config& config);
        static expected<std::shared_ptr<database>> load(const std::filesystem::path& base_path);

        hedge::status flush();
        std::future<hedge::status> garbage_collect_tables(const std::shared_ptr<async::executor_context>& executor);

        [[nodiscard]] double load_factor();

    private:
        database() = default;

        [[nodiscard]] size_t _find_matching_partition_for_key(const key_t& key) const;

        hedge::status _rotate_value_table();
        hedge::status _flush_mem_index();
        hedge::status _compaction_job(bool ignore_ratio, const std::shared_ptr<async::executor_context>& executor);
        async::task<hedge::status> _garbage_collect_table(std::shared_ptr<value_table> table, size_t id, const std::shared_ptr<async::executor_context>& executor);
        async::task<expected<std::pair<value_ptr_t, std::shared_ptr<value_table>>>> _find_value_ptr_and_value_table(key_t key, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedge::db
