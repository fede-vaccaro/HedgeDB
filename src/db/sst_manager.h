#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include <error.hpp>
#include <logger.h>

#include "cache.h"
#include "io/io_executor.h"
#include "scan_iterator.h"
#include "sst.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/ex_braid.hpp"
#include "tmc/semaphore.hpp"
#include "types.h"

namespace hedge::db
{

    class sst_manager
    {
    public:
        struct config
        {
            size_t num_partition_exponent;
            size_t max_num_levels;
            size_t min_merge_width;
            size_t max_merge_width;
            double bucket_ratio;
            size_t compaction_read_ahead_size_bytes;
            size_t _compaction_io_workers;
            bool use_odirect_for_indices;
            std::filesystem::path indices_path;
        };

        sst_manager() = default;

        explicit sst_manager(const config& cfg,
                             std::shared_ptr<io::io_executor> compaction_pool,
                             std::shared_ptr<sharded_page_cache> page_cache);

        static hedge::expected<std::unique_ptr<sst_manager>> load(const config& cfg,
                                                                  std::shared_ptr<io::io_executor> compaction_pool,
                                                                  std::shared_ptr<sharded_page_cache> page_cache);

        // Called by memtable flush callback
        tmc::task<void> push_new_ssts(std::vector<sst> new_ssts);

        // SST lookup for the read path
        tmc::task<expected<value_t>> lookup_async(const key_t& key, size_t matching_partition_id);

        // Returns a snapshot (copy) of the partition's level tree, or an error if the partition is not found
        [[nodiscard]] hedge::expected<partition_t> partition_snapshot(size_t partition_id) const;

        // Range scan: returns an iterator over deduplicated entries within [lower, upper]
        hedge::expected<scan_iterator> range_iterator(std::optional<key_t> lower, std::optional<key_t> upper, size_t matching_partition_id, size_t read_ahead_size = 256 * 1024);

        // Compaction control
        void schedule_compaction(bool compact_all);
        void wait_for_compactions_to_finish();

        // Observability
        [[nodiscard]] double read_amplification_factor();
        void print_tree_structure() const;

        // Backpressure flag (read by memtable/database)
        auto& compaction_backpressure() { return _compaction_backpressure; }

        // Flush iteration counter (shared with memtable for SST naming)
        std::atomic_size_t& flush_iteration() { return _flush_iteration; }

        struct compaction_stats
        {
            size_t input_bytes{0};
            size_t output_bytes{0};
            size_t num_inputs{0};
            size_t items_merged{0};

            compaction_stats& operator+=(const compaction_stats& other)
            {
                this->input_bytes += other.input_bytes;
                this->output_bytes += other.output_bytes;
                this->num_inputs += other.num_inputs;
                this->items_merged += other.items_merged;
                return *this;
            }
        };

    private:
        using permissions_t = std::vector<std::shared_ptr<tmc::semaphore>>;
        using sst_level_created_t = std::vector<uint8_t>;
        struct partition_state
        {
            alignas(64) mutable std::shared_mutex mutex;
            partition_t levels;
            permissions_t permissions;     // For fine-grained coordination between compaction commits
            sst_level_created_t created; // For tracking (for each level) whether it's been created, or whether a running compaction task will create it
                                           // Needed for coordinating tombstone garbage collection
            std::atomic_size_t levels_seq_num{0};
            fs::file state_file{};
            std::optional<int> dir_fd{};
            std::mutex state_write_mutex{};

            ~partition_state() { if(dir_fd) ::close(*dir_fd); }
        };

        using sorted_indices_map_t = std::map<uint16_t, std::unique_ptr<partition_state>>;
        using partition_snapshot_map_t = std::map<uint16_t, partition_t>;

        config _cfg{};

        std::atomic_size_t _flush_iteration{0};
        sorted_indices_map_t _sorted_indices;

        std::shared_ptr<io::io_executor> _compaction_pool;
        std::optional<tmc::ex_braid> _compaction_braid;

        size_t _compactor_executor_id{0};
        std::atomic_size_t _compaction_jobs_in_flight{0};

        std::shared_ptr<sharded_page_cache> _page_cache;

        std::mutex _pending_compactions_mutex;
        size_t _pending_compacting_sst_in_l0_count{0};
        size_t _pending_compacting_sst_count{0};
        std::condition_variable _pending_compactions_cv;

        alignas(64) tmc::atomic_condvar<bool> _compaction_backpressure{false};

        logger _logger{"sst_manager"};

        struct compaction_output
        {
            struct compaction_args
            {
                std::vector<sst_ptr_t> inputs;
                hedge::expected<sst_ptr_t> output;

                size_t input_bytes{};
                size_t output_bytes{};
            };

            std::mutex m;
            std::vector<compaction_args> results;
        };

        //
        // COSA STAVO FACENDO:
        // STAVO RAGIONANDO SUL FATTO CHE - VISTO LO SCHEDULING CHE ACCADE IN SEQUENZA (BRAID) -
        // POSSO DECIDERE PRIMA DI LANCIARE _MAKE_SELF_COMPLECTING_.... SE IN QUESTO TASK POSSO
        // ABILITARE IL GC DELLE CHIAVI (cioe' se next_level + 1 e' vuoto oppure se il livello e' l'ultimo, se sono il bucket del livello)
        //
        tmc::task<void> _make_compaction_task(
            std::shared_ptr<tmc::semaphore> can_write,
            std::shared_ptr<tmc::semaphore> next_can_write,
            size_t level,
            std::vector<sst_ptr_t> inputs,
            size_t merge_width,
            bool discard_deleted_keys);

        static std::string _serialize_levels(const partition_t& levels);

        static hedge::expected<partition_t> _deserialize_levels(std::string_view content,
                                                                const std::filesystem::path& dir_path,
                                                                size_t max_num_levels,
                                                                bool use_odirect);

        tmc::task<hedge::status> _persist_partition_state(uint16_t partition_id);

        hedge::expected<compaction_stats> _compaction_job_size_tiered(bool compact_all);

        static auto check_is_sorted_by_epoch(const std::vector<sst_ptr_t>& vec) -> bool
        {
            auto sorted = std::ranges::is_sorted(
                vec,
                [](const sst_ptr_t& lhs, const sst_ptr_t& rhs)
                {
                    return lhs->epoch() < rhs->epoch();
                });
            if(!sorted)
            {
                std::cout << "Indices not sorted by epoch:" << std::endl;
                for(const auto& idx_ptr : vec)
                {
                    std::cout << " - Path: " << idx_ptr->path().string() << ", epoch: " << idx_ptr->epoch() << std::endl;
                }
            }
            return sorted;
        };
    };

} // namespace hedge::db
