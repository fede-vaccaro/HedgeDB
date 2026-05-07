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
#include "range_iterator.h"
#include "sst.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/channel.hpp"
#include "tmc/semaphore.hpp"
#include "types.h"

namespace hedge::db
{

    class sst_manager
    {
    public:
        struct config
        {
            size_t num_partition_exponent{4};
            size_t max_num_levels{40};
            size_t min_merge_width{4};
            size_t max_merge_width{16};
            double bucket_ratio{1.5};
            size_t compaction_read_ahead_size_bytes{2 * MiB};
            bool use_odirect_for_ssts{true};
            std::optional<size_t> ssts_in_l0_block_write_threshold{40};
            std::filesystem::path partitions_path;
        };

        sst_manager() = default;

        explicit sst_manager(const config& cfg,
                             std::shared_ptr<io::io_executor> compaction_pool,
                             std::shared_ptr<sharded_page_cache> page_cache);

        static hedge::expected<std::unique_ptr<sst_manager>> load(const config& cfg,
                                                                  std::shared_ptr<io::io_executor> compaction_pool,
                                                                  std::shared_ptr<sharded_page_cache> page_cache);

        // push_new_ssts_to_l0 Push a batch of SSTs to L0 as the most recent
        // Returns a second callback (via tmc::task) that should be called for updating the manifest
        tmc::task<void> push_new_ssts_to_l0(std::vector<sst> new_ssts);

        // SST lookup for the read path
        tmc::task<expected<value_t>> lookup_async(const key_t& key, size_t matching_partition_id);

        // Returns a snapshot (copy) of the partition's level tree, or an error if the partition is not found
        [[nodiscard]] hedge::expected<partition_t> acquire_partition_snapshot(size_t partition_id) const;

        // Range scan: returns an iterator over deduplicated entries within [lower, upper]
        [[nodiscard]] hedge::expected<range_iterator> make_range_iterator(std::optional<key_t> lower, std::optional<key_t> upper, size_t matching_partition_id, size_t read_ahead_size = 256 * 1024) const;

        // Compaction control
        void launch_compaction_worker();
        void schedule_compaction(bool compact_all);
        void wait_for_compactions_to_finish();

        // Observability
        [[nodiscard]] double read_amplification_factor();
        void print_tree_structure() const;

        // Returns the highest max_seq_nr across all SSTs at all levels and partitions.
        // Returns 0 if no SSTs are loaded (first boot or empty DB).
        [[nodiscard]] uint64_t max_seq_nr() const;

        // Backpressure flag (read by memtable/database)
        auto& compaction_backpressure() { return this->_compaction_backpressure; }

        // Flush iteration counter (shared with memtable for SST naming)
        std::atomic_size_t& flush_iteration() { return this->_flush_iteration; }

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
        friend class sst_manager_helpers;

        using permissions_t = std::vector<std::shared_ptr<tmc::semaphore>>;
        using sst_level_created_t = std::vector<uint8_t>;
        struct _partition_state
        {
            alignas(64) mutable std::shared_mutex mutex;
            partition_t levels;
            permissions_t permissions;   // For fine-grained coordination between compaction commits
            sst_level_created_t created; // For tracking (for each level) whether it's been created, or whether a running compaction task will create it
                                         // Needed for coordinating tombstone garbage collection
            std::atomic_size_t levels_seq_num{0};
            fs::file state_file{};
            std::optional<int> dir_fd{};
            std::mutex state_write_mutex{};

            ~_partition_state()
            {
                if(dir_fd)
                    ::close(*dir_fd);
            }
        };

        using sorted_indices_map_t = std::map<uint16_t, std::unique_ptr<_partition_state>>;
        using partition_snapshot_map_t = std::map<uint16_t, partition_t>;

        config _cfg{};

        std::atomic_size_t _flush_iteration{0};
        sorted_indices_map_t _sorted_indices;

        std::shared_ptr<io::io_executor> _compaction_pool;

        size_t _compactor_executor_id{0};
        std::atomic_size_t _compaction_jobs_in_flight{0};

        std::shared_ptr<sharded_page_cache> _page_cache;

        std::mutex _pending_compactions_mutex;
        int64_t _pending_compacting_sst_for_backpressure_count{0};

        std::mutex _total_pending_compactions_mutex;
        int64_t _total_pending_compacting_sst_count{0};
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

        // Manifest related methods
        inline const static std::string MANIFEST_FILENAME = "manifest";
        static std::string _serialize_levels(const partition_t& levels);
        tmc::task<hedge::status> _persist_partition_state(uint16_t partition_id);

        // Compaction related types
        using compaction_bucket_t = std::vector<sst_ptr_t>;                                                // A bucket is a group of SSTs
        using compaction_buckets_t = std::multimap<size_t, compaction_bucket_t>;                           // level_id -> list of buckets (each bucket is a list of SSTs)
        using compaction_buckets_per_partition_t = std::vector<std::pair<uint16_t, compaction_buckets_t>>; // partition_id -> buckets (per level)

        // Compaction related methods
        tmc::chan_tok<bool> _compaction_scheduler_signal_chan = tmc::make_channel<bool>();

        void _update_pending_compactions_counter(int32_t count);

        tmc::task<void> _make_compaction_task(
            std::shared_ptr<tmc::semaphore> can_write,
            std::shared_ptr<tmc::semaphore> next_can_write,
            size_t level,
            std::vector<sst_ptr_t> inputs,
            bool discard_deleted_keys);

        hedge::expected<compaction_stats> _schedule_compaction(bool compact_all);

        static void create_buckets_from_level(
            compaction_buckets_t& out_buckets,
            size_t level_idx,
            const level_t& level,
            size_t min_merge_width,
            size_t max_merge_width);
        static compaction_buckets_per_partition_t pick_ssts_into_buckets(
            const sst_manager::partition_snapshot_map_t& index_snapshot,
            size_t min_merge_width,
            size_t max_merge_width);
        void launch_compaction_tasks(
            const partition_snapshot_map_t& index_snapshot,
            compaction_buckets_per_partition_t&& buckets_per_partition,
            size_t min_merge_width,
            size_t max_merge_width);

        // Check helpers
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
