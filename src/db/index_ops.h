#pragma once

#include <cstdlib>

#include "cache.h"
#include "key.h"
#include "memtable.h"
#include "page_aligned_buffer.h"
#include "sst.h"
#include "tmc/task.hpp"
#include "types.h"

namespace tmc
{
    class ex_cpu;
}

namespace hedge::db
{

    struct index_ops
    {
        struct sst_footer_builder
        {
            std::optional<uint64_t> upper_bound{};
            std::optional<uint64_t> indexed_kv{};
            std::optional<uint64_t> meta_index_entries{};
            std::optional<uint64_t> index_offset{};
            std::optional<uint64_t> meta_index_offset{};
            std::optional<uint64_t> qf_offset{};
            std::optional<uint64_t> qf_size{};
            std::optional<uint64_t> footer_offset{};
            std::optional<uint64_t> epoch{};
            std::optional<uint64_t> max_seq_nr{};

            hedge::expected<sst_footer> build()
            {
                // Simple validation: ensure all fields have been assigned a value.
                if(!this->upper_bound.has_value())
                    return hedge::error("Footer upper_bound not set");
                if(!this->indexed_kv.has_value())
                    return hedge::error("Footer indexed_keys not set");
                if(!this->meta_index_entries.has_value())
                    return hedge::error("Footer meta_index_entries not set");
                if(!this->index_offset.has_value())
                    return hedge::error("Footer index_offset not set");
                if(!this->meta_index_offset.has_value())
                    return hedge::error("Footer meta_index_offset not set");
                if(!this->footer_offset.has_value())
                    return hedge::error("Footer footer_offset not set");
                if(!this->epoch.has_value())
                    return hedge::error("Footer epoch not set");
                if(!this->max_seq_nr.has_value())
                    return hedge::error("Footer max_seq_nr not set");

                return sst_footer{
                    .version = sst_footer::CURRENT_FOOTER_VERSION,
                    .upper_bound = this->upper_bound.value(),
                    .indexed_keys = this->indexed_kv.value(),
                    .meta_index_entries = this->meta_index_entries.value(),
                    .index_offset = this->index_offset.value(),
                    .meta_index_offset = this->meta_index_offset.value(),
                    .qf_offset = this->qf_offset.value_or(0),
                    .qf_size = this->qf_size.value_or(0),
                    .footer_offset = this->footer_offset.value(),
                    .epoch = this->epoch.value(),
                    .max_seq_nr = this->max_seq_nr.value()};
            }
        };

        template <typename T, size_t PAGE_SIZE = PAGE_SIZE_IN_BYTES>
        static page_aligned_buffer<key_t> create_super_index(const page_aligned_buffer<key_t>& meta_index);

        static void append_meta_index_key(page_aligned_buffer<std::byte>& buffer, const std::span<const std::byte>& key_span)
        {
            const size_t old_buf_size = buffer.size();

            // Extend buffer size
            buffer.grow_uninitialized(key_span.size() + 1); // +1 for key size byte

            // Write key size
            write_key_unsafe(buffer.data() + old_buf_size, key_span);
        }

        struct partition_range
        {
            size_t partition_id;
            skiplist_t::Accessor::const_iterator begin;
            skiplist_t::Accessor::const_iterator end;
            size_t count;
            size_t sum_key_value_lengths;
        };

        static tmc::task<hedge::expected<std::vector<sst>>> flush_memtable(
            std::filesystem::path base_path,
            skiplist_t::Accessor::const_iterator begin,
            skiplist_t::Accessor::const_iterator end,
            size_t num_partition_exponent,
            size_t flush_iteration,
            std::shared_ptr<db::sharded_page_cache> cache,
            bool use_odirect,
            tmc::ex_cpu& flush_executor);

        struct merge_config
        {
            size_t read_ahead_size{};              ///< Number of bytes to read from each input index file at a time during the merge. (e.g., 64 * 1024 for 64KB chunks).
            size_t new_index_id{};                 ///< The unique ID (iteration number) to use for the output merged index file name (e.g., ".<new_index_id>").
            std::filesystem::path base_path{};     ///< The base directory where the output file will be created (within its partition subdirectory).
            bool discard_deleted_keys{false};      ///< If `true`, entries carrying a `tombstone_t` value will not be written to the output file.
                                                   ///< Set `false` if there are more indices belonging to the same partition to be merged later, to preserve tombstones until the final merge.
                                                   ///< Set `true` when this is the final merge for the partition to eliminate tombstones and reclaim space.
            bool create_new_with_odirect{false};   ///< If `true`, opens the output file with O_DIRECT flag for direct I/O access.
            bool populate_cache_with_output{true}; ///< If `true`, tries to fill the cache with the resulting sorted index
        };

        static tmc::task<hedge::expected<sst>> k_way_merge_async2(
            const merge_config& config,
            const std::vector<const sst*>& indices,
            std::shared_ptr<db::sharded_page_cache> cache);
    };

} // namespace hedge::db
