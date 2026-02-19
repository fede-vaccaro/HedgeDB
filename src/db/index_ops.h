#pragma once

#include <cstdlib>

#include "cache.h"
#include "key.h"
#include "memtable.h"
#include "page_aligned_buffer.h"
#include "sorted_index.h"
#include "sst.h"
#include "types.h"

namespace hedge::db
{

    struct index_ops
    {

        // Constants
        static constexpr size_t SUPER_INDEX_ENABLED_THRESHOLD = 16;

        struct sst_footer_builder
        {
            std::optional<uint64_t> upper_bound{};
            std::optional<uint64_t> indexed_kv{};
            std::optional<uint64_t> meta_index_entries{};
            std::optional<uint64_t> index_offset{};
            std::optional<uint64_t> meta_index_offset{};
            std::optional<uint64_t> footer_offset{};
            std::optional<uint64_t> epoch{};

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

                return sst_footer{
                    .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                    .upper_bound = this->upper_bound.value(),
                    .indexed_keys = this->indexed_kv.value(),
                    .meta_index_entries = this->meta_index_entries.value(),
                    .index_offset = this->index_offset.value(),
                    .meta_index_offset = this->meta_index_offset.value(),
                    .footer_offset = this->footer_offset.value(),
                    .epoch = this->epoch.value()};
            }
        };

        template <typename T, size_t PAGE_SIZE = PAGE_SIZE_IN_BYTES>
        static page_aligned_buffer<key_t> create_super_index(const page_aligned_buffer<key_t>& meta_index);

        static void append_meta_index_key(page_aligned_buffer<uint8_t>& buffer, const std::span<const uint8_t>& key_span)
        {
            size_t buf_size = buffer.size();

            // Extend buffer size
            buffer.resize(buf_size + key_span.size() + 1); // +1 for key size byte

            // Write key size
            write_key_unsafe(buffer.data(), key_span);
        }

        static hedge::expected<sorted_index> load_sorted_index(const std::filesystem::path& path, bool use_direct, bool load_index = false);

        static hedge::expected<sorted_index> save_as_sorted_index(
            const std::filesystem::path& path,
            page_aligned_buffer<index_entry_t>&& sorted_keys,
            size_t upper_bound,
            size_t epoch,
            const std::shared_ptr<db::sharded_page_cache>& cache,
            bool use_odirect);

        static hedge::expected<sst> save_as_sorted_index2(
            const std::filesystem::path& path,
            page_aligned_buffer<index_entry2_t>&& sorted_keys,
            size_t average_key_length,
            size_t upper_bound,
            size_t epoch,
            const std::shared_ptr<db::sharded_page_cache>& cache,
            bool use_odirect);

        static hedge::expected<std::vector<sorted_index>> flush_mem_index(const std::filesystem::path& base_path,
                                                                          memtable_impl_t* index,
                                                                          size_t num_partition_exponent,
                                                                          size_t flush_iteration,
                                                                          const std::shared_ptr<db::sharded_page_cache>& cache,
                                                                          bool use_odirect = false);

        static hedge::expected<std::vector<sst>> flush_mem_index2(const std::filesystem::path& base_path,
                                                                  memtable_impl2_t* index,
                                                                  size_t num_partition_exponent,
                                                                  size_t flush_iteration,
                                                                  const std::shared_ptr<db::sharded_page_cache>& cache,
                                                                  bool use_odirect = false);

        struct merge_config
        {
            size_t read_ahead_size{};              ///< Number of bytes to read from each input index file at a time during the merge. (e.g., 64 * 1024 for 64KB chunks).
            size_t new_index_id{};                 ///< The unique ID (iteration number) to use for the output merged index file name (e.g., ".<new_index_id>").
            std::filesystem::path base_path{};     ///< The base directory where the output file will be created (within its partition subdirectory).
            bool discard_deleted_keys{false};      ///< If `true`, entries marked with the delete flag (`value_ptr_t::is_deleted()`) will not be written to the output file.
                                                   ///< Set `false` if there are more indices belonging to the same partition to be merged later, to preserve delete markers until the final merge.
                                                   ///< Set `true` when this is the final merge for the partition to eliminate deleted entries from the final index.
            bool create_new_with_odirect{false};   ///< If `true`, opens the output file with O_DIRECT flag for direct I/O access.
            bool populate_cache_with_output{true}; ///< If `true`, tries to fill the cache with the resulting sorted index
            bool try_reading_from_cache{false};    ///< If `true`, attempts to read input index pages from the shared page cache before issuing disk reads.
        };

        static async::task<hedge::expected<sorted_index>> k_way_merge_async(
            const merge_config& config,
            const std::vector<const sorted_index*>& indices,
            const std::shared_ptr<async::executor_context>& executor,
            const std::shared_ptr<db::sharded_page_cache>& cache);

        static async::task<hedge::expected<sst>> k_way_merge_async2(
            const merge_config& config,
            const std::vector<const sst*>& indices,
            const std::shared_ptr<async::executor_context>& executor,
            std::shared_ptr<db::sharded_page_cache> cache);
    };

} // namespace hedge::db