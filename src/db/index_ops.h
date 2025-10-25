#pragma once

#include "mem_index.h"
#include "sorted_index.h"
#include "types.h"

namespace hedge::db
{

    struct index_ops
    {
        static std::vector<index_entry_t> merge_memtables_in_mem(std::vector<mem_index>&& indices);
        static hedge::expected<sorted_index> load_sorted_index(const std::filesystem::path& path, bool load_index = false);
        static hedge::expected<sorted_index> save_as_sorted_index(const std::filesystem::path& path, std::vector<index_entry_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent = false);
        static hedge::expected<std::vector<sorted_index>> flush_mem_index(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent, size_t flush_iteration);

        struct merge_config
        {
            size_t read_ahead_size{};
            size_t new_index_id{};
            std::filesystem::path base_path{};
            bool discard_deleted_keys{false};
        };

        static async::task<hedge::expected<sorted_index>> two_way_merge_async(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);
        static hedge::expected<sorted_index> two_way_merge(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedge::db