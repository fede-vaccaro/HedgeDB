#include <algorithm>
#include <cstdint>
#include <error.hpp>

#include "db/block.h"
#include "db/sorted_index.h"
#include "index_ops.h"
#include "page_aligned_buffer.h"
#include "sst.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    hedge::expected<std::vector<sst>> index_ops::flush_mem_index2(const std::filesystem::path& base_path,
                                                                  memtable_impl2_t* index,
                                                                  size_t num_partition_exponent,
                                                                  size_t flush_iteration,
                                                                  const std::shared_ptr<db::sharded_page_cache>& cache,
                                                                  bool use_odirect)
    {
        if(num_partition_exponent > 16)
            return hedge::error("Number of partitions exponent must be less than or equal to 16");

        std::vector<sst> resulting_indices; // To store the created sorted_index objects.

        constexpr double SLACK = 0.5; // Slack factor to avoid the need of reallocation.
        size_t partition_estimated_size;

        if(num_partition_exponent == 0)
            partition_estimated_size = index->size();
        else
            partition_estimated_size = ((double)index->size() / (1 << num_partition_exponent) + 1.0) * (1.0 + SLACK);

        partition_estimated_size = std::max(partition_estimated_size, PAGE_SIZE_IN_BYTES / sizeof(index_entry2_t));

        page_aligned_buffer<index_entry2_t> current_partition_entries(0, partition_estimated_size); // Temp buffer for entries of the current partition.
        size_t curr_partition_count{0};
        size_t sum_of_key_lengths{0};

        // Calculate the range of key prefixes per partition.
        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        // Determine the partition ID for the very first key.
        size_t current_partition_id = hedge::find_partition_prefix_for_key(index->begin().key(), partition_key_prefix_range);

        // Step 2: Iterate through the sorted list, grouping entries by partition ID.
        // TODO: Handle potential errors during file writing more gracefully (e.g., cleanup).
        for(auto it = index->begin(); it != index->end(); /* Incr happens in loop */)
        {
            current_partition_entries.emplace_back(it.key(), it.value());
            curr_partition_count++;
            sum_of_key_lengths += it.key().size();

            // Incr happens here
            auto curr = it;
            ++it;

            // Check if this is the last entry for the current partition.
            bool is_last_in_list = (it == index->end());

            size_t next_partition_id{};
            bool partition_changes = false;

            if(!is_last_in_list)
            {
                // Safely look ahead to the next key's partition ID.
                next_partition_id = hedge::find_partition_prefix_for_key(it.key(), partition_key_prefix_range);
                partition_changes = (next_partition_id != current_partition_id);
            }

            if(is_last_in_list || partition_changes)
            {
                // End of the current partition reached. Write its entries to a file.
                auto [dir_prefix, file_prefix] = format_prefix(current_partition_id);
                auto dir_path = base_path / dir_prefix;
                std::filesystem::create_directories(dir_path);

                auto path = dir_path / with_extension(file_prefix, std::format(".{}", flush_iteration));

                current_partition_entries.resize(curr_partition_count);

                auto maybe_sst = index_ops::save_as_sorted_index2(
                    path,
                    std::exchange(current_partition_entries, page_aligned_buffer<index_entry2_t>(0, partition_estimated_size)),
                    hedge::ceil(sum_of_key_lengths, curr_partition_count),
                    current_partition_id,
                    flush_iteration,
                    cache,
                    use_odirect);

                if(!maybe_sst)
                    return hedge::error("Error while creating sst file: " + maybe_sst.error().to_string());

                curr_partition_count = 0;
                sum_of_key_lengths = 0;

                resulting_indices.push_back(std::move(maybe_sst.value()));

                current_partition_id = next_partition_id;
            }
        }

        // TODO: Implement WAL clearing logic upon successful flush.
        // Well, WAL should be implemented first...

        return resulting_indices;
    }

    template <typename T, size_t PAGE_SIZE>
    page_aligned_buffer<key_t> index_ops::create_super_index(const page_aligned_buffer<key_t>& meta_index)
    {
        // Calculate how many meta-index entries are needed (one per page).
        constexpr size_t ENTRIES_PER_PAGE = PAGE_SIZE / sizeof(T);

        auto super_index_size = hedge::ceil(meta_index.size(), ENTRIES_PER_PAGE);
        auto super_index = page_aligned_buffer<key_t>(super_index_size);

        // Iterate through each conceptual page.
        for(size_t i = 0; i < super_index_size; ++i)
        {
            auto idx = std::min(((i + 1) * ENTRIES_PER_PAGE) - 1, meta_index.size() - 1);
            super_index[i] = meta_index[idx];
        }

        return super_index;
    }

    template page_aligned_buffer<key_t> index_ops::create_super_index<key_t, PAGE_SIZE_IN_BYTES>(const page_aligned_buffer<key_t>&);

    struct tmp_in_mem_sst
    {
        page_aligned_buffer<uint8_t> index;
        page_aligned_buffer<uint8_t> meta_index_bytes;
        page_aligned_buffer<key_t> meta_index;
    };

    tmp_in_mem_sst prepare_in_mem(const page_aligned_buffer<index_entry2_t>& sorted_entries, size_t average_key_length)
    {
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_length + sizeof(value_ptr_t));
        size_t pages_estimate = hedge::ceil(sorted_entries.size(), key_values_per_page_estimate);

        page_aligned_buffer<uint8_t> index(PAGE_SIZE_IN_BYTES, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<uint8_t> meta_index_bytes(0, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<key_t> meta_index(0, pages_estimate);
        size_t pages_written = 0;
        size_t total_entries_written = 0;

        block_encoder bb(index.data());

        while(total_entries_written < sorted_entries.size())
        {
            const auto& entry = sorted_entries[total_entries_written];

            auto s = bb.push(entry.key, entry.value_ptr);

            if(!s && s.error().code() == errc::BUFFER_FULL)
            {
                // The meta index key is the last key of the current block.
                // This way calling std::lower_bound on the meta index will give us the correct block for the key we're looking for.
                index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
                meta_index.emplace_back(bb.last_pushed_key());
                assert(bb.committed());

                // Allocate space for the next block if needed
                constexpr double SLACK = 0.15;
                if(index.size() == index.capacity())
                    index.reserve(index.size() * (1.0 + SLACK)); 

                pages_written++;
                index.resize(index.size() + PAGE_SIZE_IN_BYTES);
                bb.reset(index.data() + (PAGE_SIZE_IN_BYTES * pages_written));

                continue; // Retry pushing the same entry in the new block
            }

            total_entries_written++;
        }

        if(bb.kv_count() == 0)
        {
            // Handle the case where the last block is empty (which can happen if the last entry perfectly fills a block).
            index.resize(pages_written * PAGE_SIZE_IN_BYTES);
        }
        else
        {
            // Handle the case where the last block wasn't committed yet
            index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
            meta_index.emplace_back(bb.last_pushed_key());
            bb.commit(); // Commit is automatic when the buffer is full
        }

        index.shrink_to_fit();
        meta_index_bytes.shrink_to_fit();
        meta_index.shrink_to_fit();

        return tmp_in_mem_sst{
            .index = std::move(index),
            .meta_index_bytes = std::move(meta_index_bytes),
            .meta_index = std::move(meta_index)};
    }

    template <typename T>
    hedge::expected<size_t> pwrite_buffer(int fd, const page_aligned_buffer<T>& buffer, size_t offset)
    {
        const size_t bytes_to_write = hedge::round_up(buffer.size() * sizeof(T), PAGE_SIZE_IN_BYTES);

        int res = pwrite(fd,
                         buffer.data(),
                         bytes_to_write,
                         offset);

        if(res < 0)
            return hedge::error(std::format("Failed to write iovec to fd {} at offset {}: {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            strerror(errno)));

        if(static_cast<size_t>(res) != bytes_to_write)
            return hedge::error(std::format("Partial write occurred when writing iovec to fd {} at offset {}: wrote {} bytes out of {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            res,
                                            bytes_to_write));

        return res;
    }

    hedge::expected<sst> index_ops::save_as_sorted_index2(
        const std::filesystem::path& path,
        page_aligned_buffer<index_entry2_t>&& keys,
        size_t average_key_length,
        size_t upper_bound,
        size_t epoch,
        const std::shared_ptr<db::sharded_page_cache>& cache,
        bool use_odirect)
    {
        auto sorted_keys = std::move(keys);
        size_t num_keys = sorted_keys.size();
        tmp_in_mem_sst in_mem = prepare_in_mem(sorted_keys, average_key_length);
        sorted_keys.free(); // Not needed anymore, release memory

        const size_t sst_size =
            in_mem.index.size() + // Already page-aligned
            hedge::round_up(in_mem.meta_index_bytes.size(), PAGE_SIZE_IN_BYTES) +
            hedge::round_up(sizeof(sst_footer), PAGE_SIZE_IN_BYTES);

        assert(is_page_aligned(sst_size));

        // Create and open SST
        auto maybe_sorted_index_file = fs::file::from_path(
            path,
            fs::file::open_mode::read_write_new,
            use_odirect,
            sst_size);

        if(!maybe_sorted_index_file)
            return hedge::error("Failed to create sorted index file: " + maybe_sorted_index_file.error().to_string());

        auto fd = maybe_sorted_index_file.value().fd();

        // Initialize footer
        sst_footer_builder fbuilder{};
        fbuilder.indexed_kv = num_keys;
        fbuilder.upper_bound = upper_bound;
        fbuilder.index_offset = 0;
        fbuilder.meta_index_entries = in_mem.meta_index.size();
        fbuilder.epoch = epoch;

        size_t bytes_written = 0;

        // Write main index
        auto maybe_res = pwrite_buffer(fd, in_mem.index, 0);
        if(!maybe_res)
            return hedge::error("Failed to write index data to sorted index file: " + maybe_res.error().to_string());
        bytes_written += maybe_res.value();
        fbuilder.meta_index_offset = bytes_written;

        // Write meta-index
        maybe_res = pwrite_buffer(fd, in_mem.meta_index_bytes, *fbuilder.meta_index_offset);
        if(!maybe_res)
            return hedge::error("Failed to write meta-index data to sorted index file: " + maybe_res.error().to_string());
        bytes_written += maybe_res.value();
        fbuilder.footer_offset = bytes_written;
        in_mem.meta_index_bytes.free(); // Release memory

        // Build footer
        auto maybe_footer = fbuilder.build();
        if(!maybe_footer)
            return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        const sst_footer& footer = maybe_footer.value();
        page_aligned_buffer<sst_footer> sorted_index_footer_buf(1);
        sorted_index_footer_buf[0] = footer;

        // Write footer
        maybe_res = pwrite_buffer(fd, sorted_index_footer_buf, *fbuilder.footer_offset);
        if(!maybe_res)
            return hedge::error("Failed to write footer data to sorted index file: " + maybe_res.error().to_string());
        assert(bytes_written + maybe_res.value() == sst_size);

        // Prepopulate cache with the index blocks
        if(cache != nullptr)
        {
            size_t curr_offset = 0;
            auto pages = cache->get_write_slots_range(maybe_sorted_index_file.value().id(), 0, footer.meta_index_offset / PAGE_SIZE_IN_BYTES);
            for(auto& page : pages)
            {
                std::memcpy(page.data + page.idx, in_mem.index.data() + curr_offset, PAGE_SIZE_IN_BYTES);
                curr_offset += PAGE_SIZE_IN_BYTES;
            }
        }
        in_mem.index.free(); // Release memory

        constexpr size_t REF_PAGE_SIZE = PAGE_SIZE_IN_BYTES;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        // Super index enabled if there would be 512 * 256 = 131072 meta index entries (2MB)
        // In this case the super index would be 512 * 16 bytes = 8KB
        std::optional<page_aligned_buffer<key_t>> super_index;

        if(in_mem.meta_index.size() > KEYS_PER_META_INDEX_PAGE * index_ops::SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(in_mem.meta_index);

        // print meta index
        // std::cout << "Meta index:" << std::endl;
        // for(const auto& key : in_mem.meta_index)
        //     std::cout << to_hex_string(key) << std::endl;

        // Create the final object, moving the key and meta-index data into it.
        auto ss = sst(std::move(maybe_sorted_index_file.value()), std::move(in_mem.meta_index), footer, std::move(super_index));

        return ss;
    }

} // namespace hedge::db