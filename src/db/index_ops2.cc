#include <algorithm>
#include <cstdint>
#include <error.hpp>
#include <mutex>

#include "async/wait_group.h"
#include "db/block.h"
#include "db/sorted_index.h"
#include "generator.h"
#include "index_ops.h"
#include "page_aligned_buffer.h"
#include "sst.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    hedge::expected<std::vector<sst>> index_ops::flush_mem_index2(const std::filesystem::path& base_path,
                                                                  memtable_impl3_t* index,
                                                                  size_t num_partition_exponent,
                                                                  size_t flush_iteration,
                                                                  const std::shared_ptr<db::sharded_page_cache>& cache,
                                                                  bool use_odirect)
    {
        // std::cout << "Flushing epoch " << flush_iteration << ": memtable size: " << index->size() << "\n";

        if(num_partition_exponent > 16)
            return hedge::error("Number of partitions exponent must be less than or equal to 16");

        std::vector<sst> resulting_indices; // To store the created sorted_index objects.

        constexpr double SLACK = 0.5; // Slack factor to avoid the need of reallocation.
        size_t partition_estimated_size;

        size_t index_size = index->size();

        if(num_partition_exponent == 0)
            partition_estimated_size = index_size;
        else
            partition_estimated_size = ((double)index_size / (1 << num_partition_exponent) + 1.0) * (1.0 + SLACK);

        partition_estimated_size = std::max(partition_estimated_size, PAGE_SIZE_IN_BYTES / sizeof(index_entry2_t));

        page_aligned_buffer<index_entry2_t> current_partition_entries(0, partition_estimated_size); // Temp buffer for entries of the current partition.
        size_t curr_partition_count{0};
        size_t sum_of_key_value_lengths{0};

        // Calculate the range of key prefixes per partition.
        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        // Determine the partition ID for the very first key.
        auto accessor = index->accessor();
        size_t current_partition_id = 0;
        if(index_size > 0)
            current_partition_id = hedge::find_partition_prefix_for_key(accessor.begin()->key, partition_key_prefix_range);
        size_t previous_partition_id = -1;

        // Step 2: Iterate through the sorted list, grouping entries by partition ID.
        // TODO: Handle potential errors during file writing more gracefully (e.g., cleanup).
        for(auto it = accessor.begin(); it != accessor.end(); /* Incr happens in loop */)
        {
            current_partition_entries.emplace_back(it->key, it->value);
            curr_partition_count++;
            sum_of_key_value_lengths += it->key.size() + it->value.size();

            // Incr happens here
            ++it;

            // Check if this is the last entry for the current partition.
            bool is_last_in_list = (it == accessor.end());

            size_t next_partition_id{};
            bool partition_changes = false;

            if(!is_last_in_list)
            {
                // Safely look ahead to the next key's partition ID.
                next_partition_id = hedge::find_partition_prefix_for_key(it->key, partition_key_prefix_range);
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

                auto t0 = std::chrono::high_resolution_clock::now();
                auto maybe_sst = index_ops::save_as_sorted_index2(
                    path,
                    std::exchange(current_partition_entries, page_aligned_buffer<index_entry2_t>(0, partition_estimated_size)),
                    hedge::ceil(sum_of_key_value_lengths, curr_partition_count),
                    current_partition_id,
                    flush_iteration,
                    cache,
                    use_odirect);
                auto t1 = std::chrono::high_resolution_clock::now();
                [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
                // std::cout << "Flushed partition " << current_partition_id << " with " << curr_partition_count << " entries and average key-value length " << (sum_of_key_value_lengths / curr_partition_count) << " in " << duration.count() << " ms\n";

                if(!maybe_sst)
                {
                    // throw std::runtime_error("Error while creating sst file: " + maybe_sst.error().to_string() + " current prefix" + std::to_string(current_partition_id) + " next prefix " + std::to_string(next_partition_id) + " partition count " + std::to_string(curr_partition_count));
                    // throw a runtime error and print any relevant stack information:
                    std::string error = "Error while creating sst file: " + maybe_sst.error().to_string() +
                                        " previous prefix" + std::to_string(previous_partition_id) +
                                        " current prefix" + std::to_string(current_partition_id) +
                                        " next prefix " + std::to_string(next_partition_id) +
                                        " partition count " + std::to_string(curr_partition_count) +
                                        " index_size before: " + std::to_string(index_size) +
                                        " index_size now: " + std::to_string(index->size());
                    throw std::runtime_error(error);
                } // return hedge::error("Error while creating sst file: " + maybe_sst.error().to_string());

                curr_partition_count = 0;
                sum_of_key_value_lengths = 0;

                resulting_indices.push_back(std::move(maybe_sst.value()));

                previous_partition_id = current_partition_id;
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

    tmp_in_mem_sst prepare_in_mem(const page_aligned_buffer<index_entry2_t>& sorted_entries, size_t average_key_value_length)
    {
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_value_length);
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

            auto s = bb.push(entry.key, entry.value);

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
        size_t average_key_value_length,
        size_t upper_bound,
        size_t epoch,
        const std::shared_ptr<db::sharded_page_cache>& cache,
        bool use_odirect)
    {
        auto sorted_keys = std::move(keys);
        size_t num_keys = sorted_keys.size();
        tmp_in_mem_sst in_mem = prepare_in_mem(sorted_keys, average_key_value_length);
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

    static async::generator<index_ops::partition_range> partition_ranges_generator(
        memtable_impl3_t* index,
        size_t num_partition_exponent)
    {
        std::vector<index_ops::partition_range> ranges;

        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        auto accessor = index->accessor();

        if(index->size() == 0)
            co_return;

        size_t current_partition_id = hedge::find_partition_prefix_for_key(accessor.begin()->key, partition_key_prefix_range);
        auto range_begin = accessor.begin();
        size_t count = 0;
        size_t sum_kv_len = 0;

        for(auto it = accessor.begin(); it != accessor.end(); ++it)
        {
            size_t part_id = hedge::find_partition_prefix_for_key(it->key, partition_key_prefix_range);

            if(part_id != current_partition_id)
            {
                co_yield index_ops::partition_range{
                    .partition_id = current_partition_id,
                    .begin = range_begin,
                    .end = it,
                    .count = count,
                    .sum_key_value_lengths = sum_kv_len,
                };
                current_partition_id = part_id;
                range_begin = it;
                count = 0;
                sum_kv_len = 0;
            }

            count++;
            sum_kv_len += it->key.size() + it->value.size();
        }

        // Last partition
        co_yield index_ops::partition_range{
            .partition_id = current_partition_id,
            .begin = range_begin,
            .end = accessor.end(),
            .count = count,
            .sum_key_value_lengths = sum_kv_len,
        };

        co_return;
    }

    static tmp_in_mem_sst prepare_in_mem_from_range(
        skiplist_t::Accessor::iterator begin,
        skiplist_t::Accessor::iterator end,
        size_t entry_count,
        size_t average_key_value_length)
    {
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_value_length);
        size_t pages_estimate = hedge::ceil(entry_count, key_values_per_page_estimate);

        page_aligned_buffer<uint8_t> index(PAGE_SIZE_IN_BYTES, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<uint8_t> meta_index_bytes(0, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<key_t> meta_index(0, pages_estimate);
        size_t pages_written = 0;

        block_encoder bb(index.data());

        auto it = begin;
        while(it != end)
        {
            auto s = bb.push(it->key, it->value);

            if(!s && s.error().code() == errc::BUFFER_FULL)
            {
                index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
                meta_index.emplace_back(bb.last_pushed_key());
                assert(bb.committed());

                constexpr double SLACK = 0.15;
                if(index.size() == index.capacity())
                    index.reserve(index.size() * (1.0 + SLACK));

                pages_written++;
                index.resize(index.size() + PAGE_SIZE_IN_BYTES);
                bb.reset(index.data() + (PAGE_SIZE_IN_BYTES * pages_written));

                continue;
            }

            ++it;
        }

        if(bb.kv_count() == 0)
        {
            index.resize(pages_written * PAGE_SIZE_IN_BYTES);
        }
        else
        {
            index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
            meta_index.emplace_back(bb.last_pushed_key());
            bb.commit();
        }

        meta_index_bytes.shrink_to_fit();
        meta_index.shrink_to_fit();

        return tmp_in_mem_sst{
            .index = std::move(index),
            .meta_index_bytes = std::move(meta_index_bytes),
            .meta_index = std::move(meta_index)};
    }

    static async::task<hedge::expected<sst>> save_as_sorted_index2_async(
        std::filesystem::path path,
        skiplist_t::Accessor::iterator begin,
        skiplist_t::Accessor::iterator end,
        size_t entry_count,
        size_t average_key_value_length,
        size_t upper_bound,
        size_t epoch,
        std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect)
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        tmp_in_mem_sst in_mem = prepare_in_mem_from_range(begin, end, entry_count, average_key_value_length);
        auto t1 = std::chrono::high_resolution_clock::now();
        [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        // std::cout << "Prepared in-memory structures for SST flush in " << duration.count() << " ms\n";

        const size_t sst_size =
            in_mem.index.size() +
            hedge::round_up(in_mem.meta_index_bytes.size(), PAGE_SIZE_IN_BYTES) +
            hedge::round_up(sizeof(sst_footer), PAGE_SIZE_IN_BYTES);

        assert(is_page_aligned(sst_size));

        auto executor = async::this_thread_executor();

        auto maybe_file = co_await fs::file::from_path_async(
            path,
            fs::file::open_mode::read_write_new,
            executor,
            use_odirect,
            sst_size);

        if(!maybe_file)
            co_return hedge::error("Failed to create sorted index file: " + maybe_file.error().to_string());

        auto fd = maybe_file.value().fd();

        // Build footer
        index_ops::sst_footer_builder fbuilder{};
        fbuilder.indexed_kv = entry_count;
        fbuilder.upper_bound = upper_bound;
        fbuilder.index_offset = 0;
        fbuilder.meta_index_entries = in_mem.meta_index.size();
        fbuilder.epoch = epoch;

        size_t bytes_written = 0;

        // Write main index
        size_t index_write_size = hedge::round_up(in_mem.index.size(), PAGE_SIZE_IN_BYTES);
        auto index_write_res = co_await executor->submit_request(async::write_request{
            .fd = fd,
            .data = in_mem.index.data(),
            .size = index_write_size,
            .offset = 0});

        if(index_write_res.error_code < 0)
            co_return hedge::error("Failed to write index data: " + std::string(strerror(-index_write_res.error_code)));

        bytes_written += index_write_res.bytes_written;
        fbuilder.meta_index_offset = bytes_written;

        // Write meta-index
        size_t meta_write_size = hedge::round_up(in_mem.meta_index_bytes.size(), PAGE_SIZE_IN_BYTES);
        auto meta_write_res = co_await executor->submit_request(async::write_request{
            .fd = fd,
            .data = in_mem.meta_index_bytes.data(),
            .size = meta_write_size,
            .offset = *fbuilder.meta_index_offset});

        if(meta_write_res.error_code < 0)
            co_return hedge::error("Failed to write meta-index data: " + std::string(strerror(-meta_write_res.error_code)));

        bytes_written += meta_write_res.bytes_written;
        fbuilder.footer_offset = bytes_written;
        in_mem.meta_index_bytes.free();

        // Build and write footer
        auto maybe_footer = fbuilder.build();
        if(!maybe_footer)
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        const sst_footer& footer = maybe_footer.value();
        page_aligned_buffer<sst_footer> footer_buf(1);
        footer_buf[0] = footer;

        size_t footer_write_size = hedge::round_up(sizeof(sst_footer), PAGE_SIZE_IN_BYTES);
        auto footer_write_res = co_await executor->submit_request(async::write_request{
            .fd = fd,
            .data = reinterpret_cast<uint8_t*>(footer_buf.data()),
            .size = footer_write_size,
            .offset = *fbuilder.footer_offset});

        if(footer_write_res.error_code < 0)
            co_return hedge::error("Failed to write footer: " + std::string(strerror(-footer_write_res.error_code)));

        assert(bytes_written + footer_write_res.bytes_written == sst_size);

        // Prepopulate cache
        if(cache != nullptr)
        {
            size_t curr_offset = 0;
            auto pages = cache->get_write_slots_range(maybe_file.value().id(), 0, footer.meta_index_offset / PAGE_SIZE_IN_BYTES);
            for(auto& page : pages)
            {
                std::memcpy(page.data + page.idx, in_mem.index.data() + curr_offset, PAGE_SIZE_IN_BYTES);
                curr_offset += PAGE_SIZE_IN_BYTES;
            }
        }
        in_mem.index.free();

        constexpr size_t REF_PAGE_SIZE = PAGE_SIZE_IN_BYTES;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        std::optional<page_aligned_buffer<key_t>> super_index;
        if(in_mem.meta_index.size() > KEYS_PER_META_INDEX_PAGE * index_ops::SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(in_mem.meta_index);

        co_return sst(std::move(maybe_file.value()), std::move(in_mem.meta_index), footer, std::move(super_index));
    }

    static async::task<void> flush_partition_task(
        std::filesystem::path path,
        skiplist_t::Accessor::iterator range_begin,
        skiplist_t::Accessor::iterator range_end,
        size_t entry_count,
        size_t sum_kv_len,
        size_t partition_id,
        size_t flush_iteration,
        std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect,
        std::mutex* results_mutex,
        std::vector<sst>* results,
        std::vector<hedge::error>* errors,
        std::shared_ptr<async::wait_group> wg)
    {
        size_t avg_kv_len = hedge::ceil(sum_kv_len, entry_count);

        auto maybe_sst = co_await save_as_sorted_index2_async(
            std::move(path),
            range_begin,
            range_end,
            entry_count,
            avg_kv_len,
            partition_id,
            flush_iteration,
            std::move(cache),
            use_odirect);

        {
            std::lock_guard lk(*results_mutex);
            if(maybe_sst)
                results->push_back(std::move(maybe_sst.value()));
            else
                errors->emplace_back(maybe_sst.error());
        }

        wg->decr();
    }

    hedge::expected<std::vector<sst>> index_ops::flush_mem_index2_parallel(
        const std::filesystem::path& base_path,
        memtable_impl3_t* index,
        size_t num_partition_exponent,
        size_t flush_iteration,
        const std::shared_ptr<db::sharded_page_cache>& cache,
        bool use_odirect,
        const std::vector<std::shared_ptr<async::executor_context>>& executor_pool)
    {
        if(num_partition_exponent > 16)
            return hedge::error("Number of partitions exponent must be less than or equal to 16");

        auto ranges = partition_ranges_generator(index, num_partition_exponent);

        auto wg = async::wait_group::make_shared();
        wg->set(1 << num_partition_exponent); // Max number of partitions, i.e. tasks

        std::mutex results_mutex;
        std::vector<sst> results;
        std::vector<hedge::error> errors;

        size_t executor_id = 0;
        size_t range_count = 0;
        for(auto& range : ranges)
        {
            // Create directories synchronously (cheap, bounded count)
            auto [dir_prefix, file_prefix] = format_prefix(range.partition_id);
            auto dir_path = base_path / dir_prefix;
            std::filesystem::create_directories(dir_path);

            auto path = base_path / dir_prefix / with_extension(file_prefix, std::format(".{}", flush_iteration));

            const auto& executor = executor_pool[executor_id++ % executor_pool.size()];
            executor->submit_io_task(flush_partition_task(
                std::move(path),
                range.begin,
                range.end,
                range.count,
                range.sum_key_value_lengths,
                range.partition_id,
                flush_iteration,
                cache,
                use_odirect,
                &results_mutex,
                &results,
                &errors,
                wg));

            range_count++;
        }

        for(size_t i = range_count; i < (1 << num_partition_exponent); ++i)
            wg->decr(); // Decr for the partitions that don't exist

        wg->wait();

        if(!errors.empty())
            return hedge::error("Parallel flush failed: " + errors.front().to_string());

        return results;
    }

} // namespace hedge::db