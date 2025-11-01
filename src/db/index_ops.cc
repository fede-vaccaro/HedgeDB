#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <unistd.h>
#include <vector>

#include <error.hpp>
#include <uuid.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"
#include "async/task.h"
#include "fs/fs.hpp"
#include "index_ops.h"
#include "merge_utils.h"
#include "sorted_index.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{
    /**
     * @brief Helper struct for building a `sorted_index_footer` incrementally and validating it.
     */
    struct footer_builder
    {
        std::optional<uint64_t> upper_bound{};
        std::optional<uint64_t> indexed_keys{};
        std::optional<uint64_t> meta_index_entries{};
        std::optional<uint64_t> index_start_offset{};
        std::optional<uint64_t> index_end_offset{};
        std::optional<uint64_t> meta_index_start_offset{};
        std::optional<uint64_t> meta_index_end_offset{};
        std::optional<uint64_t> footer_start_offset{};

        /**
         * @brief Validates that all required fields are set and constructs the footer.
         * @return `expected<sorted_index_footer>` containing the footer or an error string.
         */
        hedge::expected<sorted_index_footer> build()
        {
            // Simple validation: ensure all fields have been assigned a value.
            if(!this->upper_bound.has_value())
                return hedge::error("Footer upper_bound not set");
            if(!this->indexed_keys.has_value())
                return hedge::error("Footer indexed_keys not set");
            if(!this->meta_index_entries.has_value())
                return hedge::error("Footer meta_index_entries not set");
            if(!this->index_start_offset.has_value())
                return hedge::error("Footer index_start_offset not set");
            if(!this->index_end_offset.has_value())
                return hedge::error("Footer index_end_offset not set");
            if(!this->meta_index_start_offset.has_value())
                return hedge::error("Footer meta_index_start_offset not set");
            if(!this->meta_index_end_offset.has_value())
                return hedge::error("Footer meta_index_end_offset not set");
            if(!this->footer_start_offset.has_value())
                return hedge::error("Footer footer_start_offset not set");

            return sorted_index_footer{
                .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                .upper_bound = this->upper_bound.value(),
                .indexed_keys = this->indexed_keys.value(),
                .meta_index_entries = this->meta_index_entries.value(),
                .index_start_offset = this->index_start_offset.value(),
                .index_end_offset = this->index_end_offset.value(),
                .meta_index_start_offset = this->meta_index_start_offset.value(),
                .meta_index_end_offset = this->meta_index_end_offset.value(),
                .footer_start_offset = this->footer_start_offset.value(),
            };
        }
    };

    /**
     * @brief Calculates the padding bytes needed to align data to a page boundary.
     * @details This is useful when we are writing a vector of data to disk and want to ensure
     * that it is aligned to a specific page size (4KB) for faster DIRECT I/O access.
     * @tparam T Type of the data elements.
     * @param element_count Number of elements.
     * @param page_size The alignment boundary (default: PAGE_SIZE_IN_BYTES).
     * @return Number of padding bytes needed (0 if already aligned).
     */
    template <typename T>
    size_t compute_alignment_padding(size_t element_count, size_t page_size = PAGE_SIZE_IN_BYTES)
    {
        size_t complement = page_size - ((element_count * sizeof(T)) % page_size);

        if(complement == page_size)
            return 0;

        return complement;
    }

    /** @brief A static zero-filled buffer used for writing alignment padding efficiently. */
    static std::vector<uint8_t> PADDING_BUFFER(PAGE_SIZE_IN_BYTES, 0);

    /**
     * @brief Writes a single data object to an output stream, optionally adding alignment padding.
     * @tparam T Type of the data object.
     * @param ofs Output file stream.
     * @param data The data object to write.
     * @param align If true, adds padding bytes after the object to reach the next page boundary.
     * @return Pair: {offset after data, offset after padding (if any)}.
     */
    template <typename T>
    std::pair<size_t, size_t> write_to(std::ofstream& ofs, const T& data, bool align)
    {
        ofs.write(reinterpret_cast<const char*>(&data), sizeof(T));
        size_t end_data_pos = ofs.tellp();

        if(align)
        {
            size_t padding_size = compute_alignment_padding<T>(1);
            if(padding_size > 0)
                ofs.write(reinterpret_cast<const char*>(PADDING_BUFFER.data()), padding_size);
        }
        return {end_data_pos, ofs.tellp()};
    }

    /**
     * @brief Writes a vector of data objects to an output stream, optionally adding alignment padding.
     * @tparam T Type of the data elements in the vector.
     * @param ofs Output file stream.
     * @param data The vector of data objects to write.
     * @param align If true, adds padding bytes after the vector data to reach the next page boundary.
     * @return Pair: {offset after data, offset after padding (if any)}.
     */
    template <typename T>
    std::pair<size_t, size_t> write_to(std::ofstream& ofs, const std::vector<T>& data, bool align)
    {
        if(data.empty())
        { // Handle empty vector case explicitly
            size_t current_pos = ofs.tellp();
            return {current_pos, current_pos};
        }
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
        size_t end_data_pos = ofs.tellp();

        if(align)
        {
            size_t padding_size = compute_alignment_padding<T>(data.size());
            if(padding_size > 0)
                ofs.write(reinterpret_cast<const char*>(PADDING_BUFFER.data()), padding_size);
        }

        return {end_data_pos, ofs.tellp()};
    }

    /**
     * @brief Reads a single data object from an input stream.
     * @tparam T Type of the data object.
     * @param ifs Input file stream.
     * @param data Reference to the object where data will be read into.
     */
    template <typename T>
    void read_from(std::ifstream& ifs, T& data)
    {
        ifs.read(reinterpret_cast<char*>(&data), sizeof(T));
    }

    /**
     * @brief Reads data from an input stream into a pre-allocated vector.
     * @tparam T Type of the data elements in the vector.
     * @param ifs Input file stream.
     * @param allocated_data The vector (already sized) to fill with data.
     */
    template <typename T>
    void read_from(std::ifstream& ifs, std::vector<T>& allocated_data)
    {
        ifs.read(reinterpret_cast<char*>(allocated_data.data()), allocated_data.size() * sizeof(T));
    }

    /**
     * @brief Constructs the meta-index data structure from a sorted list of index entries.
     * @details Calculates the required size and iterates through the sorted keys, adding the
     * maximum key (which is the last one, since each page is sorted) of each conceptual data page
     * to the meta-index vector. Through the meta-index, we can quickly locate (because each meta-index lookup
     * is in memory with complexity O(logN), where N is the number of pages) which page might
     * contain a given key.
     * @param sorted_keys A const reference to the vector containing all index entries, sorted by key.
     * @return The generated `std::vector<meta_index_entry>`.
     */
    std::vector<meta_index_entry> create_meta_index(const std::vector<index_entry_t>& sorted_keys)
    {
        // Calculate how many meta-index entries are needed (one per page).
        auto meta_index_size = hedge::ceil(sorted_keys.size(), INDEX_PAGE_NUM_ENTRIES);
        auto meta_index = std::vector<meta_index_entry>{};
        meta_index.reserve(meta_index_size);

        // Iterate through each conceptual page.
        for(size_t i = 0; i < meta_index_size; ++i)
        {
            auto idx = std::min(((i + 1) * INDEX_PAGE_NUM_ENTRIES) - 1, sorted_keys.size() - 1);
            meta_index.push_back({.page_max_id = sorted_keys[idx].key});
        }

        meta_index.shrink_to_fit();

        return meta_index;
    }

    std::vector<index_entry_t> index_ops::merge_memtables_in_mem(std::vector<mem_index>&& indices)
    {
        auto total_size = std::accumulate(indices.begin(), indices.end(), 0, [](size_t acc, const mem_index& idx)
                                          { return acc + idx._index.size(); });

        std::vector<index_entry_t> index_sorted;
        index_sorted.reserve(total_size);

        for(auto& idx : indices)
        {
            for(const auto& [key, value] : idx._index)
                index_sorted.push_back({key, value});

            idx._index = mem_index::index_t{};
        }

        std::sort(index_sorted.begin(), index_sorted.end());

        return index_sorted;
    }

    hedge::expected<std::vector<sorted_index>> index_ops::flush_mem_index(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent, size_t flush_iteration)
    {
        if(num_partition_exponent > 16)
            return hedge::error("Number of partitions exponent must be less than or equal to 16");

        // Step 1: Combine all memtables into one large sorted list in memory.
        auto index_sorted = merge_memtables_in_mem(std::move(indices));

        std::vector<sorted_index> resulting_indices;          // To store the created sorted_index objects.
        std::vector<index_entry_t> current_partition_entries; // Temp buffer for entries of the current partition.

        // Calculate the range of key prefixes per partition.
        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        // Determine the partition ID for the very first key.
        size_t current_partition_id = hedge::find_partition_prefix_for_key(index_sorted[0].key, partition_key_prefix_range);

        // Step 2: Iterate through the sorted list, grouping entries by partition ID.
        // TODO: Handle potential errors during file writing more gracefully (e.g., cleanup).
        for(auto it = index_sorted.begin(); it != index_sorted.end(); ++it)
        {
            current_partition_entries.push_back(*it);

            // Check if this is the last entry for the current partition.
            bool is_last_in_list = (it + 1 == index_sorted.end());

            size_t next_partition_id{};
            bool partition_changes = false;

            if(!is_last_in_list)
            {
                // Safely look ahead to the next key's partition ID.
                next_partition_id = hedge::find_partition_prefix_for_key((it + 1)->key, partition_key_prefix_range);
                partition_changes = (next_partition_id != current_partition_id);
            }

            if(is_last_in_list || partition_changes)
            {
                // End of the current partition reached. Write its entries to a file.
                auto [dir_prefix, file_prefix] = format_prefix(current_partition_id);
                auto dir_path = base_path / dir_prefix;
                std::filesystem::create_directories(dir_path);

                auto path = dir_path / with_extension(file_prefix, std::format(".{}", flush_iteration));

                OUTCOME_TRY(auto sorted_index, index_ops::save_as_sorted_index(
                                                   path,
                                                   std::exchange(current_partition_entries, std::vector<index_entry_t>{}),
                                                   current_partition_id));

                resulting_indices.push_back(std::move(sorted_index));

                current_partition_id = next_partition_id;
            }
        }

        // TODO: Implement WAL clearing logic upon successful flush.
        // Well, WAL should be implemented first...

        return resulting_indices;
    }

    hedge::expected<sorted_index> index_ops::save_as_sorted_index(const std::filesystem::path& path, std::vector<index_entry_t>&& sorted_keys, size_t upper_bound)
    {
        // Step 1: Generate the meta-index from the sorted keys.
        auto meta_index = create_meta_index(sorted_keys);

        footer_builder builder;
        {
            // Step 2: Write data sections [index, meta-index, footer] sequentially.
            std::ofstream ofs_sorted_index(path, std::ios::binary);
            if(!ofs_sorted_index.good())
                return hedge::error("Failed to open sorted index file for writing: " + path.string());

            builder.index_start_offset = 0; // Index always starts at the beginning.
            auto [end_of_index, end_of_index_padding] = write_to(ofs_sorted_index, sorted_keys, true);
            builder.index_end_offset = end_of_index;

            builder.meta_index_start_offset = end_of_index_padding;
            auto [end_of_meta_index, end_of_meta_index_padding] = write_to(ofs_sorted_index, meta_index, true);
            builder.meta_index_end_offset = end_of_meta_index;

            builder.footer_start_offset = end_of_meta_index_padding;
            builder.upper_bound = upper_bound;
            builder.indexed_keys = sorted_keys.size();
            builder.meta_index_entries = meta_index.size();
            OUTCOME_TRY(auto footer, builder.build());

            // Write the footer at the end.
            write_to(ofs_sorted_index, footer, false); // No padding after footer.
            if(!ofs_sorted_index.good())
                return hedge::error("Failed to write sorted index file: " + path.string());
        }

        // Step 3: Create the sorted_index object to represent the file.
        OUTCOME_TRY(auto fd, fs::file::from_path(path, fs::file::open_mode::read_only, false));
        OUTCOME_TRY(auto footer, builder.build());

        // Create the final object, moving the key and meta-index data into it.
        auto ss = sorted_index(std::move(fd), std::move(sorted_keys), std::move(meta_index), footer);

        return ss;
    }

    /**
     * @brief Loads a sorted_index object from a file, reading footer and meta-index.
     * @param path Path to the sorted_index file.
     * @param load_index If true, also loads the entire main index data into memory.
     * @return `expected<sorted_index>` containing the loaded object or an error.
     */
    hedge::expected<sorted_index> index_ops::load_sorted_index(const std::filesystem::path& path, bool load_index)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("Sorted index file does not exist: " + path.string());

        auto maybe_fd = fs::file::from_path(path, fs::file::open_mode::read_only, false, std::nullopt);

        if(!maybe_fd.has_value())
            return hedge::error("Failed to open sorted index file: " + maybe_fd.error().to_string());

        auto& fd = maybe_fd.value();

        std::ifstream ifs(path, std::ios::binary);
        if(!ifs.good())
            return hedge::error("Failed to open sorted index file for reading: " + path.string());

        // Read footer from the end of the file.
        ifs.seekg(-static_cast<std::streamoff>(sizeof(sorted_index_footer)), std::ios::end);
        sorted_index_footer footer{};
        read_from(ifs, footer);
        if(!ifs.good())
            return hedge::error("Failed to read sorted index footer: " + path.string());

        // Read meta-index using offset from the footer.
        std::vector<meta_index_entry> meta_index(footer.meta_index_entries);
        ifs.seekg(footer.meta_index_start_offset, std::ios::beg);
        read_from(ifs, meta_index);
        if(!ifs.good())
            return hedge::error("Failed to read sorted index meta index: " + path.string());

        auto ss = sorted_index(std::move(fd), {}, std::move(meta_index), footer);

        if(!load_index)
            return ss;

        // Finally, read index if requested
        if(auto status = ss.load_index(); !status)
            return hedge::error("Failed to load sorted index: " + status.error().to_string());

        return ss;
    }

    // Note from the author: before delving into this function, please make sure to understand the code of the merge_utils,
    // i.e. `rolling_buffer` and `entry_deduplicator` since they are heavily used here.
    //
    // NOLINTNEXTLINE(readability-function-cognitive-complexity) - Acknowledging complexity inherent in external merge. TODO: consider refactoring later.
    async::task<hedge::expected<sorted_index>> index_ops::two_way_merge_async(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor)
    {
        std::unique_lock lk_left(*left._compaction_mutex);
        std::unique_lock lk_right(*right._compaction_mutex);

        /*
        -- Step 0: Validate preconditions and init all the necessary structures --
        */
        if(config.read_ahead_size < PAGE_SIZE_IN_BYTES)
            co_return hedge::error("Read ahead size must be at least one page size");

        if(config.read_ahead_size % PAGE_SIZE_IN_BYTES != 0)
            co_return hedge::error("Read ahead size must be page aligned (page size: " + std::to_string(PAGE_SIZE_IN_BYTES) + ")");

        if(left._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION ||
           right._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION)
            co_return hedge::error("Cannot merge sorted indices with different versions");

        if(left._footer.upper_bound != right._footer.upper_bound)
            co_return hedge::error("Cannot merge sorted indices with different upper bounds");

        // Create new footer builder and start populating it
        footer_builder fb;
        fb.upper_bound = left._footer.upper_bound;
        fb.index_start_offset = 0;

        // Create file readers for both indices
        auto lhs_reader = fs::file_reader(
            left,
            {
                .start_offset = 0,
                .end_offset = left._footer.index_end_offset,
            },
            executor);

        auto rhs_reader = fs::file_reader(
            right,
            {
                .start_offset = 0,
                .end_offset = right._footer.index_end_offset,
            },
            executor);

        // Extrapolate new index path
        auto [dir, file_name] = format_prefix(left.upper_bound());
        auto new_path = config.base_path / dir / with_extension(file_name, std::format(".{}", config.new_index_id));

        auto fd_maybe = co_await fs::file::from_path_async(new_path, fs::file::open_mode::write_new, executor, false);

        if(!fd_maybe.has_value())
            co_return hedge::error("Failed to create file descriptor for merged index at " + new_path.string() + ": " + fd_maybe.error().to_string());

        auto fd = std::move(fd_maybe.value());

        // Create rolling buffers for both readers
        // Those will maintain a sliding window over the data read from disk
        auto lhs_rbuf = rolling_buffer(std::move(lhs_reader));
        auto rhs_rbuf = rolling_buffer(std::move(rhs_reader));

        auto init_lhs = co_await lhs_rbuf.next(config.read_ahead_size);

        if(!init_lhs)
            co_return hedge::error("Some error occurred while getting the first page from LHS inde: " + init_lhs.error().to_string());

        auto init_rhs = co_await rhs_rbuf.next(config.read_ahead_size);

        if(!init_rhs)
            co_return hedge::error("Some error occurred while getting the first page from RHS inde: " + init_rhs.error().to_string());

        size_t indexed_keys = 0;
        [[maybe_unused]] size_t filtered_keys = 0; // TO FIX: this was used for an assert that is currently disabled
        size_t bytes_written = 0;

        // Meta-index entries collected during the merge
        std::vector<meta_index_entry> merged_meta_index;

        // For shortening the syntax
        auto& lhs = lhs_rbuf;
        auto& rhs = rhs_rbuf;

        // Helper lambda to refresh both buffers by reading the next chunk from each index
        auto refresh_buffers = [&lhs, &rhs, &config]() -> async::task<hedge::status>
        {
            auto status = co_await lhs.next(config.read_ahead_size);

            if(!status)
                co_return hedge::error("Cannot refresh LHS view: " + status.error().to_string());

            status = co_await rhs.next(config.read_ahead_size);

            if(!status)
                co_return hedge::error("Cannot refresh RHS view: " + status.error().to_string());

            co_return hedge::ok();
        };

        // Needed to handle possible entry with duplicated keys (but different value_ptr) between LHS and RHS
        // In different sorted indices, there might be duplicated keys with different value_ptr because this
        // is how update/remove operations are implemented. Upon merge, we have to respect the Key-Uniqueness
        // constraint of the output sorted index
        //
        // Brief recall on how the entry deduplication works:
        // When pushing a new item:
        // - If its key is the same as the last pushed one, the internal buffer keeps only the one with the highest value_ptr (most recent entry).
        // - If its key is different from the last pushed one, the last pushed is rotate to the output (ready to be popped), and the new item is stored as the current one.
        entry_deduplicator dedup{};

        // Keep track of the last written key for building the meta-index
        uuids::uuid last_written_key{};

        /*
        -- Step 1: Main merge loop. Here happen the comparisons between the two buffer --
        */
        while(true)
        {
            std::vector<uint8_t> merged_keys(config.read_ahead_size * 2);
            auto merged_keys_span = view_as<index_entry_t>(merged_keys);
            auto merged_keys_it = merged_keys_span.begin();

            // Loop until one of the two views is exhausted
            while(lhs.it() != lhs.end() && rhs.it() != rhs.end())
            {
                if(*lhs.it() < *rhs.it())
                {
                    dedup.push(*lhs.it());
                    ++lhs.it();
                }
                else
                {
                    dedup.push(*rhs.it());
                    ++rhs.it();
                }

                if(dedup.ready()) // For getting ready, we need to have pushed at least two entries
                {
                    // Note that we are always lagging by one entry!
                    // After popping from `dedup`, there is still one entry sitting there waiting to get checked against the next pushed one and thus getting ready
                    auto new_item = dedup.pop();

                    if(config.discard_deleted_keys && new_item.value_ptr.is_deleted())
                    {
                        filtered_keys++;
                        continue;
                    }

                    *merged_keys_it = new_item;
                    last_written_key = new_item.key;

                    indexed_keys++;
                    if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                        merged_meta_index.emplace_back(last_written_key);

                    if(++merged_keys_it == merged_keys_span.end())
                        break; // The output buffer is full, exit loop to write to disk
                }
            }

            // Resize the merged keys buffer to the actual number of bytes written
            auto bytes_in_chunk = std::distance(merged_keys_span.begin(), merged_keys_it) * sizeof(index_entry_t);
            merged_keys.resize(bytes_in_chunk);

            // Reminder that the `entry_deduplicator` is strictly necessary when reading chunks from disk:
            // The easiest solution would be to filter the duplicates from the `merged_keys` when full,
            // just before writing it to disk; however, this might lead to a subtle bug: a entry duplicate
            // might just be in the of the two forecoming chunks
            auto res = co_await executor->submit_request(async::write_request{
                .fd = fd.get_fd(),
                .data = merged_keys.data(),
                .size = merged_keys.size(),
                .offset = bytes_written});

            bytes_written += res.bytes_written;

            if(res.error_code != 0)
                co_return hedge::error("Failed to write merged keys to file: " + std::string(strerror(res.error_code)));

            auto status = co_await refresh_buffers();
            if(!status)
                co_return hedge::error("Failed to refresh views: " + status.error().to_string());

            // One of the two data stream has been consumed
            if(lhs.eof() || rhs.eof())
                break;
        }

        /*
        -- Step 2: Handle remaining keys from the non-exhausted view, keeping in consideration the deduplicator --
           The deduplicator maintains a 1-item lag.
           We must handle this final buffered item carefully.
        */

        // Handle remaining keys from the non exhausted-view
        auto& non_empty_view = !lhs.eof() ? lhs : rhs;

        std::vector<index_entry_t> remaining_keys;           // Buffer for remaining keys to be written
        remaining_keys.reserve(non_empty_view.items_left()); // Estimate capacity

        // Push the next item (if any) to the deduplicator.
        // Remember that there is necessary an item left from the previous loop (because of the 1-item lag)
        // This item, is sitting there waiting to get ready
        if(!non_empty_view.eof() && non_empty_view.it() != non_empty_view.end())
        {
            dedup.push(*non_empty_view.it());
            ++non_empty_view.it();
        }

        std::vector<index_entry_t> last_items{};
        last_items.reserve(2); // The deduplicator might contain up to two items

        // We enter this block ONLY IF the non_empty_view had at least one item left
        if(dedup.ready())
            last_items.push_back(dedup.pop());

        // Empty the deduplicator buffer
        last_items.push_back(dedup.force_pop());

        // Process the last items from the deduplicator
        for(const auto& new_item : last_items)
        {
            if(config.discard_deleted_keys && new_item.value_ptr.is_deleted())
            {
                filtered_keys++;
            }
            else
            {
                remaining_keys.push_back(new_item);
                last_written_key = new_item.key;
                indexed_keys++;

                if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                    merged_meta_index.emplace_back(remaining_keys.back().key);
            }
        }

        /*
        -- Step 3: Read all remaining items from the non-exhausted view in chunks, writing them to disk --
        From now on, we can ignore the dedup since there are no duplicated keys within the same index
        */

        // In this loop, we just read all (in chunks) remaining items from the non_empty_view
        while(!non_empty_view.eof())
        {
            // Read all items from the current buffer
            while(non_empty_view.it() != non_empty_view.end())
            {
                if(config.discard_deleted_keys && non_empty_view.it()->value_ptr.is_deleted())
                {
                    filtered_keys++;
                    continue;
                }

                ++indexed_keys;

                if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                    merged_meta_index.emplace_back(non_empty_view.it()->key);

                last_written_key = non_empty_view.it()->key;
                remaining_keys.push_back(*non_empty_view.it());
                non_empty_view.it()++;
            }

            // When the buffer is consumed, write remaining keys to disk
            if(!remaining_keys.empty())
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get_fd(),
                    .data = reinterpret_cast<uint8_t*>(remaining_keys.data()),
                    .size = remaining_keys.size() * sizeof(index_entry_t),
                    .offset = bytes_written});

                remaining_keys.clear();

                if(res.error_code != 0)
                    co_return hedge::error("Failed to write remaining keys to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            // Refresh the view to get the next chunk
            auto refresh_status = co_await non_empty_view.next(config.read_ahead_size);

            if(!refresh_status)
                co_return hedge::error("Failed to refresh view: " + refresh_status.error().to_string());
        }

        // The condition is needed to write the last meta-index entry if the total number of indexed keys is not a multiple of the page size
        // We avoid entering in this condition if this key was already recorded (as happens when the last written key aligns with the page size)
        if(indexed_keys % INDEX_PAGE_NUM_ENTRIES != 0)
            merged_meta_index.emplace_back(last_written_key);

        // THIS CHECK WAS COMMENTED OUT DUE TO INCONSISTENT BEHAVIOR DURING DELETION MERGES
        // assert(indexed_keys == (left._footer.indexed_keys + right._footer.indexed_keys - filtered_keys) && "Item count does not match footer indexed keys");
        fb.indexed_keys = indexed_keys;
        fb.index_end_offset = bytes_written;

        /*
        -- Step 4: Write meta-index and footer --
        */
        // Write index padding if any
        size_t padding_size = compute_alignment_padding<index_entry_t>(indexed_keys);
        if(padding_size > 0)
        {
            auto padding = std::vector<uint8_t>(padding_size, 0);
            auto res = co_await executor->submit_request(async::write_request{
                .fd = fd.get_fd(),
                .data = padding.data(),
                .size = padding.size(),
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write padding to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        fb.meta_index_start_offset = bytes_written;
        fb.meta_index_entries = merged_meta_index.size();

        // Write meta-index
        {
            auto res = co_await executor->submit_request(async::write_request{
                .fd = fd.get_fd(),
                .data = reinterpret_cast<uint8_t*>(merged_meta_index.data()),
                .size = merged_meta_index.size() * sizeof(meta_index_entry),
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write meta index to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        fb.meta_index_end_offset = bytes_written;

        // Write meta index padding if any
        size_t meta_index_padding_size = compute_alignment_padding<meta_index_entry>(merged_meta_index.size());
        if(meta_index_padding_size > 0)
        {
            auto padding = std::vector<uint8_t>(meta_index_padding_size, 0);
            auto res = co_await executor->submit_request(async::write_request{
                .fd = fd.get_fd(),
                .data = padding.data(),
                .size = meta_index_padding_size,
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write meta index padding to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        fb.footer_start_offset = bytes_written;

        auto maybe_footer = fb.build();
        if(!maybe_footer.has_value())
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        auto& footer = maybe_footer.value();

        // Write footer
        {
            auto res = co_await executor->submit_request(async::write_request{
                .fd = fd.get_fd(),
                .data = reinterpret_cast<uint8_t*>(&footer),
                .size = sizeof(footer),
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write footer to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        /*
        -- Step 5: Create sorted_index object for the merged file and return it --
        */
        auto read_fd = fs::file::from_path(fd.path(), fs::file::open_mode::read_only, false, bytes_written);
        if(!read_fd.has_value())
            co_return hedge::error("Failed to open merged index file for reading: " + read_fd.error().to_string());

        sorted_index result{
            std::move(read_fd.value()),
            {},
            std::move(merged_meta_index),
            footer};

        co_return result;
    }

    hedge::expected<sorted_index> index_ops::two_way_merge(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor)
    {
        if(!executor)
            return hedge::error("Executor context is null");

        auto result = executor->sync_submit(two_way_merge_async(config, left, right, executor));

        if(!result.has_value())
            return hedge::error("Failed to merge sorted indices: " + result.error().to_string());

        return std::move(result.value());
    }

} // namespace hedge::db