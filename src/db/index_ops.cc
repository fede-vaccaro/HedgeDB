#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
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
#include "cache.h"
#include "db/mem_index.h"
#include "fs/file_reader.h"
#include "fs/fs.hpp"
#include "index_ops.h"
#include "merge_utils.h"
#include "perf_counter.h"
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
    constexpr size_t compute_alignment_padding(size_t element_count, size_t page_size = PAGE_SIZE_IN_BYTES)
    {
        size_t complement = page_size - ((element_count * sizeof(T)) % page_size);

        if(complement == page_size)
            return 0;

        return complement;
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

    ///< Threshold indicating whether the super index should be enabled or not
    constexpr size_t SUPER_INDEX_ENABLED_THRESHOLD = 16;

    /**
     * @brief Constructs the meta-index data structure from a sorted list of index entries.
     * @details Calculates the required size and iterates through the sorted keys, adding the
     * maximum key (which is the last one, since each page is sorted) of each conceptual data page
     * to the meta-index vector. Through the meta-index, we can quickly locate (because each meta-index lookup
     * is in memory with complexity O(logN), where N is the number of pages) which page might
     * contain a given key.
     * This same function can be used for building the super-index (or, meta-meta-index)
     * @param sorted_keys A const reference to the vector containing all index entries, sorted by key.
     * @tparam T Item of ordered keys or key/value Ordered std::vector<T>.
     * @return The generated `std::vector<meta_index_entry>`.
     */
    template <typename T, size_t PAGE_SIZE>
    page_aligned_buffer<meta_index_entry> create_meta_index(const page_aligned_buffer<T>& sorted_keys)
    {
        // Calculate how many meta-index entries are needed (one per page).
        constexpr size_t ENTRIES_PER_PAGE = PAGE_SIZE / sizeof(T);

        auto meta_index_size = hedge::ceil(sorted_keys.size(), ENTRIES_PER_PAGE);
        auto meta_index = page_aligned_buffer<meta_index_entry>(meta_index_size);

        // Iterate through each conceptual page.
        for(size_t i = 0; i < meta_index_size; ++i)
        {
            auto idx = std::min(((i + 1) * ENTRIES_PER_PAGE) - 1, sorted_keys.size() - 1);
            meta_index[i] = {.key = sorted_keys[idx].key};
        }

        return meta_index;
    }

    page_aligned_buffer<index_entry_t> index_ops::sort_memtable(mem_index* index)
    {
        page_aligned_buffer<index_entry_t> buf(index->size());

        auto* buf_ptr = buf.data();

        size_t idx = 0;
        for(auto [k, v] : index->_index)
            buf_ptr[idx++] = {.key = k, .value_ptr = v};

        std::sort(
            buf.begin(),
            buf.end(),
            [](const index_entry_t& lhs, const index_entry_t& rhs)
            {
                if(lhs.key < rhs.key)
                    return true;

                if(rhs.key < lhs.key)
                    return false;

                return lhs.value_ptr < rhs.value_ptr;
            });

        auto* last_it = std::unique(buf.begin(), buf.end());

        // move the size cursor back
        buf.resize(std::distance(buf.begin(), last_it));

        return buf;
    }

    hedge::expected<std::vector<sorted_index>> index_ops::flush_mem_index(const std::filesystem::path& base_path,
                                                                          mem_index* index,
                                                                          size_t num_partition_exponent,
                                                                          size_t flush_iteration,
                                                                          bool use_odirect)
    {
        if(num_partition_exponent > 16)
            return hedge::error("Number of partitions exponent must be less than or equal to 16");

        // Step 1: Combine all memtables into one large sorted list in memory.
        auto index_sorted = sort_memtable(index);

        std::vector<sorted_index> resulting_indices; // To store the created sorted_index objects.

        constexpr double SLACK = 0.5; // Slack factor to avoid the need of reallocation.
        size_t partition_estimated_size;

        if(num_partition_exponent == 0)
            partition_estimated_size = index_sorted.size();
        else
            partition_estimated_size = ((double)index_sorted.size() / (1 << num_partition_exponent) + 1.0) * (1.0 + SLACK);

        partition_estimated_size = std::max(partition_estimated_size, PAGE_SIZE_IN_BYTES / sizeof(index_entry_t));

        page_aligned_buffer<index_entry_t> current_partition_entries(partition_estimated_size); // Temp buffer for entries of the current partition.
        size_t curr_partition_count{0};

        // Calculate the range of key prefixes per partition.
        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        // Determine the partition ID for the very first key.
        size_t current_partition_id = hedge::find_partition_prefix_for_key(index_sorted.begin()->key, partition_key_prefix_range);

        // Step 2: Iterate through the sorted list, grouping entries by partition ID.
        // TODO: Handle potential errors during file writing more gracefully (e.g., cleanup).
        for(auto* it = index_sorted.begin(); it != index_sorted.end(); ++it)
        {
            current_partition_entries[curr_partition_count++] = *it;

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

                current_partition_entries.resize(curr_partition_count);

                OUTCOME_TRY(auto sorted_index, index_ops::save_as_sorted_index(
                                                   path,
                                                   std::exchange(current_partition_entries, page_aligned_buffer<index_entry_t>(partition_estimated_size)),
                                                   current_partition_id,
                                                   use_odirect));

                curr_partition_count = 0;

                resulting_indices.push_back(std::move(sorted_index));

                current_partition_id = next_partition_id;
            }
        }

        // TODO: Implement WAL clearing logic upon successful flush.
        // Well, WAL should be implemented first...

        return resulting_indices;
    }

    hedge::expected<size_t> pwrite_iovec(int fd, const iovec& iovec, size_t offset)
    {
        int res = pwrite(fd,
                         reinterpret_cast<uint8_t*>(iovec.iov_base),
                         iovec.iov_len,
                         offset);

        if(res < 0)
            return hedge::error(std::format("Failed to write iovec to fd {} at offset {}: {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            strerror(errno)));

        if(static_cast<size_t>(res) != iovec.iov_len)
            return hedge::error(std::format("Partial write occurred when writing iovec to fd {} at offset {}: wrote {} bytes out of {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            res,
                                            iovec.iov_len));

        return res;
    }

    hedge::expected<sorted_index>
    index_ops::save_as_sorted_index(const std::filesystem::path& path, page_aligned_buffer<index_entry_t>&& sorted_keys, size_t upper_bound, bool use_odirect)
    {
        // Step 1: Generate the meta-index from the sorted keys.
        auto meta_index = create_meta_index<index_entry_t, PAGE_SIZE_IN_BYTES>(sorted_keys);

        footer_builder builder;

        // Step 2: Write data sections [index, meta-index, footer] sequentially.
        size_t index_size_bytes = sizeof(index_entry_t) * sorted_keys.size();
        size_t index_padding_bytes = compute_alignment_padding<index_entry_t>(sorted_keys.size());
        size_t meta_index_size_bytes = sizeof(meta_index_entry) * meta_index.size();
        size_t meta_index_padding_bytes = compute_alignment_padding<meta_index_entry>(meta_index.size());
        size_t footer_alignment_padding_bytes = compute_alignment_padding<sorted_index_footer>(1);

        builder.upper_bound = upper_bound;
        builder.indexed_keys = sorted_keys.size();
        builder.meta_index_entries = meta_index.size();
        builder.index_start_offset = 0;
        builder.index_end_offset = index_size_bytes;
        builder.meta_index_start_offset = index_size_bytes + index_padding_bytes;
        builder.meta_index_end_offset = index_size_bytes + index_padding_bytes + meta_index_size_bytes;
        builder.footer_start_offset = index_size_bytes + index_padding_bytes + meta_index_size_bytes + meta_index_padding_bytes;

        OUTCOME_TRY(auto footer, builder.build());

        size_t estimated_size =
            index_size_bytes +
            index_padding_bytes +
            meta_index_size_bytes +
            meta_index_padding_bytes +
            sizeof(sorted_index_footer) +
            footer_alignment_padding_bytes;

        auto maybe_sorted_index_file = fs::file::from_path(path, fs::file::open_mode::read_write_new, use_odirect, estimated_size);
        if(!maybe_sorted_index_file)
            return hedge::error("Failed to create sorted index file: " + maybe_sorted_index_file.error().to_string());

        auto fd = maybe_sorted_index_file.value().fd();

        page_aligned_buffer<sorted_index_footer> sorted_index_footer_buf(1);
        sorted_index_footer_buf[0] = footer;

        std::array<iovec, 3> iovecs{
            iovec{
                .iov_base = reinterpret_cast<uint8_t*>(sorted_keys.data()),
                .iov_len = index_size_bytes + index_padding_bytes},
            iovec{
                .iov_base = reinterpret_cast<uint8_t*>(meta_index.data()),
                .iov_len = meta_index_size_bytes + meta_index_padding_bytes},
            iovec{
                .iov_base = reinterpret_cast<uint8_t*>(sorted_index_footer_buf.data()),
                .iov_len = sizeof(sorted_index_footer) + footer_alignment_padding_bytes},
        };

        auto status = pwrite_iovec(fd, iovecs[0], 0);
        if(!status)
            return hedge::error("Failed to write index data to sorted index file: " + status.error().to_string());

        status = pwrite_iovec(fd, iovecs[1], builder.meta_index_start_offset.value());
        if(!status)
            return hedge::error("Failed to write meta-index data to sorted index file: " + status.error().to_string());

        status = pwrite_iovec(fd, iovecs[2], builder.footer_start_offset.value());
        if(!status)
            return hedge::error("Failed to write footer data to sorted index file: " + status.error().to_string());

        // Step 3: Create the sorted_index object to represent the file.
        if(!use_odirect)
            posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);

        constexpr size_t REF_PAGE_SIZE = 4096;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(meta_index_entry);

        // Super index enabled if there would be 512 * 256 = 131072 meta index entries (2MB)
        // In this case the super index would be 512 * 16 bytes = 8KB
        std::optional<page_aligned_buffer<meta_index_entry>> super_index;

        if(meta_index.size() > KEYS_PER_META_INDEX_PAGE * SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = create_meta_index<meta_index_entry, REF_PAGE_SIZE>(meta_index);

        // Create the final object, moving the key and meta-index data into it.
        auto ss = sorted_index(std::move(maybe_sorted_index_file.value()), std::move(sorted_keys), std::move(meta_index), footer);

        ss._super_index = std::move(super_index);

        return ss;
    }

    /**
     * @brief Loads a sorted_index object from a file, reading footer and meta-index.
     * @param path Path to the sorted_index file.
     * @param load_index If true, also loads the entire main index data into memory.
     * @return `expected<sorted_index>` containing the loaded object or an error.
     */
    hedge::expected<sorted_index> index_ops::load_sorted_index(const std::filesystem::path& path, bool use_direct, bool load_index)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("Sorted index file does not exist: " + path.string());

        auto maybe_fd = fs::file::from_path(path, fs::file::open_mode::read_only, use_direct);

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
        page_aligned_buffer<meta_index_entry> meta_index(footer.meta_index_entries);
        ifs.seekg(footer.meta_index_start_offset, std::ios::beg);
        read_from(ifs, meta_index);
        if(!ifs.good())
            return hedge::error("Failed to read sorted index meta index: " + path.string());

        auto ss = sorted_index(std::move(fd), {}, std::move(meta_index), footer);

        if(!load_index)
            return ss;

        // Finally, read index if requested
        if(auto status = ss.load_index_from_fs(); !status)
            return hedge::error("Failed to load sorted index: " + status.error().to_string());

        return ss;
    }

    // Note from the author: before delving into this function, please make sure to understand the code of the merge_utils,
    // i.e. `rolling_buffer` and `entry_deduplicator` since they are heavily used here.
    //
    // NOLINTNEXTLINE(readability-function-cognitive-complexity) - Acknowledging complexity inherent in external merge. TODO: consider refactoring later.
    async::task<hedge::expected<sorted_index>> index_ops::two_way_merge_async(
        const merge_config& config,
        const sorted_index& left,
        const sorted_index& right,
        const std::shared_ptr<async::executor_context>& executor,
        const std::shared_ptr<db::shared_page_cache>& cache)
    {
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
                .read_ahead_size = config.read_ahead_size,
            });

        auto rhs_reader = fs::file_reader(
            right,
            {
                .start_offset = 0,
                .end_offset = right._footer.index_end_offset,
                .read_ahead_size = config.read_ahead_size,
            });

        // Extrapolate new index path
        auto [dir, file_name] = format_prefix(left.upper_bound());
        auto new_path = config.base_path / dir / with_extension(file_name, std::format(".{}", config.new_index_id));

        size_t max_new_index_keys = left.size() + right.size();
        size_t max_new_meta_index_entries = hedge::ceil(max_new_index_keys, INDEX_PAGE_NUM_ENTRIES);
        size_t estimated_size =
            (max_new_index_keys * sizeof(index_entry_t)) +
            compute_alignment_padding<index_entry_t>(max_new_index_keys) +
            (max_new_meta_index_entries * sizeof(meta_index_entry)) +
            compute_alignment_padding<meta_index_entry>(max_new_meta_index_entries) +
            sizeof(sorted_index_footer) +
            compute_alignment_padding<sorted_index_footer>(1);

        auto fd_maybe = co_await fs::file::from_path_async(new_path,
                                                           fs::file::open_mode::write_new,
                                                           async::this_thread_executor(),
                                                           false,
                                                           estimated_size);

        if(!fd_maybe.has_value())
            co_return hedge::error("Failed to create file descriptor for merged index at " + new_path.string() + ": " + fd_maybe.error().to_string());

        auto output_fd = std::move(fd_maybe.value());

        auto read_fd = co_await fs::file::from_path_async(
            output_fd.path(),
            fs::file::open_mode::read_only,
            async::this_thread_executor(),
            config.create_new_with_odirect,
            std::nullopt);

        if(!read_fd.has_value())
            co_return hedge::error("Failed to open merged index file for reading: " + read_fd.error().to_string());

        if(read_fd.value().has_direct_access())
            posix_fadvise(read_fd.value().fd(), 0, 0, POSIX_FADV_RANDOM);

        // Create rolling buffers for both readers
        // Those will maintain a sliding window over the data read from disk
        auto lhs_rbuf = rolling_buffer();
        auto rhs_rbuf = rolling_buffer();

        using read_response_with_buffer_span_t = std::pair<async::read_response, std::span<uint8_t>>;
        using unpacked_t = std::variant<db::page_cache::page_guard, read_response_with_buffer_span_t>;

        auto span_from_unpacked = hedge::overloaded{
            [](const db::page_cache::page_guard& page_guard)
            {
                return std::span<uint8_t>(page_guard.data + (page_guard.idx * PAGE_SIZE_IN_BYTES), page_guard.data + ((page_guard.idx + 1) * PAGE_SIZE_IN_BYTES));
            },
            [](const read_response_with_buffer_span_t& read_response)
            {
                return read_response.second;
            }};

        auto unpack_mailbox = [](fs::file_reader::awaitable_from_cache_or_fs_t& v) -> async::task<expected<unpacked_t>>
        {
            co_return co_await std::visit(
                hedge::overloaded{
                    [](db::page_cache::awaitable_page_guard& awaitable) -> async::task<expected<unpacked_t>>
                    {
                        co_return unpacked_t{db::page_cache::page_guard(co_await awaitable)};
                    },
                    [](fs::file_reader::awaitable_read_request_t& awaitable) -> async::task<expected<unpacked_t>>
                    {
                        auto response = co_await awaitable.first;

                        if(response.error_code != 0)
                            co_return hedge::error("Read request failed with error code: " + std::string(strerror(-response.error_code)));

                        co_return unpacked_t{read_response_with_buffer_span_t{response, awaitable.second}};
                    },
                },
                v);
        };

        size_t indexed_keys = 0;
        [[maybe_unused]] size_t filtered_keys = 0; // TO FIX: this was used for an assert that is currently disabled
        size_t bytes_written = 0;

        // Meta-index entries collected during the merge
        std::vector<meta_index_entry> merged_meta_index;

        std::vector<fs::file_reader::awaitable_from_cache_or_fs_t> lhs_pending_reads;
        std::vector<fs::file_reader::awaitable_from_cache_or_fs_t> rhs_pending_reads;

        lhs_pending_reads = lhs_reader.next(cache);
        rhs_pending_reads = rhs_reader.next(cache);

        // Helper lambda to refresh both buffers by reading the next chunk from each index
        auto refresh_buffers = [&]() -> async::task<hedge::status>
        {
            for(auto& read_mailbox : lhs_pending_reads)
            {
                auto maybe_read = co_await unpack_mailbox(read_mailbox);
                if(!maybe_read)
                    co_return hedge::error("Failed to read from LHS during buffer refresh: " + maybe_read.error().to_string());

                // fetch data from LHS
                auto span = std::visit(span_from_unpacked, maybe_read.value());
                lhs_rbuf.consume_and_push(span);
            }
            lhs_pending_reads.clear();

            for(auto& read_mailbox : rhs_pending_reads)
            {
                auto maybe_read = co_await unpack_mailbox(read_mailbox);
                if(!maybe_read)
                    co_return hedge::error("Failed to read from RHS during buffer refresh: " + maybe_read.error().to_string());

                // fetch data from RHS
                auto span = std::visit(span_from_unpacked, maybe_read.value());
                rhs_rbuf.consume_and_push(span);
            }
            rhs_pending_reads.clear();

            // prepare new requests
            if(!lhs_reader.is_eof())
                lhs_pending_reads = lhs_reader.next(cache);
            else
                lhs_rbuf.consume_and_push({});

            // prepare new requests
            if(!rhs_reader.is_eof())
                rhs_pending_reads = rhs_reader.next(cache);
            else
                rhs_rbuf.consume_and_push({});

            co_return hedge::ok();
        };

        auto initial_refresh_status = co_await refresh_buffers();
        if(!initial_refresh_status)
            co_return hedge::error("Failed to perform initial buffer refresh: " + initial_refresh_status.error().to_string());

        // Needed to handle possible entry with duplicated keys (but different value_ptr) between LHS and RHS
        // In different sorted indices, there might be duplicated keys with different value_ptr because this
        // is how update/remove operations are implemented. Upon merge, we have to respect the Key-Uniqueness
        // constraint of the output sorted index
        //
        // Brief recall on how the entry deduplication works:
        // When pushing a new item:
        // - If its key is the same as the last pushed one, the internal buffer keeps only the one with the highest value_ptr (most recent entry).
        // - If its key is different from the last pushed one, the last pushed is rotate to the output (ready to be popped), and the new item is stored as the current one.
        merge_iterator dedup{};

        // Keep track of the last written key for building the meta-index
        uuids::uuid last_written_key{};

        [[maybe_unused]] uint64_t compute_duration{0};
        [[maybe_unused]] size_t merge_iterations{0};

        page_aligned_buffer<index_entry_t> write_buffer((config.read_ahead_size * 2) / sizeof(index_entry_t));
        auto* write_it = write_buffer.begin();

        auto buffer_full = [&write_buffer, &write_it]()
        {
            return write_it == write_buffer.end();
        };

        auto buffer_empty = [&write_buffer, &write_it]()
        {
            return write_it == write_buffer.begin();
        };

        auto reset_write_buffer = [&write_buffer, &write_it]()
        {
            write_it = write_buffer.begin();
            write_buffer.zero();
        };

        auto flush_buffer = [&write_buffer, &executor, &output_fd, &write_it](size_t offset) -> async::task<expected<async::write_response>>
        {
            auto awaitable_write_response = executor->submit_request(async::write_request{
                .fd = output_fd.fd(),
                .data = reinterpret_cast<uint8_t*>(write_buffer.data()),
                .size = std::distance(write_buffer.begin(), write_it) * sizeof(index_entry_t),
                .offset = offset});

            auto res = co_await awaitable_write_response;

            if(res.error_code != 0)
                co_return hedge::error("Failed to write merged keys to file: " + std::string(strerror(-res.error_code)));

            co_return res;
        };

        /*
        -- Step 1: Main merge loop. Here happen the comparisons between the two buffer --
        */
        while(true)
        {
            auto t0 = std::chrono::high_resolution_clock::now();
            prof::DoNotOptimize(t0);

            [[maybe_unused]] size_t iteration_count{0};

            // Loop until one of the two views is exhausted
            while(lhs_rbuf.it() != lhs_rbuf.end() && rhs_rbuf.it() != rhs_rbuf.end())
            {
                if(*lhs_rbuf.it() < *rhs_rbuf.it())
                {
                    dedup.push(*lhs_rbuf.it());
                    ++lhs_rbuf.it();
                }
                else
                {
                    dedup.push(*rhs_rbuf.it());
                    ++rhs_rbuf.it();
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

                    *write_it = new_item;
                    ++write_it;

                    last_written_key = new_item.key;

                    indexed_keys++;
                    if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                        merged_meta_index.emplace_back(last_written_key);

                    if(buffer_full())
                        break; // The output buffer is full, exit loop to write to disk
                }
            }
            // Reminder that the `entry_deduplicator` is strictly necessary when reading chunks from disk:
            // The easiest solution would be to filter the duplicates from the `merged_keys` when full,
            // just before writing it to disk; however, this might lead to a subtle bug: a entry duplicate
            // might just be in the of the two forecoming chunks

            auto t1 = std::chrono::high_resolution_clock::now();
            prof::DoNotOptimize(t1);
            compute_duration += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
            merge_iterations++;

            if(buffer_full())
            {
                auto write_res = co_await flush_buffer(bytes_written);
                if(!write_res)
                    co_return write_res.error();

                if(cache != nullptr)
                {
                    // Copy new pages to cache
                    size_t num_written_pages = write_res.value().bytes_written / PAGE_SIZE_IN_BYTES;

                    assert(bytes_written % PAGE_SIZE_IN_BYTES == 0);
                    assert(num_written_pages % PAGE_SIZE_IN_BYTES == 0);

                    auto page_guards = cache->get_write_slots_range(read_fd.value().fd(), bytes_written / PAGE_SIZE_IN_BYTES, num_written_pages);

                    size_t cur_page = 0;
                    for(auto& page : page_guards)
                    {
                        auto* dst = page.data + (page.idx * PAGE_SIZE_IN_BYTES);
                        auto* src_begin = reinterpret_cast<uint8_t*>(write_buffer.begin()) + (cur_page * PAGE_SIZE_IN_BYTES);
                        auto* src_end = reinterpret_cast<uint8_t*>(write_buffer.begin()) + ((cur_page + 1) * PAGE_SIZE_IN_BYTES);

                        std::copy(src_begin, src_end, dst);

                        ++cur_page;
                    }
                }

                bytes_written += write_res.value().bytes_written;
                reset_write_buffer();
            }

            auto status = co_await refresh_buffers();
            if(!status)
                co_return hedge::error("Failed to refresh views: " + status.error().to_string());

            // One of the two data stream has been exhausted
            if(lhs_rbuf.empty() || rhs_rbuf.empty())
                break;

            iteration_count++;
        }

        /*
        -- Step 2: Handle remaining keys from the non-exhausted view, keeping in consideration the deduplicator --
           The deduplicator maintains a 1-item lag.
           We must handle this final buffered item carefully.
        */

        // Handle remaining keys from the non exhausted-view
        auto& non_empty_view = !lhs_rbuf.empty() ? lhs_rbuf : rhs_rbuf;

        // Push the next item (if any) to the deduplicator.
        // Remember that there is necessary an item left from the previous loop (because of the 1-item lag)
        // This item, is sitting there waiting to get ready
        if(!non_empty_view.empty() && non_empty_view.it() != non_empty_view.end())
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
                *write_it = new_item;
                ++write_it;

                if(buffer_full())
                {
                    auto write_res = co_await flush_buffer(bytes_written);
                    if(!write_res)
                        co_return write_res.error();

                    bytes_written += write_res.value().bytes_written;
                    reset_write_buffer();
                }

                last_written_key = new_item.key;
                indexed_keys++;

                if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                    merged_meta_index.emplace_back(new_item.key);
            }
        }

        /*
        -- Step 3: Read all remaining items from the non-exhausted view in chunks, writing them to disk --
        From now on, we can ignore the dedup since there are no duplicated keys within the same index
        */

        // In this loop, we just read all (in chunks) remaining items from the non_empty_view

        while(!non_empty_view.empty())
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

                *write_it = *non_empty_view.it();
                ++write_it;

                if(buffer_full())
                    break;

                non_empty_view.it()++;
            }

            // When the buffer is consumed, write remaining keys to disk
            if(!buffer_empty() && buffer_full())
            {
                auto write_res = co_await flush_buffer(bytes_written);
                if(!write_res)
                    co_return write_res.error();

                bytes_written += write_res.value().bytes_written;
                reset_write_buffer();
            }

            // Refresh the view to get the next chunk
            auto refresh_status = co_await refresh_buffers();

            if(!refresh_status)
                co_return hedge::error("Failed to refresh view: " + refresh_status.error().to_string());
        }

        // The condition is needed to write the last meta-index entry if the total number of indexed keys is not a multiple of the page size
        // We avoid entering in this condition if this key was already recorded (as happens when the last written key aligns with the page size)
        if(indexed_keys % INDEX_PAGE_NUM_ENTRIES != 0)
            merged_meta_index.emplace_back(last_written_key);

        // Final buffer flush
        // align write_it to page size
        size_t written_keys = std::distance(write_buffer.begin(), write_it);
        size_t remaining_keys_to_fill_page = INDEX_PAGE_NUM_ENTRIES - (written_keys % INDEX_PAGE_NUM_ENTRIES);
        if(remaining_keys_to_fill_page != INDEX_PAGE_NUM_ENTRIES)
            write_it += remaining_keys_to_fill_page;

        auto write_res = co_await flush_buffer(bytes_written);
        if(!write_res)
            co_return write_res.error();

        bytes_written += write_res.value().bytes_written;

        // THIS CHECK WAS COMMENTED OUT DUE TO INCONSISTENT BEHAVIOR DURING DELETION MERGES
        // assert(indexed_keys == (left._footer.indexed_keys + right._footer.indexed_keys - filtered_keys) && "Item count does not match footer indexed keys");
        fb.indexed_keys = indexed_keys;
        fb.index_end_offset = bytes_written - (remaining_keys_to_fill_page * sizeof(index_entry_t));

        /*
        -- Step 4: Write meta-index and footer --
        */
        // Write meta-index
        fb.meta_index_start_offset = bytes_written;
        fb.meta_index_entries = merged_meta_index.size();

        {
            auto res = co_await executor->submit_request(async::write_request{
                .fd = output_fd.fd(),
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
                .fd = output_fd.fd(),
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

        page_aligned_buffer<sorted_index_footer> page_aligned_footer(1);
        page_aligned_footer[0] = footer;

        // Write footer
        {
            auto res = co_await executor->submit_request(async::write_request{
                .fd = output_fd.fd(),
                .data = reinterpret_cast<uint8_t*>(page_aligned_footer.data()),
                .size = sizeof(sorted_index_footer) + compute_alignment_padding<sorted_index_footer>(1),
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write footer to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        // Sync to disk
        // auto fsync_status = co_await executor->submit_request(async::fsync_request{
        // .fd = output_fd.fd()});

        // if(fsync_status.error_code != 0)
        // co_return hedge::error("Failed to fsync merged index file: " + std::string(strerror(-fsync_status.error_code)));

        if(config.create_new_with_odirect)
        {
            auto fsync_res = co_await executor->submit_request(async::fsync_request{
                .fd = output_fd.fd()});

            if(fsync_res.error_code != 0)
                co_return hedge::error("Failed to fsync merged index file: " + std::string(strerror(-fsync_res.error_code)));
        }

        constexpr size_t REF_PAGE_SIZE = 4096;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(meta_index_entry);

        // Super index enabled if there would be 512 * 256 = 131072 meta index entries (2MB)
        // In this case the super index would be 512 * 16 bytes = 8KB
        std::optional<page_aligned_buffer<meta_index_entry>> super_index;

        auto page_aligned_meta_index = page_aligned_buffer<meta_index_entry>(merged_meta_index.size());

        std::copy(merged_meta_index.begin(), merged_meta_index.end(), page_aligned_meta_index.begin());

        if(merged_meta_index.size() > KEYS_PER_META_INDEX_PAGE * SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = create_meta_index<meta_index_entry, REF_PAGE_SIZE>(page_aligned_meta_index);

        merged_meta_index.shrink_to_fit();

        sorted_index result{
            std::move(read_fd.value()),
            {},
            std::move(page_aligned_meta_index),
            footer};

        result._super_index = std::move(super_index);

        co_return result;
    }

    hedge::expected<sorted_index> index_ops::two_way_merge(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor)
    {
        if(!executor)
            return hedge::error("Executor context is null");

        auto result = executor->sync_submit(two_way_merge_async(config, left, right, executor, nullptr));

        if(!result.has_value())
            return hedge::error("Failed to merge sorted indices: " + result.error().to_string());

        return std::move(result.value());
    }

} // namespace hedge::db