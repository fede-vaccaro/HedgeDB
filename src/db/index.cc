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
#include "common.h"
#include "fs/fs.hpp"
#include "index.h"
#include "merge_utils.h"

namespace hedge::db
{

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

        hedge::expected<sorted_index_footer> build()
        {
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

    std::vector<meta_index_entry> create_meta_index(const std::vector<index_key_t>& sorted_keys)
    {
        auto meta_index_size = (sorted_keys.size() + INDEX_PAGE_NUM_ENTRIES - 1) / INDEX_PAGE_NUM_ENTRIES;

        auto meta_index = std::vector<meta_index_entry>{};

        meta_index.reserve(meta_index_size);

        for(size_t i = 0; i < meta_index_size; ++i)
        {
            auto idx = std::min((i * INDEX_PAGE_NUM_ENTRIES) + INDEX_PAGE_NUM_ENTRIES - 1, sorted_keys.size() - 1);
            meta_index.push_back({.page_max_id = sorted_keys[idx].key});
        }

        meta_index.shrink_to_fit();

        return meta_index;
    }

    std::filesystem::path get_next_index_file_path(const std::filesystem::path& path)
    {
        auto directory = path.parent_path();

        if(!std::filesystem::exists(directory))
            std::filesystem::create_directories(directory);

        int count = 0;
        std::filesystem::path new_path = with_extension(path, std::format(".{}", count));

        while(std::filesystem::exists(new_path))
        {
            ++count;
            new_path = with_extension(path, std::format(".{}", count));
        }

        return new_path;
    }

    std::vector<index_key_t> index_ops::merge_memtables_in_mem(std::vector<mem_index>&& indices)
    {
        auto total_size = std::accumulate(indices.begin(), indices.end(), 0, [](size_t acc, const mem_index& idx)
                                          { return acc + idx._index.size(); });

        std::vector<index_key_t> index_sorted;
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

        size_t num_space_partitions = 1 << num_partition_exponent; // 2^num_partition_exponent

        auto partition_size = (1 << 16) / num_space_partitions;

        auto index_sorted = merge_memtables_in_mem(std::move(indices));

        std::vector<sorted_index> sorted_indices{};
        std::vector<index_key_t> current_index{};

        size_t current_partition = hedge::find_partition_prefix_for_key(index_sorted[0].key, partition_size);

        // todo: upon error, the db might end in undefined state

        for(auto it = index_sorted.begin(); it != index_sorted.end(); ++it)
        {
            const auto& key = *it;

            current_index.push_back(key);

            size_t next_partition{};
            bool last_entry_for_partition =
                (it + 1 == index_sorted.end()) || (next_partition = hedge::find_partition_prefix_for_key((it + 1)->key, partition_size)) != current_partition;

            if(last_entry_for_partition)
            {
                auto [dir_prefix, file_prefix] = format_prefix(current_partition);

                auto dir_path = base_path / dir_prefix;
                if(!std::filesystem::exists(dir_path))
                    std::filesystem::create_directories(dir_path);

                auto path = dir_path / with_extension(file_prefix, std::format(".{}", flush_iteration));

                // std::cout << "Saving partition with prefix: " << file_prefix << " to path: " << path.string() << std::endl;

                current_index.shrink_to_fit();

                OUTCOME_TRY(auto sorted_index, index_ops::save_as_sorted_index(
                                                   path,
                                                   std::exchange(current_index, std::vector<index_key_t>{}),
                                                   current_partition,
                                                   false));

                sorted_indices.push_back(std::move(sorted_index));

                current_partition = next_partition;
            }
        }

        // todo: if OK, clear mem_index-es WALs
        // todo because WAL is not implemented yet

        return sorted_indices;
    }

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key) const
    {
        auto comparator = [](const meta_index_entry& a, const key_t& b)
        {
            return a.page_max_id < b;
        };

        auto it = std::lower_bound(this->_meta_index.begin(), this->_meta_index.end(), key, comparator);

        if(it == this->_meta_index.end())
        {

            // for (const auto& entry : this->_meta_index)
            // {
            //     std::cout << "Meta index entry: " << entry.page_max_id << std::endl;
            //     std::cout << "Meta index size: " << this->_meta_index.size() << std::endl;
            // }

            // std::cout << "Key not found in meta index: " << key << std::endl;
            return std::nullopt;
        }
        return std::distance(this->_meta_index.begin(), it);
    }

    hedge::expected<std::optional<value_ptr_t>> sorted_index::lookup(const key_t& key) const
    {
        auto maybe_page_id = this->_find_page_id(key);

        if(!maybe_page_id.has_value())
            return std::nullopt;

        auto page_id = maybe_page_id.value();

        // std::cout << "Found page ID: " << page_id << " for key: " << key << std::endl;

        const index_key_t* page_start_ptr{};

        fs::non_owning_mmap mmap;

        if(!this->_index.empty())
        {
            page_start_ptr = this->_index.data();
        }
        else
        {
            OUTCOME_TRY(mmap, fs::non_owning_mmap::from_fd_wrapper(*this));

            page_start_ptr = reinterpret_cast<index_key_t*>(mmap.get_ptr());
        }

        page_start_ptr += page_id * INDEX_PAGE_NUM_ENTRIES;

        const index_key_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        return sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    async::task<expected<std::optional<value_ptr_t>>> sorted_index::lookup_async(const key_t& key, const std::shared_ptr<async::executor_context>& executor) const
    {
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return std::nullopt;

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        auto maybe_page_ptr = co_await this->_load_page_async(page_start_offset, executor);

        if(!maybe_page_ptr)
            co_return maybe_page_ptr.error();

        auto* page_start_ptr = reinterpret_cast<index_key_t*>(maybe_page_ptr.value().get());
        index_key_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        co_return sorted_index::_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    async::task<expected<std::unique_ptr<uint8_t>>> sorted_index::_load_page_async(size_t offset, const std::shared_ptr<async::executor_context>& executor) const
    {
        auto response = co_await executor->submit_request(
            async::read_request{
                .fd = this->get_fd(),
                .offset = offset,
                .size = PAGE_SIZE_IN_BYTES});

        if(response.error_code != 0)
        {
            auto err_msg = std::format(
                "An error occurred while reading page at offset {} from file {}:  {}",
                offset,
                this->path().string(),
                strerror(response.error_code));
            co_return hedge::error(err_msg);
        }

        if(response.bytes_read != PAGE_SIZE_IN_BYTES)
        {
            auto err_msg = std::format(
                "Read {} bytes instead of {} from file {} at offset {}",
                response.bytes_read,
                PAGE_SIZE_IN_BYTES,
                this->path().string(),
                offset);
            co_return hedge::error(err_msg);
        }

        co_return std::move(response.data);
    }

    std::optional<value_ptr_t> sorted_index::_find_in_page(const key_t& key, const index_key_t* start, const index_key_t* end)
    {
        const auto* it = std::lower_bound(start, end, index_key_t{.key = key, .value_ptr = {}});

        if(it != end && it->key == key)
            return it->value_ptr;

        // for(auto* it = start; it != end; ++it)
        // {
        // std::cout << "Index entry key: " << it->key << std::endl;
        // }

        return std::nullopt;
    }

    async::task<hedge::status> sorted_index::_update_in_page(const index_key_t& entry, size_t page_id, const index_key_t* start, const index_key_t* end, const std::shared_ptr<async::executor_context>& executor)
    {
        const auto* it = std::lower_bound(start, end, index_key_t{.key = entry.key, .value_ptr = {}});

        if(it == end || it->key != entry.key)
            co_return hedge::error("Key not found", errc::KEY_NOT_FOUND);

        const auto* entry_ptr = reinterpret_cast<const uint8_t*>(&entry);

        auto write_response = co_await executor->submit_request(async::write_request{
            .fd = this->get_fd(), // todo: use a rw fd or write fd + fsync at the end
            .data = const_cast<uint8_t*>(entry_ptr),
            .size = sizeof(index_key_t),
            .offset = (PAGE_SIZE_IN_BYTES * page_id) + (it - start),
        });

        if(write_response.error_code != 0)
            co_return hedge::error("An error occurred while updating an index entry: " + std::string(strerror(-write_response.error_code)));

        co_return hedge::ok();
    }

    async::task<hedge::status> sorted_index::try_update_async(const index_key_t& entry, const std::shared_ptr<async::executor_context>& executor)
    {
        std::unique_lock<std::mutex> lock(*this->_compaction_mutex, std::try_to_lock); // try to acquire

        if(!lock.owns_lock())
            co_return hedge::error("Busy, index is being compacted", errc::BUSY);

        auto maybe_page_id = this->_find_page_id(entry.key);
        if(!maybe_page_id)
            co_return hedge::error("Not found", errc::KEY_NOT_FOUND);

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_start_offset + (page_id * PAGE_SIZE_IN_BYTES);

        auto maybe_page_ptr = co_await this->_load_page_async(page_start_offset, executor);

        if(!maybe_page_ptr)
            co_return maybe_page_ptr.error();

        auto* page_start_ptr = reinterpret_cast<index_key_t*>(maybe_page_ptr.value().get());
        index_key_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        co_return co_await this->_update_in_page(entry, page_id, page_start_ptr, page_end_ptr, executor);
    }

    void sorted_index::clear_index()
    {
        this->_index = std::vector<index_key_t>{};
    }

    hedge::status sorted_index::load_index()
    {
        if(!this->_index.empty())
            return hedge::ok(); // already loaded

        auto mmap = fs::non_owning_mmap::from_fd_wrapper(*this);

        if(!mmap.has_value())
            throw std::runtime_error("Failed to mmap index file: " + mmap.error().to_string());

        auto* mmap_ptr = reinterpret_cast<index_key_t*>(mmap.value().get_ptr());

        this->_index.assign(mmap_ptr, mmap_ptr + this->_footer.indexed_keys);

        return hedge::ok();
    }

    sorted_index::sorted_index(fs::file fd, std::vector<index_key_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer)
        : fs::file(std::move(fd)), _index(std::move(index)), _meta_index(std::move(meta_index)), _footer(footer)
    {
    }

    template <typename T>
    size_t compute_alignment_padding(size_t element_count, size_t page_size = PAGE_SIZE_IN_BYTES)
    {
        size_t complement = page_size - ((element_count * sizeof(T)) % page_size);

        if(complement == page_size)
            return 0;

        return complement;
    }

    static std::vector<uint8_t> PADDING(PAGE_SIZE_IN_BYTES);

    template <typename T>
    std::pair<size_t, size_t> write_to(std::ofstream& ofs, const T& data, bool align)
    {
        ofs.write(reinterpret_cast<const char*>(&data), sizeof(T));

        size_t end_data_pos = ofs.tellp();

        if(align)
        {
            size_t padding_size = compute_alignment_padding<T>(1);

            if(padding_size > 0)
                ofs.write(reinterpret_cast<const char*>(PADDING.data()), static_cast<std::streamsize>(padding_size));
        }

        return {end_data_pos, ofs.tellp()};
    }

    template <typename T>
    std::pair<size_t, size_t> write_to(std::ofstream& ofs, const std::vector<T>& data, bool align)
    {
        ofs.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size() * sizeof(T)));

        size_t end_data_pos = ofs.tellp();

        if(align)
        {
            size_t padding_size = compute_alignment_padding<T>(data.size());

            if(padding_size > 0)
                ofs.write(reinterpret_cast<const char*>(PADDING.data()), static_cast<std::streamsize>(padding_size));
        }

        return {end_data_pos, ofs.tellp()};
    }

    template <typename T>
    void read_from(std::ifstream& ofs, T& data)
    {
        ofs.read(reinterpret_cast<char*>(&data), static_cast<std::streamsize>(sizeof(T)));
    }

    template <typename T>
    void read_from(std::ifstream& ofs, std::vector<T>& allocated_data)
    {
        ofs.read(reinterpret_cast<char*>(allocated_data.data()), static_cast<std::streamsize>(allocated_data.size() * sizeof(T)));
    }

    hedge::expected<sorted_index> index_ops::save_as_sorted_index(const std::filesystem::path& path, std::vector<index_key_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent)
    {
        if(merge_with_existent)
            return hedge::error("Merging with existing sorted index is not supported yet");

        auto meta_index = create_meta_index(sorted_keys);

        // std::cout << "Meta index size: " << meta_index.size() << std::endl;

        auto footer = sorted_index_footer{};

        {
            std::ofstream ofs_sorted_index(path, std::ios::binary);

            if(!ofs_sorted_index.good())
                return hedge::error("Failed to open sorted index file for writing: " + path.string());

            auto [end_of_index, end_of_index_padding] = write_to(ofs_sorted_index, sorted_keys, true);

            auto [end_of_meta_index, end_of_meta_index_padding] = write_to(ofs_sorted_index, meta_index, true);

            footer =
                {
                    .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                    .upper_bound = upper_bound,
                    .indexed_keys = sorted_keys.size(),
                    .meta_index_entries = ceil(sorted_keys.size(), INDEX_PAGE_NUM_ENTRIES),
                    .index_start_offset = 0,
                    .index_end_offset = end_of_index,
                    .meta_index_start_offset = end_of_index_padding,
                    .meta_index_end_offset = end_of_meta_index,
                    .footer_start_offset = end_of_meta_index_padding,
                };

            write_to(ofs_sorted_index, footer, false);

            if(!ofs_sorted_index.good())
                return hedge::error("Failed to write sorted index file: " + path.string());

            // std::cout << "Sorted index saved to: " << path.string() << std::endl;
        }

        OUTCOME_TRY(auto fd, fs::file::from_path(path, fs::file::open_mode::read_only, false, std::nullopt));

        auto ss = sorted_index(std::move(fd), std::move(sorted_keys), std::move(meta_index), footer);

        return ss;
    }

    hedge::expected<sorted_index> index_ops::load_sorted_index(const std::filesystem::path& path, bool load_index)
    {
        if(!std::filesystem::exists(path))
            return hedge::error("Sorted index file does not exist: " + path.string());

        auto fd_res = fs::file::from_path(path, fs::file::open_mode::read_only, false, std::nullopt);

        if(!fd_res.has_value())
            return hedge::error("Failed to open sorted index file: " + fd_res.error().to_string());

        // read footer first
        std::ifstream ifs(path, std::ios::binary);

        if(!ifs.good())
            return hedge::error("Failed to open sorted index file for reading: " + path.string());

        ifs.seekg(-static_cast<std::streamoff>(sizeof(sorted_index_footer)), std::ios::end);

        sorted_index_footer footer{};
        read_from(ifs, footer);

        if(!ifs.good())
            return hedge::error("Failed to read sorted index footer: " + path.string());

        std::vector<meta_index_entry> meta_index(footer.meta_index_entries);

        ifs.seekg(footer.meta_index_start_offset, std::ios::beg);

        read_from(ifs, meta_index);

        if(!ifs.good())
            return hedge::error("Failed to read sorted index meta index: " + path.string());

        auto ss = sorted_index(std::move(fd_res.value()), {}, std::move(meta_index), footer);

        if(!load_index)
            return ss;

        // read index if requested
        if(auto status = ss.load_index(); !status)
            return hedge::error("Failed to load sorted index: " + status.error().to_string());

        return ss;
    }

    std::pair<size_t, size_t> infer_index_end_pos(size_t indexed_keys)
    {
        size_t num_pages = ceil(indexed_keys, INDEX_PAGE_NUM_ENTRIES);
        size_t index_end_pos = num_pages * PAGE_SIZE_IN_BYTES;
        size_t padding = compute_alignment_padding<index_key_t>(indexed_keys);

        return {index_end_pos - padding, index_end_pos};
    }

    async::task<hedge::expected<sorted_index>> index_ops::two_way_merge_async(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor)
    {
        std::unique_lock lk_left(*left._compaction_mutex);
        std::unique_lock lk_right(*right._compaction_mutex);

        if(config.read_ahead_size < PAGE_SIZE_IN_BYTES)
            co_return hedge::error("Read ahead size must be at least one page size");

        if(config.read_ahead_size % PAGE_SIZE_IN_BYTES != 0)
            co_return hedge::error("Read ahead size must be page aligned (page size: " + std::to_string(PAGE_SIZE_IN_BYTES) + ")");

        if(left._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION ||
           right._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION)
            co_return hedge::error("Cannot merge sorted indices with different versions");

        if(left._footer.upper_bound != right._footer.upper_bound)
            co_return hedge::error("Cannot merge sorted indices with different upper bounds");

        footer_builder footer_builder;
        footer_builder.upper_bound = left._footer.upper_bound;

        auto lhs_view = fs::file_reader(
            left,
            {
                .start_offset = 0,
                .end_offset = left._footer.index_end_offset,
            },
            executor);

        auto rhs_view = fs::file_reader(
            right,
            {
                .start_offset = 0,
                .end_offset = right._footer.index_end_offset,
            },
            executor);

        // extrapolate path
        auto [dir, file_name] = format_prefix(left.upper_bound());
        auto new_path = config.base_path / dir / with_extension(file_name, std::format(".{}", config.new_index_id));

        auto fd_maybe = co_await fs::file::from_path_async(new_path, fs::file::open_mode::write_new, executor, false);

        if(!fd_maybe.has_value())
            co_return hedge::error("Failed to create file descriptor for merged index at " + new_path.string() + ": " + fd_maybe.error().to_string());

        auto fd = std::move(fd_maybe.value());

        auto lhs_rbuf = rolling_buffer(std::move(lhs_view));
        auto rhs_rbuf = rolling_buffer(std::move(rhs_view));

        auto init_lhs = co_await lhs_rbuf.next(config.read_ahead_size);

        if(!init_lhs)
            co_return hedge::error("Some error occurred while getting the first page from LHS inde: " + init_lhs.error().to_string());

        auto init_rhs = co_await rhs_rbuf.next(config.read_ahead_size);

        if(!init_rhs)
            co_return hedge::error("Some error occurred while getting the first page from RHS inde: " + init_rhs.error().to_string());

        size_t indexed_keys = 0;
        [[maybe_unused]] size_t filtered_keys = 0; // todo: this was used for an assert that is currently disabled
        size_t bytes_written = 0;

        std::vector<meta_index_entry> merged_meta_index;

        // for shortening syntax
        auto& lhs = lhs_rbuf;
        auto& rhs = rhs_rbuf;

        auto refresh_buffers = [&]() -> async::task<hedge::status>
        {
            auto status = co_await lhs.next(config.read_ahead_size);

            if(!status)
                co_return hedge::error("Cannot refresh LHS view: " + status.error().to_string());

            status = co_await rhs.next(config.read_ahead_size);

            if(!status)
                co_return hedge::error("Cannot refresh RHS view: " + status.error().to_string());

            co_return hedge::ok();
        };

        unique_buffer ubuf{}; // needed to handle possible duplicated keys between LHS and RHS
        uuids::uuid last_written_key{};

        footer_builder.index_start_offset = 0;

        // start writing the actual index
        while(true)
        {
            std::vector<uint8_t> merged_keys(config.read_ahead_size * 2);
            auto merged_keys_span = view_as<index_key_t>(merged_keys);
            auto merged_it = merged_keys_span.begin();

            while(lhs.it() != lhs.end() && rhs.it() != rhs.end())
            {
                if(*lhs.it() < *rhs.it())
                {
                    ubuf.push(*lhs.it());
                    ++lhs.it();
                }
                else
                {
                    ubuf.push(*rhs.it());
                    ++rhs.it();
                }

                if(ubuf.ready())
                {
                    auto new_item = ubuf.pop();

                    if(config.discard_deleted_keys && new_item.value_ptr.is_deleted())
                    {
                        filtered_keys++;
                        continue;
                    }

                    *merged_it = new_item;
                    last_written_key = new_item.key;

                    indexed_keys++;
                    if(indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0)
                        merged_meta_index.emplace_back(last_written_key);

                    if(++merged_it == merged_keys_span.end())
                        break;
                }
            }

            auto this_run_keys = std::distance(merged_keys_span.begin(), merged_it);
            merged_keys.resize(this_run_keys * sizeof(index_key_t));

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

            if(lhs.eof() || rhs.eof())
                break;
        }

        auto& non_empty_view = !lhs.eof() ? lhs : rhs;

        std::vector<index_key_t> remaining_keys;
        remaining_keys.reserve((non_empty_view.buffer().size() + 1) / sizeof(index_key_t));

        if(!non_empty_view.eof() && non_empty_view.it() != non_empty_view.end())
        {
            ubuf.push(*non_empty_view.it());
            ++non_empty_view.it();
        }
        // from now on, we can ignore the ubuf since there are no duplicated keys within the same index

        std::vector<index_key_t> last_items{};
        last_items.reserve(2);
        if(ubuf.ready())
            last_items.push_back(ubuf.pop());

        last_items.push_back(ubuf.force_pop());

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

        while(!non_empty_view.eof())
        {
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

            if(!remaining_keys.empty())
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get_fd(),
                    .data = reinterpret_cast<uint8_t*>(remaining_keys.data()),
                    .size = remaining_keys.size() * sizeof(index_key_t),
                    .offset = bytes_written});

                remaining_keys.clear();

                if(res.error_code != 0)
                    co_return hedge::error("Failed to write remaining keys to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            auto refresh_status = co_await non_empty_view.next(config.read_ahead_size);

            if(!refresh_status)
                co_return hedge::error("Failed to refresh view: " + refresh_status.error().to_string());
        }

        if(indexed_keys % INDEX_PAGE_NUM_ENTRIES != 0)
            merged_meta_index.emplace_back(last_written_key);

        auto refresh_status = co_await refresh_buffers();
        if(!refresh_status)
            co_return hedge::error("Failed to refresh views after writing remaining keys: " + refresh_status.error().to_string());

        // assert(indexed_keys == (left._footer.indexed_keys + right._footer.indexed_keys - filtered_keys) && "Item count does not match footer indexed keys");
        footer_builder.indexed_keys = indexed_keys;
        footer_builder.index_end_offset = bytes_written;

        // write index padding if any
        size_t padding_size = compute_alignment_padding<index_key_t>(indexed_keys);
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

        footer_builder.meta_index_start_offset = bytes_written;
        footer_builder.meta_index_entries = merged_meta_index.size();

        // write meta index
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

        footer_builder.meta_index_end_offset = bytes_written;

        // write meta index padding if any
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

        footer_builder.footer_start_offset = bytes_written;

        auto maybe_footer = footer_builder.build();
        if(!maybe_footer.has_value())
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        auto& footer = maybe_footer.value();

        // write footer
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

        // fsync(fd.get());

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