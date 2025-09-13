#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <unistd.h>
#include <vector>

#include <error.hpp>
#include <thread>

#include "common.h"
#include "fs.hpp"
#include "index.h"
#include "mailbox_impl.h"
#include "uuid.h"

#include "io_executor.h"
#include "paginated_view.h"
#include "task.h"

namespace hedgehog::db
{

    struct partitioned_view
    {
        std::vector<index_key_t>::const_iterator start;
        std::vector<index_key_t>::const_iterator end;

        size_t size() const
        {
            return std::distance(start, end);
        }
    };

    hedgehog::status merge_with_target(const std::filesystem::path& target_path, const partitioned_view& partition)
    {
        bool first_creation = !std::filesystem::exists(target_path);

        std::vector<index_key_t> partitioned_data;

        if(!first_creation)
        {
            auto file_size = std::filesystem::file_size(target_path);

            auto reader = std::ifstream(target_path, std::ios::binary);

            if(!reader.good())
                return hedgehog::error("Failed to open partition file: " + target_path.string());

            partitioned_data.reserve(file_size / sizeof(index_key_t) + partition.size());

            partitioned_data.resize(file_size / sizeof(index_key_t));

            reader.read(reinterpret_cast<char*>(partitioned_data.data()), static_cast<std::streamsize>(file_size));
        }

        partitioned_data.insert(partitioned_data.end(), partition.start, partition.end);
        std::sort(partitioned_data.begin(), partitioned_data.end());

        auto tmp_path = with_extension(target_path, ".tmp");
        {
            auto partitioned_file = std::ofstream(tmp_path, std::ios::binary | std::ios::trunc);
            if(!partitioned_file.good())
                return hedgehog::error("Failed to open partition file for writing: " + tmp_path.string());

            partitioned_file.write(reinterpret_cast<const char*>(partitioned_data.data()), static_cast<std::streamsize>(partitioned_data.size() * sizeof(index_key_t)));

            if(!partitioned_file.good())
                return hedgehog::error("Failed to write partition data to file: " + tmp_path.string());
        }

        if(!first_creation)
            std::filesystem::rename(target_path, with_extension(target_path, ".old"));

        std::filesystem::rename(tmp_path, target_path);

        if(!first_creation)
            std::filesystem::remove(with_extension(target_path, ".old"));

        return hedgehog::ok();
    }

    index_key_t _key_from_prefix(uint16_t prefix)
    {
        auto key = uuids::uuid{};

        auto& uuids_as_std_array = reinterpret_cast<std::array<uint16_t, 16 / sizeof(uint16_t)>&>(key);

        std::fill(uuids_as_std_array.begin(), uuids_as_std_array.end(), 0);

        uuids_as_std_array[0] = prefix;

        return index_key_t{key, {}};
    };

    // std::vector<partitioned_view> find_partitions(const std::vector<index_key_t>& index_sorted)
    // {
    //     constexpr auto PREFIX_MAX = std::numeric_limits<uint16_t>::max();

    //     std::vector<partitioned_view> partitions;
    //     partitions.reserve(PREFIX_MAX);

    //     partitioned_view current_partition;

    //     current_partition.prefix = extract_prefix(index_sorted.begin()->key);
    //     current_partition.start = index_sorted.begin();

    //     // auto range_start = index_sorted.begin();
    //     for(auto it = index_sorted.begin() + 1; it != index_sorted.end(); ++it)
    //     {
    //         auto prefix = extract_prefix(it->key);

    //         if(prefix != current_partition.prefix)
    //         {
    //             current_partition.end = it;
    //             partitions.emplace_back(current_partition);

    //             current_partition.prefix = prefix;
    //             current_partition.start = it;
    //         }
    //     }

    //     current_partition.end = index_sorted.end();
    //     partitions.emplace_back(current_partition);

    //     return partitions;
    // }

    std::vector<meta_index_entry> create_meta_index(const std::vector<index_key_t>& sorted_keys)
    {
        auto meta_index_size = (sorted_keys.size() + INDEX_PAGE_NUM_ENTRIES - 1) / INDEX_PAGE_NUM_ENTRIES;

        auto meta_index = std::vector<meta_index_entry>{};

        meta_index.reserve(meta_index_size);

        for(size_t i = 0; i < meta_index_size; ++i)
        {
            auto idx = std::min(i * INDEX_PAGE_NUM_ENTRIES + INDEX_PAGE_NUM_ENTRIES - 1, sorted_keys.size() - 1);
            meta_index.push_back({.page_max_id = sorted_keys[idx].key});
        }

        meta_index.shrink_to_fit();

        return meta_index;
    }

    std::filesystem::path get_index_file_path(const std::filesystem::path& path)
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

    hedgehog::expected<std::vector<sorted_index>> index_ops::merge_and_flush(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent)
    {
        if(num_partition_exponent > 16)
            return hedgehog::error("Number of partitions exponent must be less than or equal to 16");

        size_t num_space_partitions = 1 << num_partition_exponent; // 2^num_partition_exponent

        std::cout << "num space partitions: " << num_space_partitions << std::endl;

        auto partition_size = (1 << 16) / num_space_partitions;

        auto index_sorted = merge_memtables_in_mem(std::move(indices));

        std::vector<sorted_index> sorted_indices{};
        std::vector<index_key_t> current_index{};

        size_t current_partition = hedgehog::find_partition_prefix_for_key(index_sorted[0].key, partition_size);

        std::cout << "First element " << index_sorted[0].key << " with prefix: " << hedgehog::find_partition_prefix_for_key(index_sorted[0].key, partition_size) << std::endl;

        // todo: upon error, the db might end in undefined state

        for(auto it = index_sorted.begin(); it != index_sorted.end(); ++it)
        {
            const auto& key = *it;
            current_index.push_back(key);

            bool last_entry_for_partition =
                (it + 1 == index_sorted.end()) || (hedgehog::find_partition_prefix_for_key((it + 1)->key, partition_size) != current_partition);

            if(last_entry_for_partition)
            {
                auto [dir_prefix, file_prefix] = format_prefix(current_partition);
                auto path = get_index_file_path(base_path / dir_prefix / file_prefix);

                // std::cout << "Saving partition with prefix: " << file_prefix << " to path: " << path.string() << std::endl;

                current_index.shrink_to_fit();

                OUTCOME_TRY(auto sorted_index, index_ops::save_as_sorted_index(
                                                   path,
                                                   std::exchange(current_index, std::vector<index_key_t>{}),
                                                   current_partition,
                                                   false));

                sorted_indices.push_back(std::move(sorted_index));

                current_partition += partition_size;
            }
        }

        // todo: if OK, clear mem_index-es WALs
        // todo because WAL is not implemented yet

        return sorted_indices;
    }

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key)
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

    hedgehog::expected<std::optional<value_ptr_t>> sorted_index::lookup(const key_t& key)
    {
        auto maybe_page_id = this->_find_page_id(key);

        if(!maybe_page_id.has_value())
            return std::nullopt;

        auto page_id = maybe_page_id.value();

        // std::cout << "Found page ID: " << page_id << " for key: " << key << std::endl;

        index_key_t* page_start_ptr{};

        fs::tmp_mmap mmap;

        if(!this->_index.empty())
        {
            page_start_ptr = this->_index.data();
        }
        else
        {
            OUTCOME_TRY(mmap, fs::tmp_mmap::from_fd_wrapper(&this->_fd));

            page_start_ptr = reinterpret_cast<index_key_t*>(mmap.get_ptr());
        }

        page_start_ptr += page_id * INDEX_PAGE_NUM_ENTRIES;

        index_key_t* page_end_ptr = page_start_ptr + INDEX_PAGE_NUM_ENTRIES;

        if(bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if(last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        return this->_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    std::optional<value_ptr_t> sorted_index::_find_in_page(const key_t& key, const index_key_t* start, const index_key_t* end)
    {

        for(auto* ptr = start; ptr < end; ++ptr)
        {
            // std::cout << "Checking key: " << ptr->key << std::endl;
        }

        auto it = std::lower_bound(start, end, index_key_t{key, {}});

        if(it != end && it->key == key)
            return it->value_ptr;

        return std::nullopt;
    }

    void sorted_index::clear_index()
    {
        this->_index = std::vector<index_key_t>{};
    }

    hedgehog::status sorted_index::load_index()
    {
        if(!this->_index.empty())
            return hedgehog::ok(); // already loaded

        auto mmap = fs::tmp_mmap::from_fd_wrapper(&this->_fd);

        if(!mmap.has_value())
            throw std::runtime_error("Failed to mmap index file: " + mmap.error().to_string());

        auto* mmap_ptr = reinterpret_cast<index_key_t*>(mmap.value().get_ptr());

        this->_index.assign(mmap_ptr, mmap_ptr + this->_footer.indexed_keys);

        return hedgehog::ok();
    }

    sorted_index::sorted_index(fs::file_descriptor fd, std::vector<index_key_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer)
        : _fd(std::move(fd)), _index(std::move(index)), _meta_index(std::move(meta_index)), _footer(std::move(footer))
    {
    }

    template <typename T>
    size_t compute_alignment_padding(size_t element_count, size_t page_size = FS_PAGE_SIZE_BYTES)
    {
        size_t complement = page_size - ((element_count * sizeof(T)) % page_size);

        if(complement == page_size)
            return 0;

        return complement;
    }

    static std::vector<uint8_t> PADDING(FS_PAGE_SIZE_BYTES);

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

    hedgehog::expected<sorted_index> index_ops::save_as_sorted_index(const std::filesystem::path& path, std::vector<index_key_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent)
    {
        if(merge_with_existent)
            return hedgehog::error("Merging with existing sorted index is not supported yet");

        auto meta_index = create_meta_index(sorted_keys);

        // std::cout << "Meta index size: " << meta_index.size() << std::endl;

        auto footer = sorted_index_footer{};

        {
            std::ofstream ofs_sorted_index(path, std::ios::binary);

            if(!ofs_sorted_index.good())
                return hedgehog::error("Failed to open sorted index file for writing: " + path.string());

            auto [end_of_index, end_of_index_padding] = write_to(ofs_sorted_index, sorted_keys, true);

            auto [end_of_meta_index, end_of_meta_index_padding] = write_to(ofs_sorted_index, meta_index, true);

            footer =
                {
                    .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                    .upper_bound = upper_bound,
                    .min_key = sorted_keys.front().key,
                    .max_key = sorted_keys.back().key,
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
                return hedgehog::error("Failed to write sorted index file: " + path.string());

            // std::cout << "Sorted index saved to: " << path.string() << std::endl;
        }

        OUTCOME_TRY(auto fd, fs::file_descriptor::from_path(path, fs::file_descriptor::open_mode::read_only, false, std::nullopt));

        auto ss = sorted_index(std::move(fd), std::move(sorted_keys), std::move(meta_index), footer);

        return ss;
    }

    hedgehog::expected<sorted_index> index_ops::load_sorted_index(const std::filesystem::path& path, bool load_index)
    {
        if(!std::filesystem::exists(path))
            return hedgehog::error("Sorted index file does not exist: " + path.string());

        auto fd_res = fs::file_descriptor::from_path(path, fs::file_descriptor::open_mode::read_only, false, std::nullopt);

        if(!fd_res.has_value())
            return hedgehog::error("Failed to open sorted index file: " + fd_res.error().to_string());

        // read footer first
        std::ifstream ifs(path, std::ios::binary);

        if(!ifs.good())
            return hedgehog::error("Failed to open sorted index file for reading: " + path.string());

        ifs.seekg(-static_cast<std::streamoff>(sizeof(sorted_index_footer)), std::ios::end);

        sorted_index_footer footer{};
        read_from(ifs, footer);

        if(!ifs.good())
            return hedgehog::error("Failed to read sorted index footer: " + path.string());

        std::vector<meta_index_entry> meta_index(footer.meta_index_entries);

        ifs.seekg(footer.meta_index_start_offset, std::ios::beg);

        read_from(ifs, meta_index);

        if(!ifs.good())
            return hedgehog::error("Failed to read sorted index meta index: " + path.string());

        auto ss = sorted_index(std::move(fd_res.value()), {}, std::move(meta_index), footer);

        if(!load_index)
            return ss;

        // read index if requested
        if(auto status = ss.load_index(); !status)
            return hedgehog::error("Failed to load sorted index: " + status.error().to_string());

        return ss;
    }

    std::pair<size_t, size_t> infer_index_end_pos(size_t indexed_keys)
    {
        size_t num_pages = ceil(indexed_keys, INDEX_PAGE_NUM_ENTRIES);
        size_t index_end_pos = num_pages * FS_PAGE_SIZE_BYTES;
        size_t padding = compute_alignment_padding<index_key_t>(indexed_keys);

        return {index_end_pos - padding, index_end_pos};
    }

    hedgehog::expected<sorted_index> index_ops::two_way_merge(const std::filesystem::path& base_path, size_t read_ahead_size, const sorted_index& left, const sorted_index& right, std::shared_ptr<async::executor_context> executor)
    {
        if(read_ahead_size < FS_PAGE_SIZE_BYTES)
            return hedgehog::error("Read ahead size must be at least one page size");

        if(left._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION ||
           right._footer.version != sorted_index_footer::CURRENT_FOOTER_VERSION)
            return hedgehog::error("Cannot merge sorted indices with different versions");

        if(left._footer.upper_bound != right._footer.upper_bound)
            return hedgehog::error("Cannot merge sorted indices with different upper bounds");

        auto new_table_num_keys = left.size() + right.size();
        auto [index_end_pos, meta_index_start_pos] = infer_index_end_pos(new_table_num_keys);
        auto meta_index_entries = ceil(new_table_num_keys, INDEX_PAGE_NUM_ENTRIES);
        auto meta_index_end_pos = meta_index_start_pos + meta_index_entries * sizeof(meta_index_entry);
        auto footer_start_offset = meta_index_end_pos + compute_alignment_padding<meta_index_entry>(meta_index_entries);

        sorted_index_footer footer =
            {
                .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                .upper_bound = left._footer.upper_bound,
                .min_key = std::min(left._footer.min_key, right._footer.min_key),
                .max_key = std::max(left._footer.max_key, right._footer.max_key),
                .indexed_keys = new_table_num_keys,
                .meta_index_entries = meta_index_entries,
                .index_start_offset = 0,
                .index_end_offset = index_end_pos,
                .meta_index_start_offset = meta_index_start_pos,
                .meta_index_end_offset = meta_index_end_pos,
                .footer_start_offset = meta_index_end_pos,
            };

        auto last_page_size = [](const auto& footer)
        {
            return footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES == 0 ? INDEX_PAGE_NUM_ENTRIES : footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
        };

        auto lhs_view = async::paginated_view(
            left._fd.get(),
            {0,
             left._meta_index.size(),
             FS_PAGE_SIZE_BYTES},
            executor);

        auto rhs_view = async::paginated_view(
            right._fd.get(),
            {0,
             right._meta_index.size(),
             FS_PAGE_SIZE_BYTES},
            executor);

        auto [dir_prefix, file_prefix] = format_prefix(left._footer.upper_bound);
        auto new_path = get_index_file_path(base_path / dir_prefix / file_prefix);

        std::cout << "Creating new index file at: " << new_path.string() << std::endl;

        auto fd_maybe = fs::file_descriptor::from_path(new_path, fs::file_descriptor::open_mode::write_new, false, index_end_pos + meta_index_start_pos);

        if(!fd_maybe.has_value())
            return hedgehog::error("Failed to create file descriptor for merged index: " + fd_maybe.error().to_string());

        auto fd = std::move(fd_maybe.value());

        auto task = [&]() -> async::task<expected<hedgehog::db::sorted_index>>
        {
            using data_view = std::pair<std::vector<uint8_t>, std::span<index_key_t>>;
            using ret_type = hedgehog::expected<data_view>;

            auto lhs_it = std::span<index_key_t>::iterator{};
            auto rhs_it = std::span<index_key_t>::iterator{};

            data_view lhs_data_view{};
            data_view rhs_data_view{};

            static size_t lhs_loaded_keys{};
            static size_t rhs_loaded_keys{};

            auto next_from_view = [](async::paginated_view& view, size_t read_ahead_size) -> async::task<ret_type>
            {
                auto maybe_keys = co_await view.next(read_ahead_size);
                if(!maybe_keys.has_value())
                    co_return hedgehog::error("Failed to read from index view: " + maybe_keys.error().to_string());

                auto& keys = maybe_keys.value();

                auto keys_span = view_as<index_key_t>(keys);
                co_return std::make_pair(std::move(keys), keys_span);
            };

            auto init_views = [&]() -> async::task<hedgehog::status>
            {
                auto maybe_lhs_data_view = co_await next_from_view(lhs_view, read_ahead_size);

                if(!maybe_lhs_data_view.has_value())
                    co_return hedgehog::error("Failed to read from left index view");

                auto maybe_rhs_data_view = co_await next_from_view(rhs_view, read_ahead_size);
                if(!maybe_rhs_data_view.has_value())
                    co_return hedgehog::error("Failed to read from right index view");

                std::tie(lhs_data_view, rhs_data_view) = std::make_pair(std::move(maybe_lhs_data_view.value()),
                                                                        std::move(maybe_rhs_data_view.value()));

                lhs_loaded_keys = lhs_data_view.second.size();
                rhs_loaded_keys = rhs_data_view.second.size();

                if(lhs_view.is_eof())
                {
                    lhs_data_view.first.resize(sizeof(index_key_t) * last_page_size(left._footer));
                    lhs_data_view.second = view_as<index_key_t>(lhs_data_view.first);
                    lhs_loaded_keys = lhs_data_view.second.size();
                }

                if(rhs_view.is_eof())
                {
                    rhs_data_view.first.resize(sizeof(index_key_t) * last_page_size(right._footer));
                    rhs_data_view.second = view_as<index_key_t>(rhs_data_view.first);
                    rhs_loaded_keys = rhs_data_view.second.size();
                }

                lhs_it = lhs_data_view.second.begin();
                rhs_it = rhs_data_view.second.begin();

                co_return hedgehog::ok();
            };

            auto transform_it = [](const std::span<index_key_t>::iterator& iterator) -> std::vector<uint8_t>::iterator
            {
                return std::vector<uint8_t>::iterator(reinterpret_cast<uint8_t*>(iterator.base()));
            };

            auto erase_read_append_new = [&transform_it](std::vector<uint8_t>& data, std::vector<uint8_t>& new_read, std::span<index_key_t>& span, std::span<index_key_t>::iterator& it)
            {
                data.erase(data.begin(), transform_it(it));
                data.insert(data.end(), new_read.begin(), new_read.end());
                span = view_as<index_key_t>(data);
                it = span.begin();
            };

            auto refresh_view = [&erase_read_append_new, &next_from_view, read_ahead_size](auto& view, data_view& data_view, std::span<index_key_t>::iterator& it, size_t last_page_size) -> async::task<hedgehog::status>
            {
                auto maybe_data_view = co_await next_from_view(view, read_ahead_size);
                if(!maybe_data_view.has_value())
                    co_return hedgehog::error("Failed to read from left index view");

                auto& [data, _] = maybe_data_view.value();
                if(!data.empty() && view.is_eof())
                    data.resize(sizeof(index_key_t) * last_page_size);

                erase_read_append_new(data_view.first, data, data_view.second, it);

                co_return hedgehog::ok();
            };

            auto refresh_views = [&]() -> async::task<hedgehog::status>
            {
                if(lhs_it == lhs_data_view.second.end())
                {
                    auto status = co_await refresh_view(lhs_view, lhs_data_view, lhs_it, last_page_size(left._footer));

                    lhs_loaded_keys += lhs_data_view.second.size();

                    if(!status)
                        co_return status;
                }

                if(rhs_it == rhs_data_view.second.end())
                {
                    auto status = co_await refresh_view(rhs_view, rhs_data_view, rhs_it, last_page_size(right._footer));

                    rhs_loaded_keys += rhs_data_view.second.size();

                    if(!status)
                        co_return status;
                }

                co_return hedgehog::ok();
            };

            auto status = co_await init_views();
            if(!status)
                co_return hedgehog::error("Failed to initialize views: " + status.error().to_string());

            size_t index_key_count = 0;
            size_t bytes_written = 0;

            std::vector<meta_index_entry> merged_meta_index;

            while(true)
            {
                std::vector<uint8_t> merged_keys(read_ahead_size * 2);
                auto merged_keys_span = view_as<index_key_t>(merged_keys);
                auto merged_it = merged_keys_span.begin();

                auto& [lhs_data, lhs_span] = lhs_data_view;
                auto& [rhs_data, rhs_span] = rhs_data_view;

                while(lhs_it != lhs_span.end() && rhs_it != rhs_span.end())
                {
                    if(*lhs_it < *rhs_it)
                    {
                        *merged_it = *lhs_it;
                        ++lhs_it;
                    }
                    else if(*rhs_it < *lhs_it)
                    {
                        *merged_it = *rhs_it;
                        ++rhs_it;
                    }

                    index_key_count++;
                    if(index_key_count % INDEX_PAGE_NUM_ENTRIES == 0)
                        merged_meta_index.emplace_back(merged_it->key);

                    if(++merged_it == merged_keys_span.end())
                        break;
                }

                auto this_run_keys = std::distance(merged_keys_span.begin(), merged_it);
                merged_keys.resize(this_run_keys * sizeof(index_key_t));

                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get(),
                    .data = std::move(merged_keys),
                    .offset = bytes_written});

                bytes_written += res.bytes_written;

                if(res.error_code != 0)
                    co_return hedgehog::error("Failed to write merged keys to file: " + std::string(strerror(res.error_code)));

                auto status = co_await refresh_views();
                if(!status)
                    co_return hedgehog::error("Failed to refresh views: " + status.error().to_string());

                if(lhs_data_view.first.empty() || rhs_data_view.first.empty())
                    break;
            }

            std::cout << "Merged " << index_key_count << " keys" << std::endl;

            assert(lhs_data_view.first.empty() ^ rhs_data_view.first.empty() && "Either one of the views should be empty at this point");

            // todo: potrebbe accadere che uno dei due buffer non sia EOF. bisogna continuare a caricare finche' non finisce

            auto& non_empty_data_view = !lhs_data_view.first.empty() ? lhs_data_view : rhs_data_view;
            auto& non_empty_iterator = !lhs_data_view.first.empty() ? lhs_it : rhs_it;
            auto transformed_it = transform_it(non_empty_iterator);

            if(new_path.string() == "/tmp/hh/test/3b/3b3f.2")
                std::cout << "breakpoint\n";

            non_empty_data_view.first.erase(non_empty_data_view.first.begin(), transformed_it);
            non_empty_data_view.second = view_as<index_key_t>(non_empty_data_view.first);

            while(true)
            {
                non_empty_iterator = non_empty_data_view.second.begin();

                for(auto it = non_empty_iterator; it != non_empty_data_view.second.end(); it++)
                {
                    ++index_key_count;

                    if(index_key_count % INDEX_PAGE_NUM_ENTRIES == 0)
                        merged_meta_index.emplace_back(it->key);
                }

                // handle last page entry
                if(index_key_count % INDEX_PAGE_NUM_ENTRIES != 0 && (lhs_view.is_eof() && rhs_view.is_eof()))
                    merged_meta_index.emplace_back(non_empty_data_view.second.back().key);

                if(!non_empty_data_view.first.empty())
                {
                    auto res = co_await executor->submit_request(async::write_request{
                        .fd = fd.get(),
                        .data = non_empty_data_view.first,
                        .offset = bytes_written});

                    if(res.error_code != 0)
                        co_return hedgehog::error("Failed to write remaining keys to file: " + std::string(strerror(res.error_code)));

                    bytes_written += res.bytes_written;
                }

                non_empty_iterator = non_empty_data_view.second.end();
                auto refresh_status = co_await refresh_views();

                if(!refresh_status)
                    co_return hedgehog::error("Failed to refresh views: " + refresh_status.error().to_string());

                if(lhs_view.is_eof() && lhs_data_view.first.empty() && rhs_view.is_eof() && rhs_data_view.first.empty())
                    break;
            }

            auto refresh_status = co_await refresh_views();
            if(!refresh_status)
                co_return hedgehog::error("Failed to refresh views after writing remaining keys: " + refresh_status.error().to_string());

            std::cout << "Merged " << index_key_count << " keys in total" << std::endl;
            std::cout << "lhs loaded keys: " << lhs_loaded_keys << std::endl;
            std::cout << "rhs loaded keys: " << rhs_loaded_keys << std::endl;
            std::cout << "lhs + rhs loaded keys: " << lhs_loaded_keys + rhs_loaded_keys << std::endl;

            assert(index_key_count == footer.indexed_keys && "Item count does not match footer indexed keys");
            assert(index_key_count * sizeof(index_key_t) == footer.index_end_offset && "Item count does not match footer index end offset");

            // write index padding if any
            size_t padding_size = compute_alignment_padding<index_key_t>(index_key_count);
            if(padding_size > 0)
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get(),
                    .data = std::vector<uint8_t>(padding_size, 0),
                    .offset = bytes_written});

                if(res.error_code != 0)
                    co_return hedgehog::error("Failed to write padding to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            // write meta index
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get(),
                    .data = std::vector<uint8_t>(reinterpret_cast<uint8_t*>(merged_meta_index.data()), reinterpret_cast<uint8_t*>(merged_meta_index.data()) + merged_meta_index.size() * sizeof(meta_index_entry)),
                    .offset = bytes_written});

                if(res.error_code != 0)
                    co_return hedgehog::error("Failed to write meta index to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            // write meta index padding if any
            size_t meta_index_padding_size = compute_alignment_padding<meta_index_entry>(merged_meta_index.size());
            if(meta_index_padding_size > 0)
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get(),
                    .data = std::vector<uint8_t>(meta_index_padding_size, 0),
                    .offset = bytes_written});

                if(res.error_code != 0)
                    co_return hedgehog::error("Failed to write meta index padding to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            // write footer
            {
                auto res = co_await executor->submit_request(async::write_request{
                    .fd = fd.get(),
                    .data = std::vector<uint8_t>(reinterpret_cast<uint8_t*>(&footer), reinterpret_cast<uint8_t*>(&footer) + sizeof(footer)),
                    .offset = bytes_written});

                if(res.error_code != 0)
                    co_return hedgehog::error("Failed to write footer to file: " + std::string(strerror(res.error_code)));

                bytes_written += res.bytes_written;
            }

            fsync(fd.get());

            auto read_fd = fs::file_descriptor::from_path(fd.path(), fs::file_descriptor::open_mode::read_only, false, bytes_written);
            if(!read_fd.has_value())
                co_return hedgehog::error("Failed to open merged index file for reading: " + read_fd.error().to_string());

            sorted_index result{
                std::move(read_fd.value()),
                {},
                std::move(merged_meta_index),
                std::move(footer)};

            co_return result;
        };

        auto maybe_sorted_index = executor->sync_submit(task());

        return maybe_sorted_index;
    }

} // namespace hedgehog::db