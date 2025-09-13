#include <cstdint>
#include <unistd.h>
#include <vector>
#include <limits>
#include <filesystem>
#include <fstream>

#include <error.hpp>
#include <thread>

#include "index.h"
#include "fs.hpp"
#include "tables.h"
#include "uuid.h"

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


    index_key_t _key_from_prefix(uint16_t prefix){
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
            auto idx = std::min(i*INDEX_PAGE_NUM_ENTRIES + INDEX_PAGE_NUM_ENTRIES - 1, sorted_keys.size() - 1);
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

        while(std::filesystem::exists(path))
        {
            ++count;
            new_path = with_extension(path, std::format(".{}", count));
        }

        return new_path;
    }

    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions)
    {
        auto partition_size = (1 << 16) / num_space_partitions;

        std::vector<size_t> prefixes;
        prefixes.reserve(num_space_partitions);

        for(size_t i = 1; i <= num_space_partitions + 1; ++i)
        {
            auto last_prefix_of_partition = (i * partition_size - 1);

            prefixes.push_back(last_prefix_of_partition);
            
            std::cout << "Partition " << i << ": " << format_prefix(last_prefix_of_partition).second << " value: " << last_prefix_of_partition << std::endl;
        }

        std::vector<std::pair<size_t, std::filesystem::path>> paths;

        paths.reserve(prefixes.size());

        for(const auto& prefix : prefixes)
        {
            auto [dir_prefix, file_prefix] = format_prefix(prefix);
            paths.emplace_back(prefix, base_path / dir_prefix / (file_prefix + ".0"));
        }

        return paths;
    }



    hedgehog::expected<std::vector<sorted_index>> index_ops::merge_and_flush(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent)
    {
        if (num_partition_exponent > 16)
            return hedgehog::error("Number of partitions exponent must be less than or equal to 16");

        auto total_size = std::accumulate(indices.begin(), indices.end(), 0, [](size_t acc, const mem_index& idx) {
            return acc + idx._index.size();
        });

        std::vector<index_key_t> index_sorted;
        index_sorted.reserve(total_size);

        for(auto& idx : indices)
        {
            for(const auto& [key, value] : idx._index)
                index_sorted.push_back({key, value});

            idx._index = mem_index::index_t{};
        }

        std::sort(index_sorted.begin(), index_sorted.end());

        size_t num_space_partitions = 1 << num_partition_exponent; // 2^num_partition_exponent

        std::cout << "num space partitions: " << num_space_partitions << std::endl;

        // idea: this could be a compile/templated time decision (or, maybe it is not very relevant)
        // but could be still cool!


        auto partition_size = (1 << 16) / num_space_partitions;


        std::vector<sorted_index> sorted_indices;
        std::vector<index_key_t> current_index{};

        auto find_partition_prefix = [partition_size](key_t key)
        {
            size_t key_prefix = extract_prefix(key);

            key_prefix += partition_size;
            key_prefix /= partition_size;
            key_prefix *= partition_size;

            return key_prefix - 1;
        };

        size_t current_prefix = find_partition_prefix(index_sorted[0].key);

        std::cout << "First element " << index_sorted[0].key << " with prefix: " << find_partition_prefix(index_sorted[0].key) << std::endl;

        // todo: upon error, the db might end in undefined state

        for(auto it = index_sorted.begin(); it != index_sorted.end(); ++it)
        {
            const auto& key = *it;            
            current_index.push_back(key);

            bool last_entry_for_partition =
                (it + 1 == index_sorted.end()) || (find_partition_prefix((it + 1)->key) != current_prefix);

            if(last_entry_for_partition)
            {
                auto [dir_prefix, file_prefix] = format_prefix(current_prefix);
                auto path = get_index_file_path(base_path / dir_prefix / file_prefix);

                std::cout << "Saving partition with prefix: " << file_prefix << " to path: " << path.string() << std::endl;

                current_index.shrink_to_fit();

                OUTCOME_TRY(auto sorted_index, index_ops::save_as_sorted_index(
                    path,
                    std::move(current_index),
                    current_prefix,
                    false
                ));

                sorted_indices.push_back(std::move(sorted_index));

                current_index = std::vector<index_key_t>{}; 
                current_prefix += partition_size;
            }
        }

        // todo: if OK, clear mem_index-es WALs
        // todo because WAL is not implemented yet

        return sorted_indices;
    }

    std::optional<size_t> sorted_index::_find_page_id(const key_t& key)
    {
        auto comparator = [](const meta_index_entry& a, const key_t& b) {
            return a.page_max_id < b;
        };

        auto it = std::lower_bound(this->_meta_index.begin(), this->_meta_index.end(), key, comparator);

        if (it == this->_meta_index.end()){

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

        if (!maybe_page_id.has_value())
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

        if (bool is_last_page = page_id == this->_meta_index.size() - 1; is_last_page)
        {
            size_t last_page_size = this->_footer.indexed_keys % INDEX_PAGE_NUM_ENTRIES;
            if (last_page_size != 0)
                page_end_ptr = page_start_ptr + last_page_size;
        }

        return this->_find_in_page(key, page_start_ptr, page_end_ptr);
    }

    std::optional<value_ptr_t> sorted_index::_find_in_page(const key_t& key, const index_key_t* start, const index_key_t* end)
    {

        for (auto* ptr = start;  ptr < end; ++ptr)
        {
            // std::cout << "Checking key: " << ptr->key << std::endl;
        }

        auto it = std::lower_bound(start, end, index_key_t{key, {}});

        if (it != end && it->key == key)
            return it->value_ptr;

        return std::nullopt;
    }

    void sorted_index::clear_index()
    {
        std::vector<index_key_t>().swap(this->_index);
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
    size_t compute_alignment_padding(size_t element_count, size_t page_size = INDEX_PAGE_SIZE_BYTES)
    {
        size_t complement = page_size - ((element_count * sizeof(T)) % page_size);

        if(complement == page_size)
            return 0;

        return complement;
    }

    static std::vector<uint8_t> PADDING(INDEX_PAGE_SIZE_BYTES);

    template <typename T>
    std::pair<size_t, size_t> write_to(std::ofstream& ofs, const T& data, bool align)
    {
        ofs.write(reinterpret_cast<const char*>(&data), sizeof(T));

        size_t end_data_pos = ofs.tellp();

        if (align)
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
            
        if (align)
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

    size_t ceil(size_t value, size_t divisor)
    {
        return (value + divisor - 1) / divisor;
    }

    hedgehog::expected<sorted_index> index_ops::save_as_sorted_index(const std::filesystem::path& path, std::vector<index_key_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent)
    {
        if(merge_with_existent)
            return hedgehog::error("Merging with existing sorted index is not supported yet");

        auto meta_index = create_meta_index(sorted_keys);

        std::cout << "Meta index size: " << meta_index.size() << std::endl;

        auto footer = sorted_index_footer{};

        {
            std::ofstream ofs_sorted_index(path, std::ios::binary);

            if(!ofs_sorted_index.good())
                return hedgehog::error("Failed to open sorted index file for writing: " + path.string());

            auto [end_of_index, end_of_index_padding] = write_to(ofs_sorted_index, sorted_keys, true);

            write_to(ofs_sorted_index, meta_index, true);

            footer = 
            {
                .version = sorted_index_footer::CURRENT_FOOTER_VERSION,
                .upper_bound = upper_bound,
                .min_key = sorted_keys.front().key,
                .max_key = sorted_keys.back().key,
                .indexed_keys = sorted_keys.size(),
                .meta_index_pages = ceil(sorted_keys.size(), INDEX_PAGE_NUM_ENTRIES),
                .index_end_pos = end_of_index,
                .meta_index_start_pos = end_of_index_padding,
            };    

            write_to(ofs_sorted_index, footer, false);
 
            if(!ofs_sorted_index.good())
                return hedgehog::error("Failed to write sorted index file: " + path.string());

            std::cout << "Sorted index saved to: " << path.string() << std::endl;
        }

        OUTCOME_TRY(auto fd, fs::file_descriptor::from_path(path, std::nullopt));

        auto ss = sorted_index(std::move(fd), std::move(sorted_keys), std::move(meta_index), footer);

        return ss;
    }

    hedgehog::expected<sorted_index> index_ops::load_sorted_index(const std::filesystem::path& path, bool load_index)
    {
        if(!std::filesystem::exists(path))
            return hedgehog::error("Sorted index file does not exist: " + path.string());
        
        auto fd_res = fs::file_descriptor::from_path(path, std::nullopt);

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

        std::vector<meta_index_entry> meta_index(footer.meta_index_pages);

        ifs.seekg(footer.meta_index_start_pos, std::ios::beg);

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


}