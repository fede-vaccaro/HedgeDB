#pragma once

#include "fs.hpp"
#include <cstdint>
#include <filesystem>

#include <sys/types.h>
#include <uuid.h>

#include <error.hpp>
namespace hedgehog::db
{
    using key_t = uuids::uuid;

    struct value_ptr_t
    {
        uint64_t offset{};
        uint32_t size{};
        uint32_t table_id{};
    };

    struct index_key_t
    {
        key_t key{};
        value_ptr_t value_ptr{};

        bool operator<(const index_key_t& other) const
        {
            return key < other.key;
        }
    };

    inline std::filesystem::path with_extension(const std::filesystem::path& path, std::string_view ext)
    {
        return path.string() + ext.data();
    }

    inline uint16_t extract_prefix(const uuids::uuid& key)
    {
        const auto& uuids_as_std_array = reinterpret_cast<const std::array<uint8_t, 16>&>(key);

        size_t prefix = 0;

        // little endian!
        prefix |= static_cast<uint16_t>(uuids_as_std_array[1]);
        prefix |= static_cast<uint16_t>(uuids_as_std_array[0]) << 8;

        return prefix;
    }

    inline std::pair<std::string, std::string> format_prefix(uint16_t prefix)
    {
        // keep in mind this is little-endian!
        auto as_array = reinterpret_cast<std::array<uint8_t, 2>&>(prefix);
        auto path = std::format("{:02x}", as_array[1]);
        auto filename = std::format("{:02x}{:02x}", as_array[1], as_array[0]); 
        return std::pair{path, filename};
    }

    std::vector<std::pair<size_t, std::filesystem::path>> get_prefixes(const std::filesystem::path& base_path, size_t num_space_partitions);

    class mem_index;
    class sorted_index;
    struct sorted_index_footer;
    struct meta_index_entry;

    constexpr size_t INDEX_PAGE_SIZE_BYTES = 4096; // 4KB
    constexpr size_t INDEX_PAGE_NUM_ENTRIES = INDEX_PAGE_SIZE_BYTES / sizeof(index_key_t);

    struct index_ops
    {
        static hedgehog::expected<sorted_index> load_sorted_index(const std::filesystem::path& path, bool load_index = false);
        static hedgehog::expected<sorted_index> save_as_sorted_index(const std::filesystem::path& path, std::vector<index_key_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent = false);
        static hedgehog::expected<std::vector<sorted_index>> merge_and_flush(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent);
    };

    class mem_index
    {
        using index_t = std::unordered_map<key_t, value_ptr_t>;
        
        friend struct index_ops;

        index_t _index;

    public:
        mem_index() = default;
        mem_index(mem_index&& other) noexcept = default;
        mem_index& operator=(mem_index&& other) noexcept = default;
        mem_index(const mem_index&) = delete;
        mem_index& operator=(const mem_index&) = delete;

        bool add(const key_t& key, const value_ptr_t& value){
            auto [it, inserted] = _index.emplace(key, value);

            if(!inserted)
                it->second = value; // Update existing entry

            return true;
        }

        bool erase(const key_t& key){
            return _index.erase(key) > 0;
        }

        std::optional<value_ptr_t> get(const key_t& key) const {
            auto it = _index.find(key);
            if(it != _index.end())
                return it->second;
            return std::nullopt; // Key not found
        }

        size_t size() const
        {
            return _index.size();
        };     
    };

    struct sorted_index_footer
    {
        inline static constexpr uint32_t CURRENT_FOOTER_VERSION = 1;

        uint8_t version{CURRENT_FOOTER_VERSION};
        
        size_t upper_bound{}; // every key in the sorted_index_footer is < upper_bound

        uuids::uuid min_key{};
        uuids::uuid max_key{};

        uint64_t indexed_keys{}; 
        uint64_t meta_index_pages{};

        uint64_t index_end_pos{};
        uint64_t meta_index_start_pos{};

        static sorted_index_footer generate(const std::vector<index_key_t>& index, const std::vector<meta_index_entry>& meta_index);
    };

    struct meta_index_entry
    {
        uuids::uuid page_max_id{};
    };

    class sorted_index
    {
        friend struct index_ops;

        fs::file_descriptor _fd;
        std::vector<index_key_t> _index;
        std::vector<meta_index_entry> _meta_index;
        sorted_index_footer _footer;

    public:
        sorted_index(fs::file_descriptor fd, std::vector<index_key_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer);

        sorted_index() = default;

        sorted_index(sorted_index&& other) = default;
        sorted_index& operator=(sorted_index&& other) = default;

        sorted_index(const sorted_index&) = delete;
        sorted_index& operator=(const sorted_index&) = delete;

        hedgehog::expected<std::optional<value_ptr_t>> lookup(const key_t& key);

        hedgehog::status load_index();

        hedgehog::status merge(sorted_index&& other);

        inline size_t get_upper_bound() const
        {
            return _footer.upper_bound;
        }

        inline void stats() const
        {
            std::cout << "Sorted index stats:\n";
            std::cout << "  - File path: " << _fd.path() << "\n";
            std::cout << "  - Indexed keys: " << _footer.indexed_keys << "\n";
            std::cout << "  - Meta index pages: " << _footer.meta_index_pages << "\n";
            std::cout << "  - Meta index size: " << _meta_index.size() << "\n";
            std::cout << "  - Meta index capacity: " << _meta_index.capacity() << "\n";
            std::cout << "  - Index size: " << _index.size() << "\n";
            std::cout << "  - Index capacity: " << _index.capacity() << "\n";
        }

        std::filesystem::path get_path() const
        {
            return _fd.path();
        }
        
        void clear_index();

    private:
        std::optional<size_t> _find_page_id(const key_t& key);
        std::optional<value_ptr_t> _find_in_page(const key_t& key, const index_key_t* page_start, const index_key_t* page_end);
        
    };

}