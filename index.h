#pragma once

#include "fs.hpp"
#include <cstdint>
#include <filesystem>

#include <sys/types.h>
#include <uuid.h>

#include <error.hpp>

#include "common.h"
#include "io_executor.h"

namespace hedgehog::db
{
    class mem_index;
    class sorted_index;

    struct sorted_index_footer;
    struct meta_index_entry;

    struct index_ops
    {
        static std::vector<index_key_t> merge_memtables_in_mem(std::vector<mem_index>&& indices);
        static hedgehog::expected<sorted_index> load_sorted_index(const std::filesystem::path& path, bool load_index = false);
        static hedgehog::expected<sorted_index> save_as_sorted_index(const std::filesystem::path& path, std::vector<index_key_t>&& sorted_keys, size_t upper_bound, bool merge_with_existent = false);
        static hedgehog::expected<std::vector<sorted_index>> merge_and_flush(const std::filesystem::path& base_path, std::vector<mem_index>&& indices, size_t num_partition_exponent, size_t flush_iteration);

        struct merge_config
        {
            size_t read_ahead_size{};
            size_t new_index_id{};
            std::filesystem::path base_path{};
        };

        static async::task<hedgehog::expected<sorted_index>> two_way_merge_async(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);
        static hedgehog::expected<sorted_index> two_way_merge(const merge_config& config, const sorted_index& left, const sorted_index& right, const std::shared_ptr<async::executor_context>& executor);
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

        void clear()
        {
            this->_index.clear();
        }

        void reserve(size_t size)
        {
            this->_index.reserve(size);
        }

        bool put(const key_t& key, const value_ptr_t& value)
        {
            auto [it, inserted] = this->_index.emplace(key, value);

            if(!inserted)
                it->second = value;

            return true;
        }

        std::optional<value_ptr_t> get(const key_t& key) const
        {
            auto it = _index.find(key);
            if(it != _index.end())
                return it->second;
            return std::nullopt; // Key not found
        }

        size_t size() const
        {
            return this->_index.size();
        };
    };

    struct sorted_index_footer
    {
        static constexpr uint32_t CURRENT_FOOTER_VERSION = 1;

        // textual header, useful for inspecting the binary
        char header[16] = "HEDGEHOG_FOOTER";

        // versioning for future use
        uint8_t version{CURRENT_FOOTER_VERSION};

        // partition identifier, every key belonging to this sorted index is <= upper_bound
        uint64_t upper_bound{};

        // min/max keys
        uuids::uuid min_key{};
        uuids::uuid max_key{};

        // sizes section
        uint64_t indexed_keys{};
        uint64_t meta_index_entries{};

        // offsets setion
        uint64_t index_start_offset{};
        uint64_t index_end_offset{};
        uint64_t meta_index_start_offset{};
        uint64_t meta_index_end_offset{};
        uint64_t footer_start_offset{};
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

        std::unique_ptr<std::mutex> _compaction_mutex = std::make_unique<std::mutex>(); // let sorted_index to be mutable

    public:
        sorted_index(fs::file_descriptor fd, std::vector<index_key_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer);
        sorted_index() = default;

        sorted_index(sorted_index&& other) noexcept = default;
        sorted_index& operator=(sorted_index&& other) noexcept = default;

        sorted_index(const sorted_index&) = delete;
        sorted_index& operator=(const sorted_index&) = delete;

        [[nodiscard]] hedgehog::expected<std::optional<value_ptr_t>> lookup(const key_t& key) const;
        [[nodiscard]] async::task<expected<std::optional<value_ptr_t>>> lookup_async(const key_t& key, const std::shared_ptr<async::executor_context>& executor) const;
        [[nodiscard]] async::task<hedgehog::status> try_update_async(const index_key_t& entry, const std::shared_ptr<async::executor_context>& executor);

        hedgehog::status load_index();

        [[nodiscard]] size_t upper_bound() const
        {
            return this->_footer.upper_bound;
        }

        [[nodiscard]] size_t size() const
        {
            return this->_footer.indexed_keys;
        }

        void stats() const
        {
            std::cout << "Sorted index stats:\n";
            std::cout << "  - File path: " << this->_fd.path() << "\n";
            std::cout << "  - Indexed keys: " << this->_footer.indexed_keys << "\n";
            std::cout << "  - Meta index pages: " << this->_footer.meta_index_entries << "\n";
            std::cout << "  - Meta index size: " << this->_meta_index.size() << "\n";
            std::cout << "  - Meta index capacity: " << this->_meta_index.capacity() << "\n";
            std::cout << "  - Index size: " << this->_index.size() << "\n";
            std::cout << "  - Index capacity: " << this->_index.capacity() << "\n";
        }

        [[nodiscard]] std::filesystem::path get_path() const
        {
            return this->_fd.path();
        }

        [[nodiscard]] size_t get_index_id() const
        {
            auto path = this->_fd.path();
            auto filename = path.filename().extension().string();
            return std::stoull(filename.substr(1)); // remove leading dot
        }

        void clear_index();

    private:
        static std::optional<value_ptr_t> _find_in_page(const key_t& key, const index_key_t* page_start, const index_key_t* page_end);

        [[nodiscard]] std::optional<size_t> _find_page_id(const key_t& key) const;
        [[nodiscard]] async::task<expected<std::unique_ptr<uint8_t>>> _load_page_async(size_t offset, const std::shared_ptr<async::executor_context>& executor) const;
        [[nodiscard]] async::task<hedgehog::status> _update_in_page(const index_key_t& entry, size_t page_id, const index_key_t* start, const index_key_t* end, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedgehog::db