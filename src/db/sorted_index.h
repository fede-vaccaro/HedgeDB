#pragma once

#include <cstdint>
#include <filesystem>

#include <sys/types.h>
#include <uuid.h>

#include <error.hpp>

#include "async/io_executor.h"
#include "fs/fs.hpp"
#include "types.h"

namespace hedge::db
{
    class mem_index;
    class sorted_index;

    struct sorted_index_footer;
    struct meta_index_entry;

    struct sorted_index_footer
    {
        static constexpr uint32_t CURRENT_FOOTER_VERSION = 1;

        // textual header, useful for inspecting the binary
        char header[16] = "hedge_FOOTER";

        // versioning for future use
        uint8_t version{CURRENT_FOOTER_VERSION};

        // partition identifier, every key belonging to this sorted index is <= upper_bound
        uint64_t upper_bound{};

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

    class sorted_index : public fs::file
    {
        friend struct index_ops;

        std::vector<index_entry_t> _index;
        std::vector<meta_index_entry> _meta_index;
        sorted_index_footer _footer;

        std::unique_ptr<std::mutex> _compaction_mutex = std::make_unique<std::mutex>(); // let sorted_index to be mutable

    public:
        sorted_index(fs::file fd, std::vector<index_entry_t> index, std::vector<meta_index_entry> meta_index, sorted_index_footer footer);
        sorted_index() = default;

        sorted_index(sorted_index&& other) noexcept = default;
        sorted_index& operator=(sorted_index&& other) noexcept = default;

        sorted_index(const sorted_index&) = delete;
        sorted_index& operator=(const sorted_index&) = delete;

        [[nodiscard]] hedge::expected<std::optional<value_ptr_t>> lookup(const key_t& key) const;
        [[nodiscard]] async::task<expected<std::optional<value_ptr_t>>> lookup_async(const key_t& key, const std::shared_ptr<async::executor_context>& executor) const;
        [[nodiscard]] async::task<hedge::status> try_update_async(const index_entry_t& entry, const std::shared_ptr<async::executor_context>& executor);

        hedge::status load_index();

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
            std::cout << "  - File path: " << this->path() << "\n";
            std::cout << "  - Indexed keys: " << this->_footer.indexed_keys << "\n";
            std::cout << "  - Meta index pages: " << this->_footer.meta_index_entries << "\n";
            std::cout << "  - Meta index size: " << this->_meta_index.size() << "\n";
            std::cout << "  - Meta index capacity: " << this->_meta_index.capacity() << "\n";
            std::cout << "  - Index size: " << this->_index.size() << "\n";
            std::cout << "  - Index capacity: " << this->_index.capacity() << "\n";
        }

        [[nodiscard]] size_t get_index_id() const
        {
            auto path = this->path();
            auto filename = path.filename().extension().string();
            return std::stoull(filename.substr(1)); // remove leading dot
        }

        void clear_index();

    private:
        static std::optional<value_ptr_t> _find_in_page(const key_t& key, const index_entry_t* page_start, const index_entry_t* page_end);

        [[nodiscard]] std::optional<size_t> _find_page_id(const key_t& key) const;
        [[nodiscard]] async::task<expected<std::unique_ptr<uint8_t>>> _load_page_async(size_t offset, const std::shared_ptr<async::executor_context>& executor) const;
        [[nodiscard]] async::task<hedge::status> _update_in_page(const index_entry_t& entry, size_t page_id, const index_entry_t* start, const index_entry_t* end, const std::shared_ptr<async::executor_context>& executor);
    };

} // namespace hedge::db