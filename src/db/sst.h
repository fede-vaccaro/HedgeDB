#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <sys/types.h>

#include <error.hpp>

#include "async/io_executor.h"
#include "async/task.h"
#include "cache.h"
#include "fs/fs.hpp"
#include "tsl/robin_map.h"
#include "types.h"
#include "utils.h"

#include "page_aligned_buffer.h"
#include "perf_counter.h"

namespace hedge::db
{
    // TODO transform to header
    struct sst_footer
    {
        static constexpr uint32_t CURRENT_FOOTER_VERSION = 1; ///< Current version of the file format.

        char header[16] = "hedge_FOOTER";        ///< Magic string ("hedge_FOOTER") to identify the footer block.
        uint8_t version{CURRENT_FOOTER_VERSION}; ///< File format version number.
        uint64_t upper_bound{};                  ///< The upper bound (inclusive) of the key partition this index belongs to. All keys in this file are <= this value.
        uint64_t indexed_keys{};                 ///< Total number of key-value pointer entries in the main index data section.
        uint64_t meta_index_entries{};           ///< Total number of entries in the meta-index section.
        uint64_t index_offset{};                 ///< Byte offset from the beginning of the file where the main index data begins.
        uint64_t meta_index_offset{};            ///< Byte offset from the beginning of the file where the meta-index data begins.
        uint64_t footer_offset{};                ///< Byte offset from the beginning of the file where this footer structure begins.
        uint64_t epoch{};                        ///< Epoch timestamp indicating when the file was created.
    };

    class sst : public fs::file
    {
        // Allow index_ops to access private members for operations like merging/saving.
        friend struct index_ops;

        page_aligned_buffer<key_t> _meta_index;                 ///< In-memory copy of the meta-index (always loaded on construction/load).
        std::optional<page_aligned_buffer<key_t>> _super_index; ///< The super index can be built to facilitate lookup over the meta index.
        sst_footer _footer;                                     ///< In-memory copy of the file's footer (always loaded on construction/load).

    public:
        sst(fs::file fd, page_aligned_buffer<key_t> meta_index, sst_footer footer, std::optional<page_aligned_buffer<key_t>> super_index);

        sst() = default;

        sst(sst&& other) noexcept = default;

        sst& operator=(sst&& other) noexcept = default;

        sst(const sst&) = delete;
        sst& operator=(const sst&) = delete;

        [[nodiscard]] async::task<expected<std::optional<value_ptr_t>>> lookup_async(const key_t& key, const std::shared_ptr<sharded_page_cache>& cache) const;

        [[nodiscard]] size_t upper_bound() const { return this->_footer.upper_bound; }

        [[nodiscard]] size_t size() const { return this->_footer.indexed_keys; }

        [[nodiscard]] size_t epoch() const { return this->_footer.epoch; }

        [[nodiscard]] const sst_footer& footer() const { return this->_footer; }

        void stats() const;

    private:
        static std::optional<value_ptr_t> _find_in_page(const key_t& key, const uint8_t* page);

        [[nodiscard]] std::optional<size_t> _find_page_id(const key_t& key) const;

        [[nodiscard]] async::task<hedge::status> _load_page_async(size_t offset, uint8_t* data_ptr) const;
    };

} // namespace hedge::db
