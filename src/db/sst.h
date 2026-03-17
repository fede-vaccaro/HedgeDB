#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include <stdexcept>
#include <sys/types.h>

#include <error.hpp>

#include "async/task.h"
#include "cache.h"
#include "fs/fs.hpp"
#include "types.h"

#include "page_aligned_buffer.h"
#include "quotient_filter.h"

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
        uint64_t qf_offset{};                    ///< Byte offset from the beginning of the file where the quotient filter data begins.
        uint64_t qf_size{};                      ///< Size in bytes of the quotient filter section (header + data).
        uint64_t footer_offset{};                ///< Byte offset from the beginning of the file where this footer structure begins.
        uint64_t epoch{};                        ///< Epoch timestamp indicating when the file was created.
    };

    enum class compaction_progress_state : int8_t
    {
        PICKABLE_FOR_COMPACTION = 0,
        IN_COMPACTION = 1,
        COMPACTED = 2,
        COMPACTION_ERROR = 3
    };

    inline std::string to_string(compaction_progress_state c)
    {
        switch(c)
        {
            case compaction_progress_state::PICKABLE_FOR_COMPACTION:
                return "PICKABLE_FOR_COMPACTION";
            case compaction_progress_state::IN_COMPACTION:
                return "IN_COMPACTION";
            case compaction_progress_state::COMPACTED:
                return "COMPACTED";
            case compaction_progress_state::COMPACTION_ERROR:
                return "COMPACTION_ERROR";
            default:
                return "UNKNOWN";
        }
    }

    class sst : public fs::file
    {
        // Allow index_ops to access private members for operations like merging/saving.
        friend struct index_ops;

        page_aligned_buffer<key_t> _meta_index;                 ///< In-memory copy of the meta-index (always loaded on construction/load).
        std::optional<page_aligned_buffer<key_t>> _super_index; ///< The super index can be built to facilitate lookup over the meta index.
        std::optional<quotient_filter> _qf;                     ///< Optional quotient filter for fast negative lookups.
        sst_footer _footer;                                     ///< In-memory copy of the file's footer (always loaded on construction/load).
        compaction_progress_state _compaction_state{compaction_progress_state::PICKABLE_FOR_COMPACTION};

    public:
        sst(fs::file fd, page_aligned_buffer<key_t> meta_index, sst_footer footer, std::optional<page_aligned_buffer<key_t>> super_index, std::optional<quotient_filter> qf = std::nullopt);

        sst() = default;

        sst(sst&& other) noexcept = default;

        sst& operator=(sst&& other) noexcept = default;

        sst(const sst&) = delete;
        sst& operator=(const sst&) = delete;

        [[nodiscard]] static hedge::expected<sst> load(const std::filesystem::path& path, bool use_odirect = false);

        void prefetch_filter_slot(uint64_t key_hash)
        {
            if(!this->_qf.has_value()) [[unlikely]]
                return;

            this->_qf->prefetch_slot(key_hash);
        }

        [[nodiscard]] bool probe_filter(uint64_t key_hash) const;

        [[nodiscard]] async::task<expected<value_t>> lookup_async(const key_t& key, const std::shared_ptr<sharded_page_cache>& cache, std::optional<uint64_t> key_hash = std::nullopt) const;

        [[nodiscard]] size_t upper_bound() const { return this->_footer.upper_bound; }

        [[nodiscard]] size_t size() const { return this->_footer.indexed_keys; }

        [[nodiscard]] size_t epoch() const { return this->_footer.epoch; }

        [[nodiscard]] const sst_footer& footer() const { return this->_footer; }

        void stats() const;

        [[nodiscard]] compaction_progress_state compaction_state() const
        {
            return this->_compaction_state;
        }

        [[nodiscard]] bool pickable_for_compaction() const
        {
            auto state = this->_compaction_state;
            return state == compaction_progress_state::PICKABLE_FOR_COMPACTION;
        }

        void set_compaction_state(compaction_progress_state new_state)
        {
            auto curr_state = this->_compaction_state;

            if(curr_state == compaction_progress_state::COMPACTION_ERROR)
                return;

            auto valid_transition = [](compaction_progress_state curr, compaction_progress_state next)
            {
                switch(curr)
                {
                    case compaction_progress_state::PICKABLE_FOR_COMPACTION:
                        return next == compaction_progress_state::IN_COMPACTION || next == compaction_progress_state::COMPACTION_ERROR;
                    case compaction_progress_state::IN_COMPACTION:
                        return next == compaction_progress_state::COMPACTED || next == compaction_progress_state::COMPACTION_ERROR;
                    case compaction_progress_state::COMPACTED:
                        return next == compaction_progress_state::COMPACTION_ERROR;
                    case compaction_progress_state::COMPACTION_ERROR:
                        return false;
                }
                __builtin_unreachable();
            };

            if(!valid_transition(curr_state, new_state))
                throw std::runtime_error("Invalid transition: " + to_string(curr_state) + " -> " + to_string(new_state));

            this->_compaction_state = new_state;
        }

    private:
        static hedge::expected<value_t> _find_in_page(const key_t& key, const uint8_t* page);

        [[nodiscard]] std::optional<size_t> _find_page_id(const key_t& key) const;

        [[nodiscard]] async::task<hedge::status> _load_page_async(size_t offset, uint8_t* data_ptr, int32_t buf_index) const;
    };

} // namespace hedge::db
