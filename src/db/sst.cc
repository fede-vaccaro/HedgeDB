#include <cstdint>
#include <cstring>
#include <unistd.h>
#include <utility>

#include "db/block.h"
#include "error.hpp"
#include "io/io_requests.hpp"
#include "key.h"
#include "sst.h"
#include "tmc/task.hpp"
#include "types.h"
#include "utils.h"

#include "buffer_pool.h"

namespace hedge::db
{

    sst::sst(fs::file fd, page_aligned_buffer<key_t> meta_index, sst_footer footer, std::optional<page_aligned_buffer<key_t>> super_index, std::optional<quotient_filter> qf)
        : fs::file(std::move(fd)), _meta_index(std::move(meta_index)), _super_index(std::move(super_index)), _qf(std::move(qf)), _footer(footer)
    {
    }

    hedge::expected<sst> sst::load(const std::filesystem::path& path, bool use_odirect)
    {
        auto maybe_fd = fs::file::from_path(path, fs::file::open_mode::read_only, use_odirect);
        if(!maybe_fd)
            return hedge::error("sst::load: failed to open file: " + maybe_fd.error().to_string());

        auto fd = std::move(maybe_fd.value());

        // Read footer from the last page
        size_t footer_page_size = hedge::round_up(sizeof(sst_footer), PAGE_SIZE_IN_BYTES);
        size_t footer_offset = fd.file_size() - footer_page_size;

        page_aligned_buffer<std::byte> footer_buf(footer_page_size);
        int res = pread(fd.fd(), footer_buf.data(), footer_page_size, footer_offset);
        if(res < 0 || static_cast<size_t>(res) != footer_page_size)
            return hedge::error("sst::load: failed to read footer from " + path.string());

        sst_footer footer{};
        std::memcpy(&footer, footer_buf.data(), sizeof(footer));

        if(std::strncmp(footer.header, "hedge_FOOTER", 12) != 0)
            return hedge::error("sst::load: invalid footer magic in " + path.string());

        // Read meta-index bytes
        size_t meta_end = (footer.qf_size > 0) ? footer.qf_offset : footer.footer_offset;
        size_t meta_size = meta_end - footer.meta_index_offset;

        page_aligned_buffer<key_t> meta_index(footer.meta_index_entries);
        if(meta_size > 0)
        {
            page_aligned_buffer<std::byte> raw_meta(meta_size);
            res = pread(fd.fd(), raw_meta.data(), meta_size, footer.meta_index_offset);
            if(res < 0 || static_cast<size_t>(res) != meta_size)
                return hedge::error("sst::load: failed to read meta-index from " + path.string());

            size_t pos = 0;
            for(size_t i = 0; i < footer.meta_index_entries; ++i)
            {
                meta_index[i] = hedge::read_key_unsafe(raw_meta.data() + pos);
                pos += hedge::serialized_key_total_length(meta_index[i]);
            }
        }

        // Load quotient filter if present
        std::optional<quotient_filter> qf;
        if(footer.qf_size > 0)
        {
            page_aligned_buffer<std::byte> qf_header_buf(PAGE_SIZE_IN_BYTES);
            res = pread(fd.fd(), qf_header_buf.data(), PAGE_SIZE_IN_BYTES, footer.qf_offset);
            if(res < 0 || static_cast<size_t>(res) != PAGE_SIZE_IN_BYTES)
                return hedge::error("sst::load: failed to read QF header from " + path.string());

            size_t data_size = footer.qf_size - PAGE_SIZE_IN_BYTES;
            size_t data_read_size = hedge::ceil_page_align(data_size);
            page_aligned_buffer<std::byte> qf_data_buf(data_read_size);
            res = pread(fd.fd(), qf_data_buf.data(), data_read_size, footer.qf_offset + PAGE_SIZE_IN_BYTES);
            if(res < 0 || static_cast<size_t>(res) != data_read_size)
                return hedge::error("sst::load: failed to read QF data from " + path.string());

            auto maybe_qf = quotient_filter::load(qf_header_buf.data(), qf_data_buf.data(), data_size);
            if(!maybe_qf)
                return hedge::error("sst::load: failed to load quotient filter: " + maybe_qf.error().to_string());

            qf = std::move(maybe_qf.value());
        }

        // Build super index if meta-index is large enough
        // The super index is an auxiliary index over the meta-index that allows to quickly narrow down the search range in the meta-index for large SST files. 
        // This needs to avoid polluting the cache with meta-index entries
        // I'll be honest, I'm not sure it actually helps with performance
        std::optional<page_aligned_buffer<key_t>> super_index;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = PAGE_SIZE_IN_BYTES / sizeof(key_t);

        if(meta_index.size() > KEYS_PER_META_INDEX_PAGE * SUPER_INDEX_ENABLE_THRESHOLD)
        {
            size_t super_index_size = hedge::ceil(meta_index.size(), KEYS_PER_META_INDEX_PAGE);
            page_aligned_buffer<key_t> si(super_index_size);
            for(size_t i = 0; i < super_index_size; ++i)
            {
                auto idx = std::min(((i + 1) * KEYS_PER_META_INDEX_PAGE) - 1, meta_index.size() - 1);
                si[i] = meta_index[idx];
            }
            super_index = std::move(si);
        }

        return sst(
            std::move(fd),
            std::move(meta_index),
            footer,
            std::move(super_index),
            std::move(qf));
    }

    [[nodiscard]] bool sst::probe_filter(uint64_t key_hash) const
    {
        if(!this->_qf.has_value()) [[unlikely]]
            return false;

        return this->_qf->may_contain(key_hash);
    }

    tmc::task<hedge::status> sst::_load_page_async(size_t offset, const registered_buffer& tbuf) const
    {
        int32_t res = tbuf.idx().has_value()
                          ? co_await hedge::io::read_fixed(this->fd(), tbuf.data(), PAGE_SIZE_IN_BYTES, offset, tbuf.idx().value())
                          : co_await hedge::io::read(this->fd(), tbuf.data(), PAGE_SIZE_IN_BYTES, offset);

        if(res < 0)
        {
            auto err_msg = std::format(
                "An error occurred while reading page at offset {} from file {}:  {} (buf idx {})",
                offset,
                this->path().string(),
                strerror(-res),
                tbuf.idx().value_or(-1));
            co_return hedge::error(err_msg);
        }

        if(static_cast<size_t>(res) != PAGE_SIZE_IN_BYTES)
        {
            auto err_msg = std::format(
                "Read {} bytes instead of {} from file {} at offset {}",
                static_cast<size_t>(res),
                PAGE_SIZE_IN_BYTES,
                this->path().string(),
                offset);
            co_return hedge::error(err_msg);
        }

        co_return hedge::ok();
    }

    tmc::task<expected<value_t>> sst::lookup_async(const key_t& key, const std::shared_ptr<sharded_page_cache>& /*cache*/) const
    {
        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return hedge::error("", errc::KEY_NOT_FOUND);

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_offset + (page_id * PAGE_SIZE_IN_BYTES);
        assert(page_start_offset < this->_footer.meta_index_offset);

        thread_local read_buffer_pool bufs = read_buffer_pool(io::io_ctx::this_thread_ctx->queue_depth() * 2);

        auto guarded_buffer = registered_buffer_guard(bufs.try_get_fixed_buffer(), bufs);

        assert(guarded_buffer.buffer().data() != nullptr);
        auto status = co_await this->_load_page_async(page_start_offset, guarded_buffer.buffer());

        if(!status)
            co_return status.error();

        assert(page_id < (this->_footer.meta_index_offset * PAGE_SIZE_IN_BYTES));
        hedge::expected<value_t> res = sst::_find_in_page(key, guarded_buffer.buffer().data());

        co_return res;
    }

    void sst::stats() const
    {
        // TODO
    }

    hedge::expected<value_t> sst::_find_in_page(const key_t& key, const std::byte* page)
    {
        block_decoder reader(page);
        if(!reader.sanity_check())
            return hedge::error("checksum mismatch for key " + hedge::to_hex_string(key));

        auto value = reader.find(key);

        if(value.empty())
            return hedge::error(std::string{}, errc::KEY_NOT_FOUND);

        auto value_result = value_from_span(value);
        if(!value_result)
            return value_result.error();

        return value_result.value();
    }

    std::optional<size_t> sst::_find_page_id(const key_t& key) const
    {
        const auto* meta_index_range_begin = this->_meta_index.begin();
        const auto* meta_index_range_end = this->_meta_index.end();

        if(this->_super_index.has_value())
        {
            const auto* it = std::lower_bound(this->_super_index->begin(), this->_super_index->end(), key);
            if(it == this->_super_index->end())
                return std::nullopt;

            constexpr size_t REF_PAGE_SIZE = 4096;
            constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

            size_t meta_index_page_id = std::distance(this->_super_index->begin(), it);

            meta_index_range_begin = this->_meta_index.begin() + (meta_index_page_id * KEYS_PER_META_INDEX_PAGE);
            meta_index_range_end = meta_index_range_begin + KEYS_PER_META_INDEX_PAGE;

            if(bool is_last_page = meta_index_page_id == this->_super_index->size() - 1; is_last_page)
            {
                size_t last_page_size = this->_meta_index.size() % KEYS_PER_META_INDEX_PAGE;
                if(last_page_size != 0)
                    meta_index_range_end = meta_index_range_begin + last_page_size;
            }

            const auto* prefetch_ptr = reinterpret_cast<const std::byte*>(meta_index_range_begin);
            const auto* prefetch_end_ptr = reinterpret_cast<const std::byte*>(meta_index_range_end);

            // // Prefetch meta index page entries
            for(const std::byte* ptr = prefetch_ptr; ptr < prefetch_end_ptr; ptr += 64)
                __builtin_prefetch(ptr);
        }

        // Perform the binary search on the meta-index.
        const auto* it = std::lower_bound(meta_index_range_begin, meta_index_range_end, key);

        // If lower_bound returns the end iterator, it means the key is greater than
        // the maximum key of all pages in this index file.
        if(it == this->_meta_index.end())
            return std::nullopt;

        // Otherwise, the distance from the beginning to the iterator gives the page ID.
        size_t page_id = std::distance(this->_meta_index.begin(), it);

        return page_id;
    }

    std::optional<page_range> sst::find_range(std::optional<key_t> lower, std::optional<key_t> upper) const
    {
        size_t first = 0;
        if(lower)
        {
            auto id = _find_page_id(*lower);
            if(!id)
                return std::nullopt;
            first = *id;
        }

        size_t last = this->_meta_index.size() - 1;
        if(upper)
        {
            auto id = _find_page_id(*upper);
            if(id)
                last = *id;
        }

        return page_range{.first_page_id = first, .last_page_id = last};
    }

} // namespace hedge::db
