#include <cstdint>
#include <cstring>
#include <unistd.h>
#include <utility>

#include "db/block.h"
#include "sst.h"
#include "types.h"
#include "utils.h"
#include "xxh64.hpp"

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

        page_aligned_buffer<uint8_t> footer_buf(footer_page_size);
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
            page_aligned_buffer<uint8_t> raw_meta(meta_size);
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
            page_aligned_buffer<uint8_t> qf_header_buf(PAGE_SIZE_IN_BYTES);
            res = pread(fd.fd(), qf_header_buf.data(), PAGE_SIZE_IN_BYTES, footer.qf_offset);
            if(res < 0 || static_cast<size_t>(res) != PAGE_SIZE_IN_BYTES)
                return hedge::error("sst::load: failed to read QF header from " + path.string());

            size_t data_size = footer.qf_size - PAGE_SIZE_IN_BYTES;
            size_t data_read_size = hedge::ceil_page_align(data_size);
            page_aligned_buffer<uint8_t> qf_data_buf(data_read_size);
            res = pread(fd.fd(), qf_data_buf.data(), data_read_size, footer.qf_offset + PAGE_SIZE_IN_BYTES);
            if(res < 0 || static_cast<size_t>(res) != data_read_size)
                return hedge::error("sst::load: failed to read QF data from " + path.string());

            auto maybe_qf = quotient_filter::load(qf_header_buf.data(), qf_data_buf.data(), data_size);
            if(!maybe_qf)
                return hedge::error("sst::load: failed to load quotient filter: " + maybe_qf.error().to_string());

            qf = std::move(maybe_qf.value());
        }

        // Build super index if meta-index is large enough
        std::optional<page_aligned_buffer<key_t>> super_index;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = PAGE_SIZE_IN_BYTES / sizeof(key_t);
        constexpr size_t SUPER_INDEX_THRESHOLD = 16;

        if(meta_index.size() > KEYS_PER_META_INDEX_PAGE * SUPER_INDEX_THRESHOLD)
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

        return sst(std::move(fd), std::move(meta_index), footer, std::move(super_index), std::move(qf));
    }

    async::task<expected<value_t>> sst::lookup_async(const key_t& key, const std::shared_ptr<sharded_page_cache>& cache, std::optional<uint64_t> key_hash) const
    {
        if(this->_qf.has_value() && key_hash.has_value() && !this->_qf->may_contain(key_hash.value()))
        {
            prof::get<"qf_false_positives">().add(0);
            co_return hedge::error("", errc::KEY_NOT_FOUND);
        }

        auto maybe_page_id = this->_find_page_id(key);
        if(!maybe_page_id)
            co_return hedge::error("", errc::KEY_NOT_FOUND);

        auto page_id = maybe_page_id.value();

        auto page_start_offset = this->_footer.index_offset + (page_id * PAGE_SIZE_IN_BYTES);

        // start_counter(fd);
        std::optional<page_cache::read_page_guard> opt_page_guard;
        std::optional<page_cache::write_page_guard> opt_write_guard;

        auto page_tag = to_page_tag(this->id(), page_start_offset);

        std::unique_ptr<uint8_t, decltype(&std::free)> data(nullptr, std::free); // might be needed for holding a temporary memory allocation
        uint8_t* page_ptr = nullptr;
        bool should_read_from_fs = (cache == nullptr);

        if(!should_read_from_fs)
        {
            prof::get<"lookup">().start();
            auto maybe_page_guard = cache->try_lookup(page_tag);

            if(maybe_page_guard.has_value())
            {
                if(!maybe_page_guard->ready())
                {
                    should_read_from_fs = true;
                }
                else
                {
                    opt_page_guard = std::move(co_await maybe_page_guard.value());
                    page_ptr = opt_page_guard->data + opt_page_guard->offset;
                    prof::get<"cache_hits">().add(1);
                    prof::get<"lookup">().stop(false);
                }

                // std::cout << "cache hit for fd " << this->fd() << " and file " << this->path() << " page offset " << page_start_offset << "\n";

                // print meta index
                // size_t count = 0;
                // for(const auto& entry : this->_meta_index)
                // std::cout << "Meta index entry key: " << count++ << " " << entry.key << "\n";
            }
            else
            {
                // should_read_from_fs = true;
                prof::get<"lookup">().stop(true);
            }
        }

        if(!should_read_from_fs && !opt_page_guard.has_value())
        {
            should_read_from_fs = true;
            prof::get<"cache_hits">().add(0);

            auto maybe_write_slot = cache->try_get_write_slot(page_tag);
            if(maybe_write_slot.has_value())
            {
                opt_write_guard = std::move(maybe_write_slot);
                page_ptr = opt_write_guard->data + opt_write_guard->idx;
            }
        }

        if(should_read_from_fs)
        {
            if(!opt_page_guard.has_value() && !opt_write_guard.has_value())
            {
                auto* page_mem_ptr = static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES));
                if(page_mem_ptr == nullptr)
                {
                    auto err_msg = std::format(
                        "Failed to allocate memory for loading page at offset {} from file {}",
                        page_start_offset,
                        this->path().string());
                    co_return hedge::error(err_msg);
                }

                data = std::unique_ptr<uint8_t, decltype(&std::free)>(page_mem_ptr, std::free);
                page_ptr = page_mem_ptr;
            }

            assert(page_ptr != nullptr);
            auto status = co_await this->_load_page_async(page_start_offset, page_ptr);
            if(!status)
                co_return status.error();
        }

        prof::get<"find_in_page">().start();
        assert(page_id < this->_footer.meta_index_offset * PAGE_SIZE_IN_BYTES);
        hedge::expected<value_t> res = sst::_find_in_page(key, page_ptr);
        prof::get<"find_in_page">().stop(should_read_from_fs);

        if(this->_qf.has_value() && !res.has_value())
            prof::get<"qf_false_positives">().add(1);

        co_return res;
    }

    void sst::stats() const
    {
        // TODO
    }

    hedge::expected<value_t> sst::_find_in_page(const key_t& key, const uint8_t* page)
    {
        block_decoder reader(page);

        auto value = reader.find(key);

        if(value.empty())
            return hedge::error("", errc::KEY_NOT_FOUND);

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
            // else
            // {
            //     const key_t* base = meta_index_range_begin;

            //     // Fully unrolled 8-step branchless lower_bound for exactly 128 elements
            //     base += (base[63] < key) ? 64 : 0;
            //     base += (base[31] < key) ? 32 : 0;
            //     base += (base[15] < key) ? 16 : 0;
            //     base += (base[7] < key) ? 8 : 0;
            //     base += (base[3] < key) ? 4 : 0;
            //     base += (base[1] < key) ? 2 : 0;
            //     base += (base[0] < key) ? 1 : 0;

            //     // Final boundary checks
            //     if(base == meta_index_range_end || *base < key)
            //         return std::nullopt;

            //     return std::distance(&(*this->_meta_index.begin()), base);
            // }

            // const auto* prefetch_ptr = reinterpret_cast<const uint8_t*>(meta_index_range_begin);
            // const auto* prefetch_end_ptr = reinterpret_cast<const uint8_t*>(meta_index_range_end);

            // // Prefetch meta index page entries
            // for(const uint8_t* ptr = prefetch_ptr; ptr < prefetch_end_ptr; ptr += 64)
            //     __builtin_prefetch(ptr, 0, 1);
        }

        // Perform the binary search on the meta-index.
        const auto* it = std::lower_bound(meta_index_range_begin, meta_index_range_end, key);

        // If lower_bound returns the end iterator, it means the key is greater than
        // the maximum key of all pages in this index file.
        if(it == this->_meta_index.end())
        {
            // Debugging output (commented out)
            // for (const auto& entry : this->_meta_index) { std::cout << "Meta index entry: " << entry.page_max_id << std::endl; }
            // std::cout << "Meta index size: " << this->_meta_index.size() << std::endl;
            // std::cout << "Key not found in meta index: " << key << std::endl;
            return std::nullopt;
        }

        // print it

        // Otherwise, the distance from the beginning to the iterator gives the page ID.
        size_t page_id = std::distance(this->_meta_index.begin(), it);

        // std::cout << "Meta index entry found for key: " << to_hex_string(key) << " with page max id: " << to_hex_string(*it) << " page id: " << page_id << std::endl;

        return page_id;
    }

    async::task<hedge::status> sst::_load_page_async(size_t offset, uint8_t* data_ptr) const
    {
        auto response = co_await async::this_thread_executor()->submit_request(
            async::read_request{
                .fd = this->fd(),
                .data = data_ptr,
                .offset = offset,
                .size = PAGE_SIZE_IN_BYTES});

        if(response.error_code != 0)
        {
            auto err_msg = std::format(
                "An error occurred while reading page at offset {} from file {}:  {}",
                offset,
                this->path().string(),
                strerror(-response.error_code));
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

        co_return hedge::ok();
    }

} // namespace hedge::db
