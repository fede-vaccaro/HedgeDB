#include <algorithm>
#include <cstdint>
#include <error.hpp>
#include <fcntl.h>
#include <mutex>
#include <size_literals.h>
#include <sys/mman.h>

#include "db/block.h"
#include "db/merge/write_buffer.h"
#include "db/quotient_filter.h"
#include "db/skiplist.h"
#include "generator.h"
#include "index_ops.h"
#include "io/io_requests.hpp"
#include "page_aligned_buffer.h"
#include "sst.h"
#include "tmc/ex_cpu.hpp"
#include "tmc/latch.hpp"
#include "tmc/sync.hpp"
#include "types.h"
#include "utils.h"
#include "xxh64.hpp"

namespace hedge::db
{

    template <typename T, size_t PAGE_SIZE>
    page_aligned_buffer<key_t> index_ops::create_super_index(const page_aligned_buffer<key_t>& meta_index)
    {
        // Calculate how many meta-index entries are needed (one per page).
        constexpr size_t ENTRIES_PER_PAGE = PAGE_SIZE / sizeof(T);

        auto super_index_size = hedge::ceil(meta_index.size(), ENTRIES_PER_PAGE);
        auto super_index = page_aligned_buffer<key_t>(super_index_size);

        // Iterate through each conceptual page.
        for(size_t i = 0; i < super_index_size; ++i)
        {
            auto idx = std::min(((i + 1) * ENTRIES_PER_PAGE) - 1, meta_index.size() - 1);
            super_index[i] = meta_index[idx];
        }

        return super_index;
    }

    template page_aligned_buffer<key_t> index_ops::create_super_index<key_t, PAGE_SIZE_IN_BYTES>(const page_aligned_buffer<key_t>&);

    static constexpr size_t QF_BITS_PER_KEY = 10;

    static quotient_filter create_qf_for_key_count(size_t num_keys)
    {
        // Target ~75% load factor: slots = num_keys / 0.75
        size_t slots = (num_keys * 4 + 2) / 3; // ceil(num_keys / 0.75)
        uint32_t q = 0;
        size_t s = 1;
        while(s < slots)
        {
            s <<= 1;
            q++;
        }
        uint32_t r = QF_BITS_PER_KEY; // remainder bits = 7
        auto maybe_qf = quotient_filter::make(q, r);
        if(!maybe_qf)
            throw std::runtime_error("Failed to create quotient filter: " + maybe_qf.error().to_string());

        return std::move(maybe_qf.value());
    }

    static async::generator<index_ops::partition_range> _partition_ranges_generator(
        skiplist_t::Accessor::const_iterator begin,
        skiplist_t::Accessor::const_iterator end,
        size_t num_partition_exponent)
    {
        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);

        if(begin == end)
            co_return;

        size_t current_partition_id = hedge::find_partition_prefix_for_key(begin->_key, partition_key_prefix_range);
        auto range_begin = begin;
        size_t count = 0;
        size_t sum_kv_len = 0;

        for(auto it = begin; it != end; ++it)
        {
            size_t part_id = hedge::find_partition_prefix_for_key(it->_key, partition_key_prefix_range);

            if(part_id != current_partition_id)
            {
                co_yield index_ops::partition_range{
                    .partition_id = current_partition_id,
                    .begin = range_begin,
                    .end = it,
                    .count = count,
                    .sum_key_value_lengths = sum_kv_len,
                };
                current_partition_id = part_id;
                range_begin = it;
                count = 0;
                sum_kv_len = 0;
            }

            count++;
            sum_kv_len += it->_key.size() + it->_value.size();
        }

        // Last partition
        co_yield index_ops::partition_range{
            .partition_id = current_partition_id,
            .begin = range_begin,
            .end = end,
            .count = count,
            .sum_key_value_lengths = sum_kv_len,
        };

        co_return;
    }

    // Writes a quotient filter (header page + data pages) to fd at the given offset.
    // Returns the total bytes written.
    static tmc::task<hedge::expected<size_t>> _write_qf_async(int fd, const quotient_filter& qf, size_t offset)
    {
        size_t written = 0;

        auto header_span = qf.header_as_byte_span();
        page_aligned_buffer<std::byte> header_buf(PAGE_SIZE_IN_BYTES);
        std::memcpy(header_buf.data(), header_span.data(), header_span.size());

        int32_t res = co_await hedge::io::write(fd, header_buf.data(), PAGE_SIZE_IN_BYTES, offset);
        if(res < 0)
            co_return hedge::error("Failed to write QF header: " + std::string(strerror(-res)));
        written += static_cast<size_t>(res);

        auto data_span = qf.data_as_byte_span();
        size_t data_size = hedge::ceil_page_align(data_span.size());
        page_aligned_buffer<std::byte> data_buf(data_size);
        std::memcpy(data_buf.data(), data_span.data(), data_span.size());

        res = co_await hedge::io::write(fd, data_buf.data(), data_size, offset + written);
        if(res < 0)
            co_return hedge::error("Failed to write QF data: " + std::string(strerror(-res)));
        written += static_cast<size_t>(res);

        co_return written;
    }

    struct sst_sizes
    {
        size_t index_size;
        size_t meta_index_size;
        size_t qf_size;
        size_t footer_size;

        [[nodiscard]] size_t total_size() const
        {
            return index_size + meta_index_size + qf_size + footer_size;
        }
    };

    sst_sizes estimate_sst_size(size_t entry_count, size_t average_key_value_length, quotient_filter* qf = nullptr)
    {
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_value_length);
        size_t estimated_pages = hedge::ceil(entry_count, key_values_per_page_estimate);

        // Main index size estimate (already page-aligned)
        size_t index_size = estimated_pages * PAGE_SIZE_IN_BYTES;

        // Meta-index size estimate (page-aligned)
        size_t meta_index_size = hedge::round_up(estimated_pages * sizeof(key_t), PAGE_SIZE_IN_BYTES);

        // Quotient filter size estimate (header page + data pages)
        size_t qf_size = 0;
        if(qf != nullptr)
        {
            qf_size = hedge::ceil_page_align(qf->header_as_byte_span().size()) +
                      hedge::ceil_page_align(qf->data_as_byte_span().size());
        }

        // Footer page
        size_t footer_size = PAGE_SIZE_IN_BYTES;

        return sst_sizes{
            .index_size = index_size,
            .meta_index_size = meta_index_size,
            .qf_size = qf_size,
            .footer_size = footer_size};
    }

    struct _write_index_infos
    {
        size_t bytes_written;
        size_t indexed_kv;
    };

    tmc::task<expected<_write_index_infos>> _write_index(
        int32_t fd,
        uint64_t file_id,
        skiplist_t::Accessor::const_iterator skiplist_begin,
        skiplist_t::Accessor::const_iterator skiplist_end,
        std::optional<quotient_filter>& qf,
        page_aligned_buffer<key_t>& meta_index,
        page_aligned_buffer<std::byte>& encoded_meta_index)
    {
        constexpr size_t WRITE_BUF_SIZE = 1 * MiB;

        // Write index entries using a merge_write_buffer to block encoding.
        merge_write_buffer write_buffer(WRITE_BUF_SIZE);
        size_t bytes_written = 0;
        auto flush = [&]() -> tmc::task<hedge::status>
        {
            auto write_response = co_await write_buffer.flush(fd, file_id, bytes_written, nullptr);
            if(write_response.error_code)
                co_return hedge::error("Failed to write index data: " + std::string(strerror(-write_response.error_code)));
            bytes_written += write_response.bytes_written;
            co_return hedge::ok();
        };

        // Iterate through entries and write them using the merge_write_buffer, which handles buffering and block encoding.
        std::optional<key_t> prev_key;
        for(auto it = skiplist_begin; it != skiplist_end; ++it)
        {
            if(prev_key && it->_key == *prev_key)
                continue; // older MVCC version, skip

            prev_key = it->_key;

            if(qf.has_value()) [[likely]]
                qf->insert(xxh64::hash((const char*)it->_key.data(), it->_key.size(), sst::QF_SEED));

            auto ok = write_buffer.write_item(it->_key, it->_value, meta_index, encoded_meta_index);
            if(ok)
                continue;

            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result.error();

            auto retry = write_buffer.write_item(it->_key, it->_value, meta_index, encoded_meta_index);
            if(!retry)
                co_return hedge::error("Could not write item after flush");
        }

        // Flush any remaining data in the buffer
        if(!write_buffer.empty())
        {
            const auto& last_key = write_buffer.encoder().last_pushed_key();
            meta_index.emplace_back(last_key);
            index_ops::append_meta_index_key(encoded_meta_index, last_key);

            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result.error();
        }

        co_return _write_index_infos{.bytes_written = bytes_written, .indexed_kv = write_buffer.indexed_kv()};
    }

    static tmc::task<hedge::expected<sst>> _export_as_sst_async(
        std::filesystem::path path,
        skiplist_t::Accessor::const_iterator skiplist_begin,
        skiplist_t::Accessor::const_iterator skiplist_end,
        size_t entry_count,
        size_t average_key_value_length,
        size_t upper_bound,
        size_t epoch,
        [[maybe_unused]] std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect,
        bool fdatasync_output)
    {
        // Build QF (TODO: make optional)
        std::optional<quotient_filter> qf = create_qf_for_key_count(entry_count);

        sst_sizes sst_estimated_size = estimate_sst_size(entry_count, average_key_value_length, &qf.value()); // footer page

        // Open file
        auto maybe_file = co_await fs::file::from_path_async(
            path,
            fs::file::open_mode::read_write_new,
            use_odirect,
            sst_estimated_size.total_size());

        if(!maybe_file)
            co_return hedge::error("Failed to create sorted index file: " + maybe_file.error().to_string());

        auto& file = maybe_file.value();
        auto fd = file.fd();
        auto file_id = file.id();

        auto meta_index = page_aligned_buffer<key_t>(0, sst_estimated_size.meta_index_size / sizeof(key_t));
        auto encoded_meta_index = page_aligned_buffer<std::byte>(0, sst_estimated_size.meta_index_size);

        // Write index entries using a merge_write_buffer to block encoding.
        auto maybe_index_infos = co_await _write_index(fd, file_id, skiplist_begin, skiplist_end, qf, meta_index, encoded_meta_index);
        if(!maybe_index_infos)
            co_return maybe_index_infos.error();
        auto [bytes_written, indexed_kv] = maybe_index_infos.value();

        // Write meta-index
        size_t encoded_meta_write_size_bytes = hedge::ceil_page_align(encoded_meta_index.size());
        int32_t meta_write_res = co_await hedge::io::write(fd, encoded_meta_index.data(), encoded_meta_write_size_bytes, bytes_written);
        if(meta_write_res < 0)
            co_return hedge::error("Failed to write meta-index: " + std::string(strerror(-meta_write_res)));
        size_t meta_index_offset = bytes_written;
        bytes_written += static_cast<size_t>(meta_write_res);
        encoded_meta_index.free();

        // Write quotient filter
        index_ops::sst_footer_builder fbuilder{};
        if(qf.has_value()) [[likely]]
        {
            fbuilder.qf_offset = bytes_written;
            auto qf_res = co_await _write_qf_async(fd, *qf, bytes_written);
            if(!qf_res)
                co_return qf_res.error();
            bytes_written += qf_res.value();
            fbuilder.qf_size = qf_res.value();
        }

        // Prepare footer
        fbuilder.indexed_kv = indexed_kv;
        fbuilder.upper_bound = upper_bound;
        fbuilder.index_offset = 0;
        fbuilder.meta_index_offset = meta_index_offset;
        fbuilder.meta_index_entries = meta_index.size();
        fbuilder.epoch = epoch;
        fbuilder.footer_offset = bytes_written;

        auto maybe_footer = fbuilder.build();
        if(!maybe_footer)
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        // Copy footer to page-aligned buffer
        const sst_footer& footer = maybe_footer.value();
        page_aligned_buffer<sst_footer> footer_buf(1);
        footer_buf[0] = footer;

        // Write footer
        int32_t footer_res = co_await hedge::io::write(fd, reinterpret_cast<std::byte*>(footer_buf.data()), PAGE_SIZE_IN_BYTES, bytes_written);
        if(footer_res < 0)
            co_return hedge::error("Failed to write footer: " + std::string(strerror(-footer_res)));
        bytes_written += static_cast<size_t>(footer_res);

        // Truncate file to actual written size
        int32_t truncate_res = co_await hedge::io::ftruncate(fd, bytes_written);
        if(truncate_res < 0)
            co_return hedge::error("Failed to truncate file: " + std::string(strerror(-truncate_res)));

        if(!use_odirect)
            posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);

        if(fdatasync_output) [[likely]]
        {
            int32_t sync_res = co_await hedge::io::fdatasync(fd);
            if(sync_res < 0)
                co_return hedge::error("Failed to fdatasync file: " + std::string(strerror(-sync_res)));
        }

        co_return sst(std::move(file), std::move(meta_index), footer, std::nullopt, std::move(qf));
    }

    static tmc::task<void> _flush_partition_task(
        std::filesystem::path path,
        skiplist_t::Accessor::const_iterator range_begin,
        skiplist_t::Accessor::const_iterator range_end,
        size_t entry_count,
        size_t sum_kv_len,
        size_t partition_id,
        size_t flush_iteration,
        std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect,
        std::mutex* results_mutex,
        std::vector<sst>* results,
        std::vector<hedge::error>* errors,
        tmc::latch* latch,
        bool fdatasync_output)
    {
        size_t avg_kv_len = hedge::ceil(sum_kv_len, entry_count);

        auto maybe_sst = co_await _export_as_sst_async(
            std::move(path),
            range_begin,
            range_end,
            entry_count,
            avg_kv_len,
            partition_id,
            flush_iteration,
            std::move(cache),
            use_odirect,
            fdatasync_output);

        {
            std::lock_guard lk(*results_mutex);
            if(maybe_sst)
                results->push_back(std::move(maybe_sst.value()));
            else
                errors->emplace_back(maybe_sst.error());
        }

        latch->count_down();
    }

    tmc::task<hedge::expected<std::vector<sst>>> index_ops::flush_memtable(
        std::filesystem::path base_path,
        skiplist_t::Accessor::const_iterator begin,
        skiplist_t::Accessor::const_iterator end,
        size_t num_partition_exponent,
        size_t flush_iteration,
        std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect,
        tmc::ex_cpu& flush_executor,
        bool fdatasync_ssts)
    {
        if(num_partition_exponent > 16)
            co_return hedge::error("Number of partitions exponent must be less than or equal to 16");

        auto ranges = _partition_ranges_generator(begin, end, num_partition_exponent);

        size_t max_partitions = size_t(1) << num_partition_exponent;
        tmc::latch latch(max_partitions);

        std::mutex results_mutex;
        std::vector<sst> results;
        std::vector<hedge::error> errors;

        size_t thread_hint = 0;
        size_t thread_count = flush_executor.thread_count();
        size_t range_count = 0;
        for(const index_ops::partition_range& range : ranges)
        {
            // Create directories synchronously (cheap, bounded count)
            auto [dir_prefix, file_prefix] = format_prefix(range.partition_id);
            auto dir_path = base_path / dir_prefix;
            std::filesystem::create_directories(dir_path);

            auto path = base_path / dir_prefix / with_extension(file_prefix, std::format(".{:06}", flush_iteration));

            tmc::post(flush_executor,
                      _flush_partition_task(
                          std::move(path),
                          range.begin,
                          range.end,
                          range.count,
                          range.sum_key_value_lengths,
                          range.partition_id,
                          flush_iteration,
                          cache,
                          use_odirect,
                          &results_mutex,
                          &results,
                          &errors,
                          &latch,
                          fdatasync_ssts),
                      0, thread_hint++ % thread_count);

            range_count++;
        }

        // Count down for partitions that don't exist
        for(size_t i = range_count; i < max_partitions; ++i)
            latch.count_down();

        co_await latch;

        if(!errors.empty())
            co_return hedge::error("Parallel flush failed: " + errors.front().to_string());

        co_return results;
    }

} // namespace hedge::db