#include <algorithm>
#include <cstdint>
#include <error.hpp>
#include <fcntl.h>
#include <mutex>
#include <sys/mman.h>

#include "db/block.h"
#include "db/merge/merge_utils.h"
#include "db/merge/write_buffer.h"
#include "db/quotient_filter.h"
#include "db/sorted_index.h"
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

    struct tmp_in_mem_sst
    {
        page_aligned_buffer<uint8_t> index;
        page_aligned_buffer<uint8_t> meta_index_bytes;
        page_aligned_buffer<key_t> meta_index;
    };

    tmp_in_mem_sst prepare_in_mem(const page_aligned_buffer<index_entry2_t>& sorted_entries, size_t average_key_value_length)
    {
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_value_length);
        size_t pages_estimate = hedge::ceil(sorted_entries.size(), key_values_per_page_estimate);

        page_aligned_buffer<uint8_t> index(PAGE_SIZE_IN_BYTES, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<uint8_t> meta_index_bytes(0, pages_estimate * PAGE_SIZE_IN_BYTES);
        page_aligned_buffer<key_t> meta_index(0, pages_estimate);
        size_t pages_written = 0;
        size_t total_entries_written = 0;

        block_encoder bb(index.data());

        while(total_entries_written < sorted_entries.size())
        {
            const auto& entry = sorted_entries[total_entries_written];

            auto s = bb.push(entry.key, entry.value);

            if(!s && s.error().code() == errc::BUFFER_FULL)
            {
                // The meta index key is the last key of the current block.
                // This way calling std::lower_bound on the meta index will give us the correct block for the key we're looking for.
                index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
                meta_index.emplace_back(bb.last_pushed_key());
                assert(bb.committed());

                // Allocate space for the next block if needed
                constexpr double SLACK = 0.15;
                if(index.size() == index.capacity())
                    index.reserve(index.size() * (1.0 + SLACK));

                pages_written++;
                index.resize(index.size() + PAGE_SIZE_IN_BYTES);
                bb.reset(index.data() + (PAGE_SIZE_IN_BYTES * pages_written));

                continue; // Retry pushing the same entry in the new block
            }

            total_entries_written++;
        }

        if(bb.kv_count() == 0)
        {
            // Handle the case where the last block is empty (which can happen if the last entry perfectly fills a block).
            index.resize(pages_written * PAGE_SIZE_IN_BYTES);
        }
        else
        {
            // Handle the case where the last block wasn't committed yet
            index_ops::append_meta_index_key(meta_index_bytes, bb.last_pushed_key());
            meta_index.emplace_back(bb.last_pushed_key());
            bb.commit(); // Commit is automatic when the buffer is full
        }

        index.shrink_to_fit();
        meta_index_bytes.shrink_to_fit();
        meta_index.shrink_to_fit();

        return tmp_in_mem_sst{
            .index = std::move(index),
            .meta_index_bytes = std::move(meta_index_bytes),
            .meta_index = std::move(meta_index)};
    }

    template <typename T>
    hedge::expected<size_t> pwrite_buffer(int fd, const page_aligned_buffer<T>& buffer, size_t offset)
    {
        const size_t bytes_to_write = hedge::round_up(buffer.size() * sizeof(T), PAGE_SIZE_IN_BYTES);

        int res = pwrite(fd,
                         buffer.data(),
                         bytes_to_write,
                         offset);

        if(res < 0)
            return hedge::error(std::format("Failed to write iovec to fd {} at offset {}: {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            strerror(errno)));

        if(static_cast<size_t>(res) != bytes_to_write)
            return hedge::error(std::format("Partial write occurred when writing iovec to fd {} at offset {}: wrote {} bytes out of {}.",
                                            fd,
                                            lseek(fd, 0, SEEK_CUR),
                                            res,
                                            bytes_to_write));

        return res;
    }

    static constexpr size_t QF_BITS_PER_KEY = 10;
    static constexpr size_t QF_FLAG_BITS = 3;

    static hedge::expected<quotient_filter> create_qf_for_key_count(size_t num_keys)
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
        return quotient_filter::make(q, r);
    }

    hedge::expected<sst> index_ops::save_as_sorted_index2(
        const std::filesystem::path& path,
        page_aligned_buffer<index_entry2_t>&& keys,
        size_t average_key_value_length,
        size_t upper_bound,
        size_t epoch,
        const std::shared_ptr<db::sharded_page_cache>& cache,
        bool use_odirect)
    {
        auto sorted_keys = std::move(keys);
        size_t num_keys = sorted_keys.size();
        tmp_in_mem_sst in_mem = prepare_in_mem(sorted_keys, average_key_value_length);

        // Build quotient filter from all keys before releasing them
        auto maybe_qf = create_qf_for_key_count(num_keys);
        std::optional<quotient_filter> qf;
        if(maybe_qf)
        {
            qf = std::move(maybe_qf.value());
            for(size_t i = 0; i < num_keys; ++i)
            {
                uint64_t hash = xxh64::hash((const char*)sorted_keys[i].key.data(), sorted_keys[i].key.size(), 0xDEADBEEF);
                qf->insert(hash);
            }
        }

        sorted_keys.free(); // Not needed anymore, release memory

        size_t qf_total_size = 0;
        if(qf.has_value())
            qf_total_size = PAGE_SIZE_IN_BYTES + hedge::ceil_page_align(qf->data_as_byte_span().size());

        const size_t sst_size =
            in_mem.index.size() + // Already page-aligned
            hedge::round_up(in_mem.meta_index_bytes.size(), PAGE_SIZE_IN_BYTES) +
            qf_total_size +
            hedge::round_up(sizeof(sst_footer), PAGE_SIZE_IN_BYTES);

        assert(is_page_aligned(sst_size));

        // Create and open SST
        auto maybe_sorted_index_file = fs::file::from_path(
            path,
            fs::file::open_mode::read_write_new,
            use_odirect,
            sst_size);

        if(!maybe_sorted_index_file)
            return hedge::error("Failed to create sorted index file: " + maybe_sorted_index_file.error().to_string());

        auto fd = maybe_sorted_index_file.value().fd();

        // Initialize footer
        sst_footer_builder fbuilder{};
        fbuilder.indexed_kv = num_keys;
        fbuilder.upper_bound = upper_bound;
        fbuilder.index_offset = 0;
        fbuilder.meta_index_entries = in_mem.meta_index.size();
        fbuilder.epoch = epoch;

        size_t bytes_written = 0;

        // Write main index
        auto maybe_res = pwrite_buffer(fd, in_mem.index, 0);
        if(!maybe_res)
            return hedge::error("Failed to write index data to sorted index file: " + maybe_res.error().to_string());
        bytes_written += maybe_res.value();
        fbuilder.meta_index_offset = bytes_written;

        // Write meta-index
        maybe_res = pwrite_buffer(fd, in_mem.meta_index_bytes, *fbuilder.meta_index_offset);
        if(!maybe_res)
            return hedge::error("Failed to write meta-index data to sorted index file: " + maybe_res.error().to_string());
        bytes_written += maybe_res.value();
        in_mem.meta_index_bytes.free(); // Release memory

        // Write quotient filter (header page + data pages)
        if(qf.has_value())
        {
            fbuilder.qf_offset = bytes_written;

            // Write QF header (the struct itself, padded to one page)
            auto header_span = qf->header_as_byte_span();
            page_aligned_buffer<uint8_t> qf_header_buf(PAGE_SIZE_IN_BYTES);
            std::memcpy(qf_header_buf.data(), header_span.data(), header_span.size());

            maybe_res = pwrite_buffer(fd, qf_header_buf, bytes_written);
            if(!maybe_res)
                return hedge::error("Failed to write QF header: " + maybe_res.error().to_string());
            bytes_written += maybe_res.value();

            // Write QF data (page-aligned)
            auto data_span = qf->data_as_byte_span();
            size_t data_write_size = hedge::ceil_page_align(data_span.size());
            page_aligned_buffer<uint8_t> qf_data_buf(data_write_size);
            std::memcpy(qf_data_buf.data(), data_span.data(), data_span.size());

            maybe_res = pwrite_buffer(fd, qf_data_buf, bytes_written);
            if(!maybe_res)
                return hedge::error("Failed to write QF data: " + maybe_res.error().to_string());
            bytes_written += maybe_res.value();

            fbuilder.qf_size = bytes_written - *fbuilder.qf_offset;
        }

        fbuilder.footer_offset = bytes_written;

        // Build footer
        auto maybe_footer = fbuilder.build();
        if(!maybe_footer)
            return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        const sst_footer& footer = maybe_footer.value();
        page_aligned_buffer<sst_footer> sorted_index_footer_buf(1);
        sorted_index_footer_buf[0] = footer;

        // Write footer
        maybe_res = pwrite_buffer(fd, sorted_index_footer_buf, *fbuilder.footer_offset);
        if(!maybe_res)
            return hedge::error("Failed to write footer data to sorted index file: " + maybe_res.error().to_string());
        assert(bytes_written + maybe_res.value() == sst_size);

        // Prepopulate cache with the index blocks
        if(cache != nullptr)
        {
            size_t curr_offset = 0;
            auto pages = cache->get_write_slots_range(maybe_sorted_index_file.value().id(), 0, footer.meta_index_offset / PAGE_SIZE_IN_BYTES);
            for(auto& maybe_page : pages)
            {
                if(!maybe_page.has_value())
                    continue;

                auto& page = maybe_page.value();
                std::memcpy(page.data + page.idx, in_mem.index.data() + curr_offset, PAGE_SIZE_IN_BYTES);
                curr_offset += PAGE_SIZE_IN_BYTES;
            }
        }
        in_mem.index.free(); // Release memory

        constexpr size_t REF_PAGE_SIZE = PAGE_SIZE_IN_BYTES;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        // Super index enabled if there would be 512 * 256 = 131072 meta index entries (2MB)
        // In this case the super index would be 512 * 16 bytes = 8KB
        std::optional<page_aligned_buffer<key_t>> super_index;

        if(in_mem.meta_index.size() > KEYS_PER_META_INDEX_PAGE * index_ops::SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(in_mem.meta_index);

        // print meta index
        // std::cout << "Meta index:" << std::endl;
        // for(const auto& key : in_mem.meta_index)
        //     std::cout << to_hex_string(key) << std::endl;

        // Create the final object, moving the key and meta-index data into it.
        auto ss = sst(std::move(maybe_sorted_index_file.value()), std::move(in_mem.meta_index), footer, std::move(super_index), std::move(qf));

        return ss;
    }

    static async::generator<index_ops::partition_range> partition_ranges_generator(
        memtable_impl3_t* index,
        size_t num_partition_exponent)
    {
        std::vector<index_ops::partition_range> ranges;

        size_t partition_key_prefix_range = (1 << 16) / (1 << num_partition_exponent);
        auto accessor = index->accessor();

        if(index->size() == 0)
            co_return;

        size_t current_partition_id = hedge::find_partition_prefix_for_key(accessor.begin()->key, partition_key_prefix_range);
        auto range_begin = accessor.begin();
        size_t count = 0;
        size_t sum_kv_len = 0;

        for(auto it = accessor.begin(); it != accessor.end(); ++it)
        {
            size_t part_id = hedge::find_partition_prefix_for_key(it->key, partition_key_prefix_range);

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
            sum_kv_len += it->key.size() + it->value.size();
        }

        // Last partition
        co_yield index_ops::partition_range{
            .partition_id = current_partition_id,
            .begin = range_begin,
            .end = accessor.end(),
            .count = count,
            .sum_key_value_lengths = sum_kv_len,
        };

        co_return;
    }

    static tmc::task<hedge::expected<sst>> save_as_sorted_index2_async(
        std::filesystem::path path,
        skiplist_t::Accessor::iterator begin,
        skiplist_t::Accessor::iterator end,
        size_t entry_count,
        size_t average_key_value_length,
        size_t upper_bound,
        size_t epoch,
        std::shared_ptr<db::sharded_page_cache> cache,
        bool use_odirect,
        bool fdatasync_output)
    {
        constexpr size_t WRITE_BUF_SIZE = PAGE_SIZE_IN_BYTES * 256;

        // Estimate file size
        size_t key_values_per_page_estimate = hedge::ceil(PAGE_SIZE_IN_BYTES, average_key_value_length);
        size_t pages_estimate = hedge::ceil(entry_count, key_values_per_page_estimate);
        size_t estimated_index_size = pages_estimate * PAGE_SIZE_IN_BYTES;
        size_t estimated_meta_index_size = hedge::round_up(pages_estimate * sizeof(key_t), PAGE_SIZE_IN_BYTES);
        size_t estimated_size = estimated_index_size + estimated_meta_index_size + PAGE_SIZE_IN_BYTES; // +footer

        auto maybe_file = co_await fs::file::from_path_async(
            path,
            fs::file::open_mode::read_write_new,
            use_odirect,
            estimated_size);

        if(!maybe_file)
            co_return hedge::error("Failed to create sorted index file: " + maybe_file.error().to_string());

        auto& file = maybe_file.value();
        auto fd = file.fd();
        auto file_id = file.id();

        // Allocate meta-index buffers
        auto meta_index = page_aligned_buffer<key_t>(0, pages_estimate);
        auto meta_index_bytes = page_aligned_buffer<uint8_t>(0, pages_estimate * sizeof(key_t));

        // Create quotient filter
        auto maybe_qf = create_qf_for_key_count(entry_count);
        std::optional<quotient_filter> qf;
        if(maybe_qf)
            qf = std::move(maybe_qf.value());

        merge_write_buffer write_buffer(WRITE_BUF_SIZE);
        size_t bytes_written = 0;

        auto flush = [&]() -> tmc::task<hedge::status>
        {
            auto write_response = co_await write_buffer.flush(
                fd, file_id, bytes_written, cache);

            if(write_response.error_code)
                co_return hedge::error("Failed to write index data: " + std::string(strerror(-write_response.error_code)));

            bytes_written += write_response.bytes_written;
            co_return hedge::ok();
        };

        for(auto it = begin; it != end; ++it)
        {
            if(qf.has_value())
                qf->insert(xxh64::hash((const char*)it->key.data(), it->key.size(), 0xDEADBEEF));

            auto s = write_buffer.write_item(it->key, it->value, meta_index, meta_index_bytes);
            if(!s)
            {
                // Buffer full — flush and retry
                auto flush_result = co_await flush();
                if(!flush_result)
                    co_return flush_result.error();

                auto retry = write_buffer.write_item(it->key, it->value, meta_index, meta_index_bytes);
                if(!retry)
                    co_return hedge::error("Could not write item after flush");
            }
        }

        // Flush remaining blocks
        if(!write_buffer.empty())
        {
            const auto& last_pushed_key = write_buffer.encoder().last_pushed_key();
            meta_index.emplace_back(last_pushed_key);
            index_ops::append_meta_index_key(meta_index_bytes, last_pushed_key);

            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result.error();
        }

        // Write meta-index
        size_t meta_write_size = hedge::ceil_page_align(meta_index_bytes.size());
        int32_t meta_write_res = co_await hedge::io::write(fd, meta_index_bytes.data(), meta_write_size, bytes_written);

        if(meta_write_res < 0)
            co_return hedge::error("Failed to write meta-index data: " + std::string(strerror(-meta_write_res)));

        size_t meta_index_offset = bytes_written;
        bytes_written += static_cast<size_t>(meta_write_res);
        meta_index_bytes.free();

        // Write quotient filter (header page + data pages)
        index_ops::sst_footer_builder fbuilder{};
        if(qf.has_value())
        {
            fbuilder.qf_offset = bytes_written;

            // Write QF header (padded to one page)
            auto header_span = qf->header_as_byte_span();
            page_aligned_buffer<uint8_t> qf_header_buf(PAGE_SIZE_IN_BYTES);
            std::memcpy(qf_header_buf.data(), header_span.data(), header_span.size());

            int32_t qf_header_res = co_await hedge::io::write(fd, qf_header_buf.data(), PAGE_SIZE_IN_BYTES, bytes_written);

            if(qf_header_res < 0)
                co_return hedge::error("Failed to write QF header: " + std::string(strerror(-qf_header_res)));
            bytes_written += static_cast<size_t>(qf_header_res);

            // Write QF data (page-aligned)
            auto data_span = qf->data_as_byte_span();
            size_t data_write_size = hedge::ceil_page_align(data_span.size());
            page_aligned_buffer<uint8_t> qf_data_buf(data_write_size);
            std::memcpy(qf_data_buf.data(), data_span.data(), data_span.size());

            int32_t qf_data_res = co_await hedge::io::write(fd, qf_data_buf.data(), data_write_size, bytes_written);

            if(qf_data_res < 0)
                co_return hedge::error("Failed to write QF data: " + std::string(strerror(-qf_data_res)));
            bytes_written += static_cast<size_t>(qf_data_res);

            fbuilder.qf_size = bytes_written - *fbuilder.qf_offset;
        }

        // Build and write footer
        fbuilder.indexed_kv = write_buffer.indexed_kv();
        fbuilder.upper_bound = upper_bound;
        fbuilder.index_offset = 0;
        fbuilder.meta_index_offset = meta_index_offset;
        fbuilder.meta_index_entries = meta_index.size();
        fbuilder.epoch = epoch;
        fbuilder.footer_offset = bytes_written;

        auto maybe_footer = fbuilder.build();
        if(!maybe_footer)
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        const sst_footer& footer = maybe_footer.value();
        page_aligned_buffer<sst_footer> footer_buf(1);
        footer_buf[0] = footer;

        int32_t footer_write_res = co_await hedge::io::write(fd, reinterpret_cast<uint8_t*>(footer_buf.data()), PAGE_SIZE_IN_BYTES, bytes_written);

        if(footer_write_res < 0)
            co_return hedge::error("Failed to write footer: " + std::string(strerror(-footer_write_res)));

        bytes_written += static_cast<size_t>(footer_write_res);

        // Truncate file to actual written size
        {
            int32_t res = co_await hedge::io::ftruncate(fd, bytes_written);

            if(res < 0)
                co_return hedge::error("Failed to truncate file to final size: " + std::string(strerror(-res)));
        }

        constexpr size_t REF_PAGE_SIZE = PAGE_SIZE_IN_BYTES;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        std::optional<page_aligned_buffer<key_t>> super_index;
        if(meta_index.size() > KEYS_PER_META_INDEX_PAGE * index_ops::SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(meta_index);

        if(!use_odirect)
            posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);

        if(fdatasync_output)
        {
            int32_t res = co_await hedge::io::fdatasync(fd);
            if(res < 0)
                co_return hedge::error("Failed to fdatasync file: " + std::string(strerror(-res)));
        }

        co_return sst(std::move(file),
                      std::move(meta_index),
                      footer,
                      std::nullopt, //  std::move(super_index),
                      std::move(qf));
    }

    static tmc::task<void> flush_partition_task(
        std::filesystem::path path,
        skiplist_t::Accessor::iterator range_begin,
        skiplist_t::Accessor::iterator range_end,
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

        auto maybe_sst = co_await save_as_sorted_index2_async(
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

    // Synchronous per-partition SST writer.
    // Streams pages to disk in 256-page (1 MB) batches via pwrite as blocks fill up.
    // No async I/O, no intermediate entry buffer — iterates directly over the skiplist range.
    static constexpr size_t FLUSH_BUF_PAGES = 512;

    tmc::task<hedge::expected<std::vector<sst>>> index_ops::flush_mem_index2_parallel(
        const std::filesystem::path& base_path,
        memtable_impl3_t* index,
        size_t num_partition_exponent,
        size_t flush_iteration,
        const std::shared_ptr<db::sharded_page_cache>& cache,
        bool use_odirect,
        tmc::ex_cpu& flush_executor,
        bool fdatasync_ssts)
    {
        if(num_partition_exponent > 16)
            co_return hedge::error("Number of partitions exponent must be less than or equal to 16");

        auto ranges = partition_ranges_generator(index, num_partition_exponent);

        size_t max_partitions = size_t(1) << num_partition_exponent;
        tmc::latch latch(max_partitions);

        std::mutex results_mutex;
        std::vector<sst> results;
        std::vector<hedge::error> errors;

        size_t thread_hint = 0;
        size_t thread_count = flush_executor.thread_count();
        size_t range_count = 0;
        for(auto& range : ranges)
        {
            // Create directories synchronously (cheap, bounded count)
            auto [dir_prefix, file_prefix] = format_prefix(range.partition_id);
            auto dir_path = base_path / dir_prefix;
            std::filesystem::create_directories(dir_path);

            auto path = base_path / dir_prefix / with_extension(file_prefix, std::format(".{:06}", flush_iteration));

            tmc::post(flush_executor,
                      flush_partition_task(
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