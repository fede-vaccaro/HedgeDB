#include <algorithm>
#include <error.hpp>

#include "db/block.h"
#include "db/index_ops.h"
#include "db/merge/merge_utils.h"
#include "db/merge/sst_stream.h"
#include "db/merge/write_buffer.h"
#include "db/quotient_filter.h"
#include "db/sst.h"
#include "io/io_requests.hpp"
#include "tmc/task.hpp"
#include "types.h"
#include "utils.h"
#include "xxh64.hpp"

namespace hedge::db
{
    static constexpr size_t QF_BITS_PER_KEY = 10;
    [[maybe_unused]] static constexpr size_t QF_FLAG_BITS = 3;

    static hedge::expected<quotient_filter> create_qf_for_key_count(size_t num_keys)
    {
        size_t slots = (num_keys * 4 + 2) / 3; // ceil(num_keys / 0.75)
        uint32_t q = 0;
        size_t s = 1;
        while(s < slots)
        {
            s <<= 1;
            q++;
        }
        uint32_t r = QF_BITS_PER_KEY;
        return quotient_filter::make(q, r);
    }

    tmc::task<hedge::expected<sst>> index_ops::k_way_merge_async2( // NOLINT(readability-function-cognitive-complexity)
        const merge_config& config,
        const std::vector<const sst*>& indices,
        std::shared_ptr<db::sharded_page_cache> cache)
    {
        // Check prefixes are all the same
        auto prefix_0 = indices[0]->upper_bound();
        if(!std::ranges::all_of(indices, [prefix_0](const sst* idx)
                                { return idx->upper_bound() == prefix_0; }))
            co_return hedge::error("All indices must have the same partition prefix for merging");

        // Step 0: Validate preconditions and init all the necessary structures --
        size_t read_ahead_size = config.read_ahead_size / indices.size();
        if(!is_page_aligned(read_ahead_size))
            read_ahead_size = hedge::ceil_page_align(read_ahead_size);

        // Create new footer builder and start populating it
        sst_footer_builder fb;

        auto argmax_it = std::ranges::max_element(indices, [](const auto* lhs, const auto* rhs)
                                                  { return lhs->epoch() > rhs->epoch(); });

        fb.epoch = (*argmax_it)->epoch();
        fb.upper_bound = indices[0]->_footer.upper_bound;
        fb.index_offset = 0;

        // Extrapolate new index path
        auto [dir, file_name] = format_prefix(indices[0]->upper_bound());
        auto new_path = config.base_path / dir / with_extension(file_name, std::format(".{:06}", config.new_index_id));

        // estimated_size upper bound, will be truncated later
        size_t estimated_size = std::accumulate(
            indices.begin(),
            indices.end(),
            static_cast<size_t>(0),
            [](size_t acc, const sst* idx)
            {
                return acc + idx->file_size();
            });

        size_t max_new_index_keys = std::accumulate(
            indices.begin(),
            indices.end(),
            static_cast<size_t>(0),
            [](size_t acc, const sst* idx)
            {
                return acc + idx->_footer.indexed_keys;
            });

        constexpr size_t ENTRIES_PER_PAGE_ESTIMATE = 32; // Rough assumption with 24 byte keys and 100 byte values
        size_t meta_index_entries_estimate = hedge::ceil(max_new_index_keys, ENTRIES_PER_PAGE_ESTIMATE);

        // Create quotient filter (slightly oversized due to dedup — acceptable)
        auto maybe_qf = create_qf_for_key_count(max_new_index_keys);
        std::optional<quotient_filter> qf;
        if(maybe_qf)
            qf = std::move(maybe_qf.value());

        auto fd_maybe = co_await fs::file::from_path_async(new_path,
                                                           fs::file::open_mode::read_write_new,
                                                           config.create_new_with_odirect,
                                                           estimated_size);

        if(!fd_maybe.has_value())
            co_return hedge::error("Failed to create file descriptor for merged index at " + new_path.string() + ": " + fd_maybe.error().to_string());

        auto output_file = std::move(fd_maybe.value());

        const size_t num_bufs = indices.size();

        sst_stream_set rbufs(num_bufs);

        for(const auto* index : indices)
        {
            rbufs.emplace_back(*index,
                               fs::file_reader2_config{
                                   .start_offset = index->_footer.index_offset,
                                   .end_offset = index->_footer.meta_index_offset,
                                   .read_ahead_size = read_ahead_size},
                               read_ahead_size);
        }

        [[maybe_unused]] size_t filtered_keys = 0;
        size_t bytes_written = 0;

        // Meta-index entries collected during the merge
        // Thes are respectively the internal representation held in memory for lookup and the serialized version of it
        auto merged_meta_index = page_aligned_buffer<key_t>(0, meta_index_entries_estimate);
        auto merged_meta_index_bytes = page_aligned_buffer<std::byte>(0, meta_index_entries_estimate * sizeof(key_t));

        auto* rbufs_begin = rbufs.begin();
        auto* rbufs_end = rbufs.end();

        // Helper lambda to refresh both buffers by reading the next chunk from each index
        auto refresh_buffers = [&]() -> tmc::task<hedge::status>
        {
            for(auto* it = rbufs_begin; it < rbufs_end; ++it)
            {
                if((double)it->size() / (double)read_ahead_size < 0.35)
                {
                    if(auto status = co_await it->refresh(); !status)
                        co_return hedge::error("Failed to read from buffer during buffer refresh: " + status.error().to_string());
                }
            }

            co_return hedge::ok();
        };

        auto initial_load_status = co_await refresh_buffers();
        if(!initial_load_status)
            co_return hedge::error("Failed to perform initial buffer refresh: " + initial_load_status.error().to_string());

        // Needed to handle possible entry with duplicated keys (but different value_ptr) between LHS and RHS
        // In different sorted indices, there might be duplicated keys with different value_ptr because this
        // is how update/remove operations are implemented. Upon merge, we have to respect the Key-Uniqueness
        // constraint of the output sorted index
        //
        // Brief recall on how the entry deduplication works:
        // When pushing a new item:
        // - If its key is the same as the last pushed one, the internal buffer keeps only the one with the highest value_ptr (most recent entry).
        // - If its key is different from the last pushed one, the last pushed is rotate to the output (ready to be popped), and the new item is stored as the current one.
        deduplicator dedup{};

        // Keep track of the last written key for debugging purposes
        [[maybe_unused]] key_t last_written_key{};
        [[maybe_unused]] uint64_t compute_duration{0};
        [[maybe_unused]] size_t merge_iterations{0};

        merge_write_buffer write_buffer(read_ahead_size * 2);

        // Hot path: returns true if buffer is full and item was NOT written
        auto try_push_kv = [&] [[nodiscard]] (const merge_entry_t& kv) -> bool
        {
            auto s = write_buffer.write_item(kv.key, kv.value, merged_meta_index, merged_meta_index_bytes);
            return (bool)s;
        };

        auto flush = [&]() -> tmc::task<hedge::status>
        {
            auto write_response = co_await write_buffer.flush(
                output_file.fd(),
                output_file.id(),
                bytes_written,
                config.populate_cache_with_output ? cache : nullptr);

            if(write_response.error_code)
            {
                co_return hedge::error(
                    "Failed to write merged keys to file: " +
                    std::string(strerror(-write_response.error_code)));
            }

            bytes_written += write_response.bytes_written;

            co_return hedge::ok();
        };

        // Cold path: flushes and retries writing the item
        auto flush_and_retry = [&](const merge_entry_t& kv) -> tmc::task<hedge::status>
        {
            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result;

            // Retry
            if(!try_push_kv(kv)) [[unlikely]]
                co_return hedge::error(std::format("Could not flush item of size: k[{}]v[{}]", kv.key.size(), 0)); // TODO: fix size using visitor

            co_return hedge::ok();
        };

        [[maybe_unused]] size_t iteration_count{0};

        using heap_item_t = std::pair<merge_entry_t, sst_stream*>; // Index entry + source buffer pointer

        auto heap_item_t_comparator = [](const heap_item_t& lhs, const heap_item_t& rhs)
        {
            // Cpp heap is a max-heap by default, we need a min-heap
            auto cmp = lhs.first.key <=> rhs.first.key;
            if(cmp != 0)
                return cmp > 0;                       // min-heap by key
            return lhs.first.epoch < rhs.first.epoch; // higher epoch pops first (newer wins)
        };

        std::vector<heap_item_t> key_heap;
        key_heap.reserve(num_bufs);

        // Initialize the heap with the first element from each buffer
        for(auto* rbuf = rbufs_begin; rbuf < rbufs_end; ++rbuf)
        {
            if(!rbuf->is_eof())
            {
                if(rbuf->buffer_empty())
                {
                    auto status = co_await refresh_buffers();
                    if(!status)
                        co_return hedge::error("Failed to refresh views: " + status.error().to_string());
                }

                merge_entry_t new_keypair{
                    .key = rbuf->front().key(),
                    .value = rbuf->front().value(),
                    .epoch = rbuf->index().epoch(),
                };

                auto ok = rbuf->pop_front();
                if(!ok) [[unlikely]]
                    co_return ok.error();

                key_heap.emplace_back(std::move(new_keypair), rbuf);
                std::ranges::push_heap(key_heap, heap_item_t_comparator);
            }
        }

        auto is_tombstone = [](const merge_entry_t& entry) -> bool
        {
            assert(entry.value.size() > 0);
            return static_cast<value_type>(entry.value.data()[0]) == value_type::TOMBSTONE;
        };

        // Outer loop, interrupted when both buffers are EOF
        [[maybe_unused]] size_t i = 0;
        while(!key_heap.empty())
        {
            // prof::counter_guard counter_guard{prof::get<"inner_merge_loop">()};

            std::ranges::pop_heap(key_heap, heap_item_t_comparator);
            auto [keyvalue, rbuf] = std::move(key_heap.back());
            key_heap.pop_back();
            // auto it = std::ranges::min_element(key_heap, heap_item_t_comparator);
            // auto [keyvalue, rbuf] = std::move(*it);
            // key_heap.erase(it);

            dedup.push(std::move(keyvalue));

            if(!rbuf->is_eof()) [[likely]]
            {
                if(rbuf->buffer_empty()) [[unlikely]]
                {
                    auto status = co_await refresh_buffers();
                    if(!status)
                        co_return hedge::error("Failed to refresh views: " + status.error().to_string());
                }

                auto maybe_value = value_from_span(rbuf->front().value());
                if(!maybe_value.has_value())
                    co_return hedge::error("Failed to parse value from index entry during heap initialization: " + maybe_value.error().to_string());

                merge_entry_t new_keypair{
                    .key = rbuf->front().key(),
                    .value = rbuf->front().value(),
                    .epoch = rbuf->index().epoch(),
                };

                auto ok = rbuf->pop_front();
                if(!ok) [[unlikely]]
                    co_return ok.error();

                key_heap.emplace_back(std::move(new_keypair), rbuf);
                std::ranges::push_heap(key_heap, heap_item_t_comparator);
            }

            iteration_count++;

            if(dedup.ready()) // For getting ready, we need to have pushed at least two entries
            {
                // Note that we are always lagging by one entry!
                // After popping from `dedup`, there is still one entry sitting there waiting to get checked against the next pushed one and thus getting ready
                auto new_item = dedup.pop();
                assert(last_written_key < new_item.key);
                last_written_key = new_item.key;

                if(config.discard_deleted_keys && is_tombstone(new_item))
                {
                    filtered_keys++;
                    continue;
                }

                if(qf.has_value())
                    qf->insert(xxh64::hash((const char*)new_item.key.data(), new_item.key.size(), sst::QF_SEED));

                if(!try_push_kv(new_item)) [[unlikely]]
                {
                    auto s = co_await flush_and_retry(new_item);
                    if(!s)
                        co_return s.error();
                }
            }
        }

        // Step 2: Handle remaining keys from the non-exhausted view, keeping in consideration the deduplicator --
        // The deduplicator maintains a 1-item lag.

        // Handle remaining keys from the non exhausted-view
        std::vector<merge_entry_t> last_items{};
        last_items.reserve(2); // The deduplicator might contain up to two items

        // We enter this block ONLY IF the non_empty_view had at least one item left
        if(dedup.ready())
            last_items.emplace_back(dedup.pop());

        // Empty the deduplicator buffer
        last_items.emplace_back(dedup.force_pop());

        for(const auto& new_item : last_items)
        {
            assert(last_written_key < new_item.key);

            if(config.discard_deleted_keys && is_tombstone(new_item))
            {
                filtered_keys++;
                continue;
            }

            if(qf.has_value())
                qf->insert(xxh64::hash((const char*)new_item.key.data(), new_item.key.size(), sst::QF_SEED));

            if(!try_push_kv(new_item)) [[unlikely]]
            {
                auto s = co_await flush_and_retry(new_item);
                if(!s)
                    co_return s.error();
            }
        }

        // The condition is needed to write the last meta-index entry if the total number of indexed keys is not a multiple of the page size
        // We avoid entering in this condition if this key was already recorded (as happens when the last written key aligns with the page size)
        if(!write_buffer.empty())
        {
            // Update meta index with the block's representative key (last pushed key)
            const auto& last_pushed_key = write_buffer.encoder().last_pushed_key();
            merged_meta_index.emplace_back(last_pushed_key);
            append_meta_index_key(merged_meta_index_bytes, last_pushed_key);

            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result.error();
        }

        [[maybe_unused]] auto check_every_buf_is_eof = [](sst_stream* rbufs_begin, sst_stream* rbufs_end)
        {
            for(auto* it = rbufs_begin; it < rbufs_end; ++it)
            {
                if(!it->is_eof())
                {
                    std::cout << "Buffer " << it->index().path() << " not EOF!\n";
                    return false;
                }
            }
            return true;
        };

        assert(check_every_buf_is_eof(rbufs_begin, rbufs_end) && "LHS reader not at EOF after merge");

        [[maybe_unused]] auto check_key_count_matches = [&write_buffer, filtered_keys, &dedup](sst_stream* rbufs_begin, sst_stream* rbufs_end)
        {
            size_t total_keys = 0;
            for(auto* it = rbufs_begin; it < rbufs_end; ++it)
                total_keys += it->index().size();

            size_t expected = write_buffer.indexed_kv() + dedup.deduplicated_keys() + filtered_keys;
            if(total_keys != expected)
            {
                std::cout << "Total keys from buffers: " << total_keys
                          << " does not match indexed keys: " << write_buffer.indexed_kv()
                          << " + deduplicated: " << dedup.deduplicated_keys()
                          << " + filtered: " << filtered_keys << "\n";
                return false;
            }

            return true;
        };

        fb.meta_index_offset = bytes_written;
        fb.index_offset = 0;
        fb.indexed_kv = write_buffer.indexed_kv();

        assert(check_key_count_matches(rbufs_begin, rbufs_end) && "Total keys from buffers does not match indexed keys count");

        /*
        -- Step 4: Write meta-index and footer --
        */
        // Write meta-index
        fb.meta_index_entries = merged_meta_index.size();

        {
            auto res = co_await hedge::io::write(
                output_file.fd(),
                merged_meta_index_bytes.data(),
                hedge::ceil_page_align(merged_meta_index_bytes.size()),
                bytes_written);

            if(res < 0)
                co_return hedge::error("Failed to write meta index to file: " + std::string(strerror(-res)));

            bytes_written += static_cast<size_t>(res);
        }

        // Write quotient filter (header page + data pages)
        if(qf.has_value())
        {
            fb.qf_offset = bytes_written;

            // Write QF header (padded to one page)
            auto header_span = qf->header_as_byte_span();
            page_aligned_buffer<std::byte> qf_header_buf(PAGE_SIZE_IN_BYTES);
            std::memcpy(qf_header_buf.data(), header_span.data(), header_span.size());

            auto qf_header_res = co_await hedge::io::write(
                output_file.fd(),
                qf_header_buf.data(),
                PAGE_SIZE_IN_BYTES,
                bytes_written);

            if(qf_header_res < 0)
                co_return hedge::error("Failed to write QF header: " + std::string(strerror(-qf_header_res)));
            bytes_written += static_cast<size_t>(qf_header_res);

            // Write QF data (page-aligned)
            auto data_span = qf->data_as_byte_span();
            size_t data_write_size = hedge::ceil_page_align(data_span.size());
            page_aligned_buffer<std::byte> qf_data_buf(data_write_size);
            std::memcpy(qf_data_buf.data(), data_span.data(), data_span.size());

            auto qf_data_res = co_await hedge::io::write(
                output_file.fd(),
                qf_data_buf.data(),
                data_write_size,
                bytes_written);

            if(qf_data_res < 0)
                co_return hedge::error("Failed to write QF data: " + std::string(strerror(-qf_data_res)));
            bytes_written += static_cast<size_t>(qf_data_res);

            fb.qf_size = bytes_written - *fb.qf_offset;
        }

        fb.footer_offset = bytes_written;

        auto maybe_footer = fb.build();
        if(!maybe_footer.has_value())
            co_return hedge::error("Failed to build footer: " + maybe_footer.error().to_string());

        auto& footer = maybe_footer.value();

        page_aligned_buffer<sst_footer> page_aligned_footer(1);
        page_aligned_footer[0] = footer;

        // Write footer
        {
            auto res = co_await hedge::io::write(
                output_file.fd(),
                (std::byte*)page_aligned_footer.raw_data(),
                PAGE_SIZE_IN_BYTES,
                bytes_written);

            if(res < 0)
                co_return hedge::error("Failed to write footer to file: " + std::string(strerror(-res)));

            bytes_written += static_cast<size_t>(res);
        }

        // Truncate file to the actual written size
        {
            auto res = co_await hedge::io::ftruncate(output_file.fd(), bytes_written);

            if(res < 0)
                co_return hedge::error("Failed to truncate merged file to final size: " + std::string(strerror(-res)));
        }

        constexpr size_t REF_PAGE_SIZE = PAGE_SIZE_IN_BYTES;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        std::optional<page_aligned_buffer<key_t>> super_index;

        if(merged_meta_index.size() > KEYS_PER_META_INDEX_PAGE * sst::SUPER_INDEX_ENABLE_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(merged_meta_index);

        merged_meta_index.shrink_to_fit();

        if(config.fdatasync_output)
        {
            int32_t res = co_await hedge::io::fdatasync(output_file.fd());
            if(res < 0)
                co_return hedge::error("Failed to fdatasync merged file: " + std::string(strerror(-res)));
        }

        if(!output_file.has_direct_access())
            posix_fadvise(output_file.fd(), 0, 0, POSIX_FADV_RANDOM);

        sst result{
            std::move(output_file),
            std::move(merged_meta_index),
            footer,
            std::move(super_index),
            std::move(qf),
        };

        co_return result;
    }
} // namespace hedge::db
