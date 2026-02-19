#include <algorithm>
#include <error.hpp>

#include "db/block.h"
#include "db/index_ops.h"
#include "db/merge/merge_utils.h"
#include "db/merge/rolling_buffer.h"
#include "db/merge/write_buffer.h"
#include "db/sst.h"
#include "mailbox_impl.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{
    async::task<hedge::expected<sst>> index_ops::k_way_merge_async2( // NOLINT(readability-function-cognitive-complexity)
        const merge_config& config,
        const std::vector<const sst*>& indices,
        const std::shared_ptr<async::executor_context>& executor,
        std::shared_ptr<db::sharded_page_cache> cache)
    {
        // Check prefixes are all the same
        auto prefix_0 = indices[0]->upper_bound();
        if(!std::ranges::all_of(indices, [prefix_0](const sst* idx)
                                { return idx->upper_bound() == prefix_0; }))
            co_return hedge::error("All indices must have the same partition prefix for merging");

        // Step 0: Validate preconditions and init all the necessary structures --
        size_t read_ahead_size = config.read_ahead_size;
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
        auto new_path = config.base_path / dir / with_extension(file_name, std::format(".{}", config.new_index_id));

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
        size_t max_new_meta_index_entries = hedge::ceil(max_new_index_keys, INDEX_PAGE_NUM_ENTRIES);

        auto fd_maybe = co_await fs::file::from_path_async(new_path,
                                                           fs::file::open_mode::write_new,
                                                           async::this_thread_executor(),
                                                           true,
                                                           estimated_size);

        if(!fd_maybe.has_value())
            co_return hedge::error("Failed to create file descriptor for merged index at " + new_path.string() + ": " + fd_maybe.error().to_string());

        auto output_file = std::move(fd_maybe.value());

        auto maybe_read_file = co_await fs::file::from_path_async(
            output_file.path(),
            fs::file::open_mode::read_only,
            async::this_thread_executor(),
            config.create_new_with_odirect,
            std::nullopt);

        if(!maybe_read_file.has_value())
            co_return hedge::error("Failed to open merged index file for reading: " + maybe_read_file.error().to_string());

        auto read_file = std::move(maybe_read_file.value());

        if(!read_file.has_direct_access())
            posix_fadvise(read_file.fd(), 0, 0, POSIX_FADV_RANDOM);

        const size_t num_bufs = indices.size();

        rolling_buffer_set rbufs(num_bufs);

        for(const auto* index : indices)
        {
            rbufs.emplace_back(*index,
                               fs::file_reader2_config{
                                   .start_offset = index->_footer.index_offset,
                                   .end_offset = index->_footer.meta_index_offset,
                                   .read_ahead_size = read_ahead_size},
                               read_ahead_size,
                               config.try_reading_from_cache ? cache : nullptr);
        }

        [[maybe_unused]] size_t filtered_keys = 0; // TODO FIX: this was used for an assert that is currently disabled
        size_t bytes_written = 0;

        // Meta-index entries collected during the merge
        // Thes are respectively the internal representation and the serialized version of it
        auto merged_meta_index = page_aligned_buffer<key_t>(0, max_new_meta_index_entries);
        auto merged_meta_index_bytes = page_aligned_buffer<uint8_t>(0, max_new_meta_index_entries * sizeof(key_t));

        auto* rbufs_begin = rbufs.begin();
        auto* rbufs_end = rbufs.end();

        // Helper lambda to refresh both buffers by reading the next chunk from each index
        auto refresh_buffers = [&]() -> async::task<hedge::status>
        {
            for(auto* it = rbufs_begin; it < rbufs_end; ++it)
            {
                if((double)it->size() / (double)read_ahead_size < 0.15)
                {
                    if(auto status = co_await it->refresh(config.try_reading_from_cache ? cache : nullptr); !status)
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
        merge_iterator2 dedup{};

        // Keep track of the last written key for debugging purposes
        [[maybe_unused]] key_t last_written_key{};
        [[maybe_unused]] uint64_t compute_duration{0};
        [[maybe_unused]] size_t merge_iterations{0};

        merge_write_buffer write_buffer(read_ahead_size * 2);

        // Hot path: returns true if buffer is full and item was NOT written
        auto try_push_kv = [&] [[nodiscard]] (const index_entry2_t& kv) -> bool
        {
            auto s = write_buffer.write_item(kv, merged_meta_index, merged_meta_index_bytes);
            return (bool)s;
        };

        auto flush = [&]() -> async::task<hedge::status>
        {
            auto write_response = co_await write_buffer.flush(
                output_file.fd(),
                read_file.id(),
                bytes_written,
                config.populate_cache_with_output ? cache : nullptr,
                executor);

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
        auto flush_and_retry = [&](const index_entry2_t& kv) -> async::task<hedge::status>
        {
            auto flush_result = co_await flush();
            if(!flush_result)
                co_return flush_result;

            // Retry
            if(!try_push_kv(kv)) [[unlikely]]
                co_return hedge::error(std::format("Could not flush item of size: k[{}]v[{}]", kv.key.size(), sizeof(kv.value_ptr)));

            co_return hedge::ok();
        };

        [[maybe_unused]] size_t iteration_count{0};

        using heap_item_t = std::pair<index_entry2_t, rolling_buffer2*>; // Index entry + source buffer pointer

        auto heap_item_t_comparator = [](const auto& lhs, const auto& rhs)
        {
            // Cpp heap is a max-heap by default, we need a min-heap
            return !(lhs.first < rhs.first);
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

                index_entry2_t new_keypair;
                new_keypair.key = rbuf->front().key();
                std::optional<value_ptr_t> value_ptr_opt = value_ptr_t::try_from_span(rbuf->front().value());

                if(!value_ptr_opt.has_value())
                    co_return hedge::error("Failed to parse value_ptr from index entry during heap initialization");

                new_keypair.value_ptr = value_ptr_opt.value();
                rbuf->pop_front();

                key_heap.emplace_back(std::move(new_keypair), rbuf);
                std::ranges::push_heap(key_heap, heap_item_t_comparator);
            }
        }

        // Outer loop, interrupted when both buffers are EOF
        while(true)
        {

            // prof::counter_guard counter_guard{prof::get<"inner_merge_loop">()};

            std::ranges::pop_heap(key_heap, heap_item_t_comparator);
            const auto& key_and_ref_buf = key_heap.back();
            dedup.push(key_and_ref_buf.first);
            auto* rbuf = key_and_ref_buf.second;
            key_heap.pop_back();

            if(!rbuf->is_eof()) [[likely]]
            {
                if(rbuf->buffer_empty()) [[unlikely]]
                {
                    auto status = co_await refresh_buffers();
                    if(!status)
                        co_return hedge::error("Failed to refresh views: " + status.error().to_string());
                }

                index_entry2_t new_keypair;
                new_keypair.key = rbuf->front().key();
                std::optional<value_ptr_t> value_ptr_opt = value_ptr_t::try_from_span(rbuf->front().value());

                if(!value_ptr_opt.has_value())
                    co_return hedge::error("Failed to parse value_ptr from index entry during heap initialization");

                new_keypair.value_ptr = value_ptr_opt.value();
                rbuf->pop_front();

                key_heap.emplace_back(std::move(new_keypair), rbuf);
                std::ranges::push_heap(key_heap, heap_item_t_comparator);
            }

            iteration_count++;

            if(dedup.ready()) // For getting ready, we need to have pushed at least two entries
            {
                // Note that we are always lagging by one entry!
                // After popping from `dedup`, there is still one entry sitting there waiting to get checked against the next pushed one and thus getting ready
                auto new_item = dedup.pop();

                if(config.discard_deleted_keys && new_item.value_ptr.is_deleted())
                {
                    filtered_keys++;
                    continue;
                }

                if(!try_push_kv(new_item)) [[unlikely]]
                {
                    auto s = co_await flush_and_retry(new_item);
                    if(!s)
                        co_return s.error();
                }

                assert(last_written_key < new_item.key);
                last_written_key = new_item.key;
            }

            if(key_heap.empty())
                break;
        }

        prof::get<"merge_cache_bulk_writes_count">().add(0, 1);

        // Step 2: Handle remaining keys from the non-exhausted view, keeping in consideration the deduplicator --
        // The deduplicator maintains a 1-item lag.

        // Handle remaining keys from the non exhausted-view
        std::vector<index_entry2_t> last_items{};
        last_items.reserve(2); // The deduplicator might contain up to two items

        // We enter this block ONLY IF the non_empty_view had at least one item left
        if(dedup.ready())
            last_items.push_back(dedup.pop());

        // Empty the deduplicator buffer
        last_items.push_back(dedup.force_pop());

        for(const auto& new_item : last_items)
        {
            assert(last_written_key < new_item.key);

            if(config.discard_deleted_keys && new_item.value_ptr.is_deleted())
            {
                filtered_keys++;
                continue;
            }

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

        [[maybe_unused]] auto check_every_buf_is_eof = [](rolling_buffer2* rbufs_begin, rolling_buffer2* rbufs_end)
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

        [[maybe_unused]] auto check_key_count_matches = [&write_buffer, filtered_keys](rolling_buffer2* rbufs_begin, rolling_buffer2* rbufs_end)
        {
            size_t total_keys = 0;
            for(auto* it = rbufs_begin; it < rbufs_end; ++it)
                total_keys += it->index().size();

            if(total_keys != (write_buffer.indexed_kv() - filtered_keys))
            {
                std::cout << "Total keys from buffers: " << total_keys << " does not match indexed keys: " << write_buffer.indexed_kv() << "\n";
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
            auto res = co_await executor->submit_request(async::write_request{
                .fd = output_file.fd(),
                .data = merged_meta_index_bytes.data(),
                .size = hedge::ceil_page_align(merged_meta_index_bytes.size()),
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write meta index to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
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
            auto res = co_await executor->submit_request(async::write_request{
                .fd = output_file.fd(),
                .data = (uint8_t*)page_aligned_footer.raw_data(),
                .size = PAGE_SIZE_IN_BYTES,
                .offset = bytes_written});

            if(res.error_code != 0)
                co_return hedge::error("Failed to write footer to file: " + std::string(strerror(res.error_code)));

            bytes_written += res.bytes_written;
        }

        // Truncate file to the actual written size
        {
            auto res = co_await async::this_thread_executor()->submit_request(async::ftruncate_request{
                .fd = output_file.fd(),
                .length = bytes_written,
            });

            if(res.error_code != 0)
                co_return hedge::error("Failed to truncate merged file to final size: " + std::string(strerror(res.error_code)));
        }

        constexpr size_t REF_PAGE_SIZE = 4096;
        constexpr size_t KEYS_PER_META_INDEX_PAGE = REF_PAGE_SIZE / sizeof(key_t);

        std::optional<page_aligned_buffer<key_t>> super_index;

        if(merged_meta_index.size() > KEYS_PER_META_INDEX_PAGE * SUPER_INDEX_ENABLED_THRESHOLD)
            super_index = index_ops::create_super_index<key_t, REF_PAGE_SIZE>(merged_meta_index);

        merged_meta_index.shrink_to_fit();

        sst result{
            std::move(read_file),
            std::move(merged_meta_index),
            footer,
            std::move(super_index),
        };

        co_return result;
    }
} // namespace hedge::db
