#pragma once

#include <cstdint>

#include "cache.h"
#include "db/block.h"
#include "db/index_ops.h"
#include "error.hpp"
#include "io/io_requests.hpp"
#include "page_aligned_buffer.h"
#include "tmc/task.hpp"
#include "types.h"

namespace hedge::db
{

    struct write_result
    {
        size_t bytes_written{0};
        int32_t error_code{0};
    };

    class merge_write_buffer
    {
        page_aligned_buffer<std::byte> _buf;
        db::block_buffer_writer _block_writer;
        size_t _indexed_kvs{};

    public:
        merge_write_buffer(size_t buf_size)
            : _buf(buf_size),
              _block_writer(_buf.begin(), _buf.end())
        {
        }

        hedge::status write_item(std::span<const std::byte> key,
                                 std::span<const std::byte> value,
                                 page_aligned_buffer<key_t>& merged_meta_index,
                                 page_aligned_buffer<std::byte>& merged_meta_index_bytes)
        {
            auto s = this->_block_writer.push(
                key, value,
                [&merged_meta_index, &merged_meta_index_bytes](const auto& last_pushed_key)
                {
                    index_ops::append_meta_index_key(merged_meta_index_bytes, last_pushed_key);
                    merged_meta_index.emplace_back(last_pushed_key);
                });

            if(!s)
            {
                assert(s.error().code() == errc::BUFFER_FULL);
                return s;
            }

            ++this->_indexed_kvs;
            return hedge::ok();
        }

        const block_encoder& encoder()
        {
            return this->_block_writer.encoder();
        }

        bool empty()
        {
            return this->_block_writer.empty();
        }

        [[nodiscard]] size_t indexed_kv() const
        {
            return this->_indexed_kvs;
        }

        tmc::task<write_result> flush(
            int32_t output_fd,
            uint32_t new_file_id,
            size_t write_offset,
            const std::shared_ptr<sharded_page_cache>& cache)
        {
            this->_block_writer.force_commit();

            const size_t bytes_written = this->_block_writer.bytes_written();
            assert(hedge::is_page_aligned(bytes_written));

            int32_t res = co_await hedge::io::write(output_fd, this->_buf.begin(), bytes_written, write_offset);

            if(cache != nullptr)
                this->_populate_cache(new_file_id, write_offset, bytes_written, cache);

            this->_block_writer.reset();

            co_return write_result{
                .bytes_written = res >= 0 ? static_cast<size_t>(res) : 0,
                .error_code = res < 0 ? res : 0};
        }

    private:
        void _populate_cache(uint32_t new_file_id, size_t write_offset, size_t bytes_written, const std::shared_ptr<sharded_page_cache>& cache) const
        {

            size_t num_written_pages = bytes_written / PAGE_SIZE_IN_BYTES;

            assert(num_written_pages != 0);

            assert(hedge::is_page_aligned(write_offset));

            size_t start_page = write_offset / PAGE_SIZE_IN_BYTES;
            auto page_guards = cache->get_write_slots_range(new_file_id,
                                                            start_page,
                                                            num_written_pages);

            for(size_t cur_page = 0; cur_page < page_guards.size(); ++cur_page)
            {
                const auto* src_begin = this->_buf.data() + (cur_page * PAGE_SIZE_IN_BYTES);
                assert(src_begin < this->_buf.end());

                [[maybe_unused]] const auto* src_end = this->_buf.data() + ((cur_page + 1) * PAGE_SIZE_IN_BYTES);
                assert(src_end <= this->_buf.end());

                auto& maybe_page = page_guards[cur_page];
                if(maybe_page.has_value())
                {
                    auto& page = maybe_page.value();
                    auto* dst = page.data + page.idx;
                    std::memcpy(dst, src_begin, PAGE_SIZE_IN_BYTES);
                }
            }

        }
    };

} // namespace hedge::db
