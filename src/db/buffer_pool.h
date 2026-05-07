#pragma once
#include <stack>

#include "io/io_ctx.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    class read_buffer_pool;

    // registered_fixed_buffer represents a fixed buffer registered on a io_uring
    // idx is the index of the buffer in the registered buffers, if it is from the pool
    // If the buffer is not registered, fixed_buffer_idx can be set to nullopt
    class registered_buffer
    {
        friend class read_buffer_pool;

        buffer_t _buf = make_null_buffer();
        std::optional<int32_t> _fixed_buffer_idx{std::nullopt};

    public:
        registered_buffer() = default;
        registered_buffer(const registered_buffer&) = delete;
        registered_buffer(registered_buffer&&) noexcept = default;

        registered_buffer(buffer_t buf, std::optional<int32_t> fixed_buffer_idx)
            : _buf(std::move(buf)), _fixed_buffer_idx(fixed_buffer_idx)
        {
        }

        [[nodiscard]] auto* data() const { return _buf.get(); }

        [[nodiscard]] std::optional<int32_t> idx() const { return _fixed_buffer_idx; }
    };

    // read_buffer_pool manages a pool of pre-allocated buffers for reading pages from disk.
    // This pools are registered with io_uring for fixed buffer reads
    class read_buffer_pool
    {
    private:
        using buffers_t = std::vector<registered_buffer>;

        std::stack<registered_buffer, buffers_t> _buffers;

    public:
        read_buffer_pool(uint32_t num_bufs)
        {
            buffers_t buffer_vec;
            buffer_vec.reserve(num_bufs);

            std::vector<std::byte*> buffer_ptrs;
            buffer_ptrs.reserve(num_bufs);

            for(size_t i = 0; i < num_bufs; ++i)
            {
                auto* buf_ptr = static_cast<std::byte*>(aligned_alloc(PAGE_SIZE_IN_BYTES, PAGE_SIZE_IN_BYTES));
                if(buf_ptr == nullptr)
                    throw std::runtime_error("Bould not allocate buffer");

                buffer_vec.emplace_back(buffer_t(buf_ptr, std::free), static_cast<int32_t>(i));
                buffer_ptrs.push_back(buf_ptr);
            }

            this->_buffers = std::stack<registered_buffer, buffers_t>(std::move(buffer_vec));
            io::io_ctx::this_thread_ctx->register_page_buffers(buffer_ptrs);
        }

        registered_buffer try_get_fixed_buffer()
        {
            if(this->_buffers.empty())
            {
                auto buf = make_aligned_buffer(PAGE_SIZE_IN_BYTES);
                if(buf.get() == nullptr)
                    throw std::runtime_error("Could not allocate buffer (read)");

                return {std::move(buf),
                        std::nullopt};
            }
            auto b = std::move(this->_buffers.top());
            this->_buffers.pop();

            // TODO fix: this disables the fixed_buffer path for io_uring as it seems problematic with work stealing
            // Also: I did not find any meaningful improvement but it still worth keeping
            b._fixed_buffer_idx = std::nullopt;

            return b;
        }

        void push(registered_buffer&& b)
        {
            this->_buffers.push(std::move(b));
        }
    };

    struct registered_buffer_guard
    {
        registered_buffer buf;
        read_buffer_pool* pool;

        registered_buffer_guard(registered_buffer buf, read_buffer_pool& reference_pool)
            : buf(std::move(buf)), pool(&reference_pool)
        {
        }

        [[nodiscard]] const auto& buffer() const { return buf; }

        registered_buffer_guard(const registered_buffer_guard&) = delete;
        registered_buffer_guard(registered_buffer_guard&& other) noexcept
            : buf(std::move(other.buf)), pool(std::exchange(other.pool, nullptr))
        {
        }

        ~registered_buffer_guard()
        {
            if(pool != nullptr && buf.idx().has_value())
                pool->push(std::move(buf));
        }
    };

} // namespace hedge::db