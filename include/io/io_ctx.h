#pragma once

#include <coroutine>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <stdexcept>
#include <unordered_map>

#include <liburing.h>
#include <liburing/io_uring.h>

#include <tmc/current.hpp>
#include <tmc/detail/awaitable_customizer.hpp>
#include <tmc/sync.hpp>

#include "io/io_requests_impl.h"
#include "types.h"

using namespace std::string_literals;

namespace hedge::io
{
    struct io_callback
    {
        tmc::detail::awaitable_customizer<int32_t> customizer;
        io_request* req;

        std::coroutine_handle<> resume(int32_t result)
        {
            req->res = result;
            *customizer.result_ptr = result;

            return customizer.resume_continuation();
        }

        void prepare(io_uring_sqe* sqe) const
        {
            req->prepare_sqe(sqe);
        }
    };

    class io_ctx
    {
        io_uring _uring;
        std::deque<io_callback> _waiting_for_io;
        std::unordered_map<size_t, io_callback> _in_flight;
        uint32_t _queue_depth;
        size_t _req_id;
        std::vector<iovec> _registered_page_buffers;

    public:
        inline static thread_local io_ctx* this_thread_ctx = nullptr;

        io_ctx(uint32_t queue_depth) : _queue_depth(queue_depth)
        {
            int32_t ret = io_uring_queue_init(this->_queue_depth, &this->_uring,
                                              IORING_SETUP_SINGLE_ISSUER |
                                                  IORING_SETUP_COOP_TASKRUN |
                                                  IORING_SETUP_DEFER_TASKRUN |
                                                  IORING_SETUP_TASKRUN_FLAG);

            if(ret < 0)
                throw std::runtime_error("error with io_uring_queue_init: "s +
                                         strerror(-ret));
        }

        [[nodiscard]] size_t queue_depth() const
        {
            return this->_queue_depth;
        }

        [[nodiscard]] size_t cq_size() const
        {
            return this->_queue_depth * 2;
        }

        static void set_thread_local(io_ctx* ex)
        {
            this_thread_ctx = ex;
        }

        [[nodiscard]] const io_uring* uring() const
        {
            return &this->_uring;
        }

        void register_page_buffers(const std::vector<std::byte*>& buffers)
        {
            this->_registered_page_buffers.resize(buffers.size());
            size_t c = 0;
            for(auto& io_vec : this->_registered_page_buffers)
            {
                io_vec.iov_base = buffers[c];
                io_vec.iov_len = PAGE_SIZE_IN_BYTES;
                c++;
            }

            auto res = io_uring_register_buffers(&this->_uring, this->_registered_page_buffers.data(), buffers.size());
            if(res < 0)
                throw std::runtime_error("io_uring_register_buffers failed: " + std::string(strerror(-res)));
        }

        size_t reap_cq()
        {
            io_uring_cqe* cqe;

            uint32_t head{};
            uint32_t cqe_count{};

            io_uring_for_each_cqe(&this->_uring, head, cqe)
            {
                uint64_t request_id = io_uring_cqe_get_data64(cqe);
                auto req = this->_in_flight.extract(request_id);
                auto continuation = req.mapped().resume(cqe->res);

                tmc::post(tmc::current_executor(), std::move(continuation), 0, tmc::current_thread_index()); // NOLINT

                cqe_count++;
            }

            io_uring_cq_advance(&this->_uring, cqe_count);
            return cqe_count;
        }

        [[nodiscard]] size_t in_flight_count() const
        {
            return this->_in_flight.size();
        }

        [[nodiscard]] size_t pending_count() const
        {
            return this->_waiting_for_io.size();
        }

        size_t submit_and_wait()
        {
            auto cq_ready = static_cast<int32_t>(io_uring_cq_ready(&this->_uring));
            auto in_flight = static_cast<int32_t>(this->_in_flight.size());

            auto potential_cqe_ready = cq_ready + in_flight;
            auto cqe_space_margin = static_cast<int32_t>(this->cq_size()) - potential_cqe_ready;

            [[maybe_unused]] size_t submitted_sqe_count = 0;

            if(cqe_space_margin > 0) // avoid cqe overflow risk
            {
                auto sq_space_left = io_uring_sq_space_left(&this->_uring);
                auto sq_to_submit = std::min(static_cast<int32_t>(sq_space_left), cqe_space_margin);

                auto pop_count = std::min(static_cast<size_t>(sq_to_submit), this->_waiting_for_io.size());

                for(size_t i = 0; i < pop_count; ++i)
                {
                    auto io_callback = this->_waiting_for_io.front();
                    this->_waiting_for_io.pop_front();

                    io_uring_sqe* sqe = io_uring_get_sqe(&this->_uring);
                    io_callback.prepare(sqe);
                    uint64_t this_req_id = this->_req_id++;
                    io_uring_sqe_set_data64(sqe, this_req_id);

                    this->_in_flight.emplace(this_req_id, io_callback);
                }
            }

            const uint32_t wait_for = this->_in_flight.size() > 0 ? 1 : 0;

            // The SQEs above are already in the ring; io_uring_enter submits before waiting, so an
            // EINTR (e.g. Go's async-preemption SIGURG hitting the blocking wait) leaves them
            // submitted — retrying only re-waits, it does not re-submit.
            int ret;
            do
            {
                ret = io_uring_submit_and_wait(&this->_uring, wait_for);
            } while(ret == -EINTR);

            if(ret < 0)
                throw std::runtime_error("io_uring_submit_and_wait: "s + strerror(-ret));

            return reap_cq();
        }

        void submit_request(io_callback req)
        {
            this->_waiting_for_io.emplace_back(req);

            if(this->_waiting_for_io.size() == this->_queue_depth)
                this->submit_and_wait();
            else if(io_uring_cq_ready(&this->_uring) > 0)
                this->reap_cq();
        }

        ~io_ctx()
        {
            io_uring_queue_exit(&this->_uring);
        }
    };

    struct aw_io
    {
        friend tmc::detail::awaitable_traits<aw_io>;

    private:
        void _initiate()
        {
            auto* this_thread_uring = io_ctx::this_thread_ctx;
            assert(this_thread_uring != nullptr);
            this_thread_uring->submit_request(io_callback{
                .customizer = this->customizer,
                .req = this->_request.get()});
        }

        tmc::detail::awaitable_customizer<int32_t> customizer;
        std::unique_ptr<io_request> _request;

    public:
        using result_type = int32_t;
        static constexpr tmc::detail::configure_mode mode = tmc::detail::ASYNC_INITIATE;

        explicit aw_io(std::unique_ptr<io_request> request)
            : _request(std::move(request))
        {
        }

        [[nodiscard]] io_request* request() const { return _request.get(); }

        bool await_ready() { return false; }

        void await_suspend(std::coroutine_handle<> Outer) noexcept
        {
            customizer.continuation = Outer.address();
            customizer.result_ptr = &this->_request->res;
            this->_initiate();
        }

        [[nodiscard]] TMC_AWAIT_RESUME int32_t await_resume() const
        {
            return this->_request->res;
        }
    };
} // namespace hedge::io

template <>
struct tmc::detail::awaitable_traits<hedge::io::aw_io>
{
    using result_type = hedge::io::aw_io::result_type;

    // Controls the behavior when awaited directly in a tmc::task.
    // This implementation requires an rvalue awaitable (so it can only be awaited once).
    static decltype(auto) get_awaiter(hedge::io::aw_io&& awaitable) noexcept
    {
        return static_cast<hedge::io::aw_io&&>(awaitable);
    }

    // Section controlling the behavior when wrapped by a utility function
    // such as tmc::spawn_*().
    static constexpr tmc::detail::configure_mode mode =
        tmc::detail::ASYNC_INITIATE;

    static void async_initiate(
        hedge::io::aw_io&& awaitable, [[maybe_unused]] tmc::ex_any* Executor,
        [[maybe_unused]] size_t Priority)
    {
        awaitable._initiate();
    }

    static void
    set_result_ptr(hedge::io::aw_io& awaitable, int32_t* result_ptr)
    {
        awaitable.customizer.result_ptr = result_ptr;
    }

    static void
    set_continuation(hedge::io::aw_io& awaitable, void* Continuation)
    {
        awaitable.customizer.continuation = Continuation;
    }

    static void
    set_continuation_executor(hedge::io::aw_io& awaitable, void* ContExec)
    {
        awaitable.customizer.continuation_executor = ContExec;
    }

    static void set_done_count(hedge::io::aw_io& awaitable, void* DoneCount)
    {
        awaitable.customizer.done_count = DoneCount;
    }

    static void set_flags(hedge::io::aw_io& awaitable, size_t Flags)
    {
        awaitable.customizer.flags = Flags;
    }
};
