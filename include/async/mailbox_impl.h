#pragma once

#include <bits/types/struct_iovec.h>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <linux/stat.h>
#include <memory>
#include <span>
#include <variant>
#include <vector>

#include <liburing.h>

namespace hedge::async
{

    template <typename Derived>
    struct mailbox_base
    {
        std::coroutine_handle<> continuation;

        void prepare_sqe(io_uring_sqe* sqe)
        {
            return static_cast<Derived*>(this)->prepare_sqe(sqe);
        };

        void handle_cqe(io_uring_cqe* cqe)
        {
            return static_cast<Derived*>(this)->handle_cqe(cqe);
        }

        void* get_response() // todo: use &response as default otherwise call get_response() on the derived class (implement through SFINAE)
        {
            return static_cast<Derived*>(this)->get_response();
        }

        void set_continuation(std::coroutine_handle<> handle)
        {
            this->continuation = handle;
        }

        void resume()
        {
            /*
            TODO:
            Use an std::atomic_bool for signaling that the response has been set.
            This is needed for allowing:

                awaitable_mailbox<read_response> future = co_await executor.submit_request(read_request{...});

                // do non blocking stuff the meanwhile

                auto read_response = co_await future;

                At the time being, this won't work and will make the program to crash because the io_executor will try to resume it later
                but the continuation is set only within awaitable_mailbox::await_suspend, which is called only on 'co_await future'
                i'm not sure an atomic_bool is needed, since so far the executor is single-threaded
            */

            if(this->continuation.done())
                return;

            this->continuation.resume();

            // return this->continuation.done();
        }
    };

    /*

        To declare a mailbox (that generally speaking it is a
        wrapper around some liburing prep operation), the following scheme is used
        struct {op}_request;
        struct {op}_response;
        struct {op}_mailbox;

        struct {op}_request
        {
            using response_t = {op}_response;
            using mailbox_t = {op}_mailbox;

            // request members
        };

        struct {op}_response
        {
            // response members
        };

        struct {op}_mailbox
        {
            // mailbox implementation
        };

        basically it is needed for a certain request to exhibit the {op}_response_t and {op}_mailbox_t types
        this is needed because the request type is determined from the client (i.e. the coroutine calling
        executor_context::submit_request) and in sequence, the associated mailbox and response types are
        derived depending on the types binded to the request.

    */

    // NOLINTBEGIN (*-readability-convert-member-functions-to-static)
    struct read_response;
    struct read_mailbox;

    struct read_request
    {
        using response_t = read_response;
        using mailbox_t = read_mailbox;

        int32_t fd{-1};
        uint8_t* data{nullptr};
        size_t offset{0};
        size_t size{0};
    };

    struct read_response
    {
        size_t bytes_read{0};
        int32_t error_code{0};
    };

    struct read_mailbox : mailbox_base<read_mailbox>
    {
        read_mailbox(read_request req)
            : request(req) {}

        read_request request;
        read_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct unaligned_read_response;
    struct unaligned_read_mailbox;

    struct unaligned_read_request
    {
        using response_t = unaligned_read_response;
        using mailbox_t = unaligned_read_mailbox;

        int32_t fd{-1};
        size_t offset{0};
        size_t size{0};
    };

    struct unaligned_read_response
    {
        std::vector<uint8_t> data{};
        size_t bytes_read{0};
        int32_t error_code{0};
    };

    struct unaligned_read_mailbox : mailbox_base<unaligned_read_mailbox>
    {
        unaligned_read_mailbox(unaligned_read_request req)
            : request(req) {}

        unaligned_read_request request;
        unaligned_read_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct unaligned_readv_response;
    struct unaligned_readv_mailbox;

    struct unaligned_readv_request
    {
        using response_t = unaligned_readv_response;
        using mailbox_t = unaligned_readv_mailbox;

        int32_t fd{-1};
        iovec* iovecs{nullptr};
        size_t iovecs_count{0};
        size_t offset{0};
    };

    struct unaligned_readv_response
    {
        size_t bytes_read{0};
        int32_t error_code{0};
    };

    struct unaligned_readv_mailbox : mailbox_base<unaligned_readv_mailbox>
    {
        unaligned_readv_mailbox(unaligned_readv_request req)
            : request(req) {}

        unaligned_readv_request request;
        unaligned_readv_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct write_request;
    struct write_response;
    struct write_mailbox;

    struct write_request
    {
        using response_t = write_response;
        using mailbox_t = write_mailbox;

        int fd;
        uint8_t* data;
        size_t size;
        size_t offset;
    };

    struct write_response
    {
        size_t bytes_written{0};
        int32_t error_code{0};
    };

    struct write_mailbox : mailbox_base<write_mailbox>
    {
        write_mailbox(write_request req)
            : request(req) {}

        write_request request;
        write_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct writev_request;
    struct writev_response;
    struct writev_mailbox;

    struct writev_request
    {
        using response_t = writev_response;
        using mailbox_t = writev_mailbox;

        int fd;
        iovec* iovecs;
        size_t iovecs_count;
        size_t offset;
    };

    struct writev_response
    {
        size_t bytes_written{0};
        int32_t error_code{0};
    };

    struct writev_mailbox : mailbox_base<writev_mailbox>
    {
        writev_mailbox(writev_request req)
            : request(req) {}

        writev_request request;
        writev_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };


    struct open_request;
    struct open_response;
    struct open_mailbox;

    struct open_request
    {
        using response_t = open_response;
        using mailbox_t = open_mailbox;

        std::string path;
        int32_t flags;
        mode_t mode{0777};
    };

    struct open_response
    {
        int32_t file_descriptor{};
        int32_t error_code{};
    };

    struct open_mailbox : mailbox_base<open_mailbox>
    {
        open_mailbox(open_request req) : request(std::move(req)) {}

        open_request request;
        open_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct fallocate_request;
    struct fallocate_response;
    struct fallocate_mailbox;

    struct fallocate_request
    {
        using response_t = fallocate_response;
        using mailbox_t = fallocate_mailbox;

        int32_t fd;
        mode_t mode;
        size_t offset;
        size_t length;
    };

    struct fallocate_response
    {
        int32_t error_code{};
    };

    struct fallocate_mailbox : mailbox_base<fallocate_mailbox>
    {
        fallocate_mailbox(fallocate_request req)
            : request(req) {}

        fallocate_request request;
        fallocate_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct close_request;
    struct close_response;
    struct close_mailbox;

    struct close_request
    {
        using response_t = close_response;
        using mailbox_t = close_mailbox;

        int32_t fd;
    };

    struct close_response
    {
        int32_t error_code{};
    };

    struct close_mailbox : mailbox_base<close_mailbox>
    {
        close_mailbox(close_request req)
            : request(req) {}

        close_request request;
        close_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct file_info_request;
    struct file_info_response;
    struct file_info_mailbox;

    struct file_info_request
    {
        using response_t = file_info_response;
        using mailbox_t = file_info_mailbox;

        std::string path;
    };

    struct file_info_response
    {
        bool exists{false};
        size_t file_size{0};
        int32_t error_code{0};
    };

    struct file_info_mailbox : mailbox_base<file_info_mailbox>
    {
        file_info_mailbox(file_info_request req)
            : request(std::move(req)) {}

        file_info_request request;
        file_info_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }

    private:
        struct statx _statx_buf
        {
        };
    };

    struct fsync_request;
    struct fsync_response;
    struct fsync_mailbox;

    struct fsync_request
    {
        using response_t = fsync_response;
        using mailbox_t = fsync_mailbox;

        int32_t fd;
    };

    struct fsync_response
    {
        int32_t error_code{};
    };

    struct fsync_mailbox : mailbox_base<fsync_mailbox>
    {
        fsync_mailbox(fsync_request req)
            : request(req) {}

        fsync_request request;
        fsync_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct ftruncate_request;
    struct ftruncate_response;
    struct ftruncate_mailbox;

    struct ftruncate_request
    {
        using response_t = ftruncate_response;
        using mailbox_t = ftruncate_mailbox;

        int32_t fd;
        size_t length;
    };

    struct ftruncate_response
    {
        int32_t error_code{};
    };

    struct ftruncate_mailbox : mailbox_base<ftruncate_mailbox>
    {
        ftruncate_mailbox(ftruncate_request req)
            : request(req) {}

        ftruncate_request request;
        ftruncate_response response;

        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    struct yield_request;
    struct yield_response;
    struct yield_mailbox;

    struct yield_request
    {
        using response_t = yield_response;
        using mailbox_t = yield_mailbox;
    };

    struct yield_response
    {
    };

    struct yield_mailbox : mailbox_base<yield_mailbox>
    {
        yield_request request;
        yield_response response{};

        yield_mailbox(yield_request) {}
        
        void prepare_sqe(io_uring_sqe* sqe);
        void handle_cqe(io_uring_cqe* cqe);

        void* get_response()
        {
            return &response;
        }
    };

    // NOLINTEND (*-readability-convert-member-functions-to-static)

    using mailbox_impls =
        std::variant<
            read_mailbox,
            unaligned_read_mailbox,
            unaligned_readv_mailbox,
            write_mailbox,
            writev_mailbox,
            open_mailbox,
            fallocate_mailbox,
            close_mailbox,
            file_info_mailbox,
            fsync_mailbox,
            yield_mailbox>;

} // namespace hedge::async