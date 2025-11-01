#pragma once

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

        uint32_t needed_sqes()
        {
            return static_cast<Derived*>(this)->needed_sqes();
        }

        void prepare_sqes(std::span<io_uring_sqe*> sqes)
        {
            assert(sqes.size() == static_cast<size_t>(this->needed_sqes()) && "sqes span size does not match needed sqes");
            return static_cast<Derived*>(this)->prepare_sqes(sqes);
        };

        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx)
        {
            return static_cast<Derived*>(this)->handle_cqe(cqe, sub_request_idx);
        }

        void* get_response() // todo: use &response as default otherwise call get_response() on the derived class (implement through SFINAE)
        {
            return static_cast<Derived*>(this)->get_response();
        }

        void set_continuation(std::coroutine_handle<> handle)
        {
            this->continuation = handle;
        }

        auto resume()
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
                return true;

            this->continuation.resume();

            return this->continuation.done();
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
        size_t offset{0};
        size_t size{0};
    };

    struct read_response
    {
        std::unique_ptr<uint8_t> data{};
        size_t bytes_read{0};
        int32_t error_code{0};
    };

    struct read_mailbox : mailbox_base<read_mailbox>
    {
        read_mailbox(read_request req)
            : request(req) {}

        read_request request;
        read_response response;

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

        void* get_response()
        {
            return &response;
        }
    };

    struct multi_read_request;
    struct multi_read_response;
    struct multi_read_mailbox;

    struct multi_read_request
    {
        using response_t = multi_read_response;
        using mailbox_t = multi_read_mailbox;

        std::vector<read_request> requests;
    };

    struct multi_read_response
    {
        std::vector<read_response> responses;
    };

    struct multi_read_mailbox : mailbox_base<multi_read_mailbox>
    {
        multi_read_mailbox(multi_read_request req)
            : request(std::move(req)) {}

        multi_read_request request;
        multi_read_response response;

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return static_cast<uint32_t>(request.requests.size());
        }

        void* get_response()
        {
            return &response;
        }

    private:
        uint64_t _landed_response{0};
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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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

        void prepare_sqes(std::span<io_uring_sqe*> sqes);
        bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx);

        uint32_t needed_sqes()
        {
            return 1;
        }

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
            write_mailbox,
            multi_read_mailbox,
            open_mailbox,
            fallocate_mailbox,
            close_mailbox,
            file_info_mailbox,
            fsync_mailbox>;

} // namespace hedge::async