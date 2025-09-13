#pragma once

#include <cassert>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include <liburing.h>

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

    void* get_response() // todo: use &response as default otherwise call get_response() on the derived class
    {
        return static_cast<Derived*>(this)->get_response();
    }

    void set_continuation(std::coroutine_handle<> handle)
    {
        this->continuation = handle;
    }

    auto resume()
    {
        if(this->continuation.done())
            return true;

        assert(continuation && !this->continuation.done() && "resume called without a continuation set");

        this->continuation.resume();

        return this->continuation.done();
    }
};

struct read_response;
struct read_mailbox;

struct read_request
{
    using response_t = read_response;
    using mailbox_t = read_mailbox;

    int fd{-1};
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
        : request(std::move(req)) {}

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

struct write_response;
struct write_mailbox;

struct write_request // todo: template for more containers? std::string, std::vector<std::byte>, etc.
{
    using response_t = write_response;
    using mailbox_t = write_mailbox;

    int fd;
    std::vector<uint8_t> data;
    size_t offset;
};

struct write_response
{
    int32_t error_code{0};
};

struct write_mailbox : mailbox_base<write_mailbox>
{
    write_mailbox(write_request req)
        : request(std::move(req)) {}

    write_request request;
    write_response response;

    void prepare_sqes(std::span<io_uring_sqe*> sqes);
    bool handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx );

    uint32_t needed_sqes()
    {
        return 1;
    }

    void* get_response()
    {
        return &response;
    }
};

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