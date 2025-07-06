#pragma once

#include <cassert>
#include <coroutine>
#include <liburing.h>
#include <span>
#include <variant>

#include "mailbox_impl.h"

struct mailbox
{
private:
    template <typename REQUEST_T>
    friend std::unique_ptr<mailbox> from_request(REQUEST_T&& request);

    std::variant<read_mailbox, write_mailbox, multi_read_mailbox> _mailbox_impl;

public:
    template <typename MAILBOX_T>
    mailbox(MAILBOX_T&& impl) : _mailbox_impl(std::move(impl))
    {
    }

    auto needed_sqes()
    {
        return std::visit([](auto& impl)
                          { return impl.needed_sqes(); }, _mailbox_impl);
    }

    auto prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        return std::visit([sqes](auto& impl)
                          { return impl.prepare_sqes(sqes); }, _mailbox_impl);
    }

    auto handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx)
    {
        return std::visit([cqe, sub_request_idx](auto& impl)
                          { return impl.handle_cqe(cqe, sub_request_idx); }, _mailbox_impl);
    }

    void* get_response()
    {
        return std::visit([](auto& impl) -> void*
                          { return impl.get_response(); }, _mailbox_impl);
    }

    void set_continuation(std::coroutine_handle<> handle)
    {
        std::visit([handle](auto& impl)
                   { impl.set_continuation(handle); }, _mailbox_impl);
    }

    auto get_continuation() const
    {
        return std::visit([](const auto& impl)
                          { return impl.continuation; }, _mailbox_impl);
    }

    auto get_continuation_u64() const
    {
        return std::visit([](const auto& impl)
                          { return reinterpret_cast<uint64_t>(impl.continuation.address()); }, _mailbox_impl);
    }

    auto resume()
    {
        return std::visit([](auto& impl)
                          { return impl.resume(); }, _mailbox_impl);
    }
};

template <typename REQUEST_T>
std::unique_ptr<mailbox> from_request(REQUEST_T&& request)
{
    return std::make_unique<mailbox>(typename REQUEST_T::mailbox_t{std::forward<REQUEST_T&&>(request)});
}

template <typename RESPONSE_T>
struct awaitable_mailbox
{
    mailbox& mbox;

    bool await_ready() noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept
    {
        mbox.set_continuation(handle);
    }

    auto await_resume() noexcept
    {
        return std::move(*reinterpret_cast<RESPONSE_T*>(mbox.get_response()));
    }

    awaitable_mailbox(auto& m) : mbox(m)
    {
    }
};
