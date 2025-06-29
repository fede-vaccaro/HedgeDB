#pragma once

#include <cassert>
#include <coroutine>
#include <liburing.h>
#include <variant>

#include "mailbox_impl.h"

struct mailbox
{
private:
    template <typename REQUEST_T>
    friend std::unique_ptr<mailbox> from_request(REQUEST_T&& request);

    std::variant<read_mailbox> _mailbox_impl;

public:
    template <typename MAILBOX_T>
    mailbox(MAILBOX_T&& impl) : _mailbox_impl(std::move(impl))
    {
    }

    bool prepare_sqes(io_uring* ring)
    {
        return std::visit([ring](auto& impl)
                          { return impl.prepare_sqes(ring); }, _mailbox_impl);
    }

    bool handle_cqe(io_uring_cqe* cqe)
    {
        return std::visit([cqe](auto& impl)
                          { return impl.handle_cqe(cqe); }, _mailbox_impl);
    }

    auto& get_response()
    {
        return std::visit([](auto& impl) -> auto&
                          { return impl.get_response(); }, _mailbox_impl);
    }

    void set_continuation(std::coroutine_handle<> handle)
    {
        std::visit([handle](auto& impl)
                   { impl.set_continuation(handle); }, _mailbox_impl);
    }

    std::coroutine_handle<> get_continuation() const
    {
        return std::visit([](const auto& impl)
                          { return impl.continuation; }, _mailbox_impl);
    }

    bool resume()
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
        return std::move(mbox.get_response());
    }

    awaitable_mailbox(auto& m) : mbox(m)
    {
    }
};
