#include <cassert>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <iostream>
#include <mutex>
#include <random>
#include <utility>
#include <chrono>
#include <exception>
#include <variant>

#undef __cpp_lib_semaphore

namespace detail {

template <typename T>
class SyncWaitTask { // A helper class only used by sync_wait()

  struct Promise {
    SyncWaitTask get_return_object() noexcept { return SyncWaitTask{*this}; }
    auto initial_suspend() noexcept { return std::suspend_always{}; }
    void unhandled_exception() noexcept { error_ = std::current_exception(); }
    auto yield_value(T&& x) noexcept { // Result has arrived
      value_ = std::addressof(x);
      return final_suspend();
    }

    auto final_suspend() noexcept {
      struct Awaitable {
        bool await_ready() noexcept { return false; }
        void await_suspend(std::coroutine_handle<Promise> h) noexcept {

#if defined(__cpp_lib_semaphore)
          h.promise().semaphore_.release(); // Signal!
#else
          {
            auto lock = std::unique_lock{h.promise().mtx_};
            h.promise().ready_ = true;
          }
          h.promise().cv_.notify_one();
#endif
        }
        void await_resume() noexcept {}
      };
      return Awaitable{};
    }

    void return_void() noexcept { assert(false); }

    T* value_ = nullptr;
    std::exception_ptr error_;

#if defined(__cpp_lib_semaphore)
    std::binary_semaphore semaphore_{2};
#else
    bool ready_{false};
    std::mutex mtx_;
    std::condition_variable cv_;
#endif
  };

  std::coroutine_handle<Promise> h_;
  explicit SyncWaitTask(Promise& p) noexcept
      : h_{std::coroutine_handle<Promise>::from_promise(p)} {}

public:
  using promise_type = Promise;
  SyncWaitTask(SyncWaitTask&& t) noexcept : h_{std::exchange(t.h_, {})} {}
  ~SyncWaitTask() {
    if (h_)
      h_.destroy();
  }

  // Called from sync_wait(). Will block and retrieve the
  // value or error from the task passed to sync_wait()
  T&& get() {
    auto& p = h_.promise();
    h_.resume();

#if defined(__cpp_lib_semaphore)
    p.semaphore_.acquire(); // Block until signal
#else
    {
      auto lock = std::unique_lock{p.mtx_};
      p.cv_.wait(lock, [&p] { return p.ready_; });
    }
#endif

    if (p.error_)
      std::rethrow_exception(p.error_);
    return static_cast<T&&>(*p.value_);
  }
};

} // namespace detail

template <typename T>
using Result = decltype(std::declval<T&>().await_resume());

template <typename T>
Result<T> sync_wait(T&& task) {
  if constexpr (std::is_void_v<Result<T>>) {
    struct Empty {};
    auto coro = [&]() -> detail::SyncWaitTask<Empty> {
      co_await std::forward<T>(task);
      co_yield Empty{};
      assert(false); // Coroutine will be destroyed
    };               // before it has a chance to return.
    (void)coro().get();
    return; // Result<T> is void
  } else {
    auto coro = [&]() -> detail::SyncWaitTask<Result<T>> {
      co_yield co_await std::forward<T>(task);
      assert(false);
    };
    return coro().get(); // Rerturn value
  }
}

template <typename T>
class [[nodiscard]] Task {

  struct Promise {
    std::variant<std::monostate, T, std::exception_ptr> result_;
    std::coroutine_handle<> continuation_; // Awaiting coroutine
    auto get_return_object() noexcept { return Task{*this}; }
    void return_value(T value) {
      result_.template emplace<1>(std::move(value));
    }
    void unhandled_exception() noexcept {
      result_.template emplace<2>(std::current_exception());
    }
    auto initial_suspend() { return std::suspend_always{}; }
    auto final_suspend() noexcept {
      struct Awaitable {
        bool await_ready() noexcept { return false; }
        auto await_suspend(std::coroutine_handle<Promise> h) noexcept {
          return h.promise().continuation_;
        }
        void await_resume() noexcept {}
      };
      return Awaitable{};
    }
  };

  std::coroutine_handle<Promise> h_;
  explicit Task(Promise & p) noexcept
      : h_{std::coroutine_handle<Promise>::from_promise(p)} {}

public:
  using promise_type = Promise;
  Task(Task && t) noexcept : h_{std::exchange(t.h_, {})} {}
  ~Task() {
    if (h_)
      h_.destroy();
  }

  // Awaitable interface
  bool await_ready() { return false; }
  auto await_suspend(std::coroutine_handle<> c) {
    h_.promise().continuation_ = c;
    return h_;
  }
  T await_resume() {
    auto&& r = h_.promise().result_;
    if (r.index() == 1) {
      return std::get<1>(std::move(r));
    } else {
      std::rethrow_exception(std::get<2>(std::move(r)));
    }
  }
};

// Template Specialization for Task<void>
template <>
class [[nodiscard]] Task<void> {

  struct Promise {
    std::exception_ptr e_;
    std::coroutine_handle<> continuation_; // Awaiting coroutine
    auto get_return_object() noexcept { return Task{*this}; }
    void return_void() {}
    void unhandled_exception() noexcept { e_ = std::current_exception(); }
    auto initial_suspend() { return std::suspend_always{}; }
    auto final_suspend() noexcept {
      struct Awaitable {
        bool await_ready() noexcept { return false; }
        auto await_suspend(std::coroutine_handle<Promise> h) noexcept {
          return h.promise().continuation_;
        }
        void await_resume() noexcept {}
      };
      return Awaitable{};
    }
  };

  std::coroutine_handle<Promise> h_;
  explicit Task(Promise & p) noexcept
      : h_{std::coroutine_handle<Promise>::from_promise(p)} {}

public:
  using promise_type = Promise;

  Task(Task && t) noexcept : h_{std::exchange(t.h_, {})} {}
  ~Task() {
    if (h_)
      h_.destroy();
  }

  // Awaitable interface
  bool await_ready() { return false; }
  auto await_suspend(std::coroutine_handle<> c) {
    h_.promise().continuation_ = c;
    return h_;
  }
  void await_resume() {
    if (h_.promise().e_)
      std::rethrow_exception(h_.promise().e_);
  }
};

auto width() -> Task<int> { co_return 20; }
auto height() -> Task<int> { co_return 30; }
auto area() -> Task<int> { co_return co_await width() * co_await area(); }

int main()
{
    auto a = area();
    
}