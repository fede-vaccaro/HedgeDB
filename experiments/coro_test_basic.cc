
#include <coroutine>
#include <iostream>
#include <utility>


struct Awaitable
{
    bool await_ready() const noexcept 
    {
        std::cout<<"await_ready\n";
        return false; 
    }

    void await_suspend(std::coroutine_handle<>) const noexcept {
        std::cout << "await_suspend\n";
    }

    void await_resume() const noexcept {
        std::cout << "await_resume\n";
    }
};


class Resumable
{

    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;

    struct Promise
    {
        size_t value;

        Resumable get_return_object()
        {
            std::cout << "get_return_object\n";
            return Resumable{handle_t::from_promise(*this)};
        }

        auto initial_suspend() { 
            std::cout << "initial suspend\n";
            return Awaitable{}; //
        }
        auto final_suspend() noexcept 
        {
            std::cout << "final suspend\n";
            return Awaitable{}; 
        }

        void return_value(size_t value) {
            std::cout << "return value\n";
            this->value = value;
        }

        void unhandled_exception() {}
    };
    
    handle_t _handle;
    
    explicit Resumable(handle_t h) : _handle(h) {}
public:
    using promise_type = Promise;
    Resumable(Resumable&& r) : _handle{std::exchange(r._handle, {})} {}
    ~Resumable() { if(this->_handle) this->_handle.destroy(); }

    bool resume()
    {
        if(!this->_handle.done())
            this->_handle.resume();
        else
            std::cout << this->_handle.promise().value << std::endl;

        return !this->_handle.done();
    }

    bool await_ready() const noexcept 
    {
        std::cout << "Resumable::await_ready\n";
        return true;
    }

    void await_suspend(std::coroutine_handle<>) const noexcept {
        std::cout << "Resumable::await_suspend\n";
    }

    void await_resume() const noexcept {
        std::cout << "Resumable::await_resume\n";
    }

};

class Eager
{

    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;

    struct Promise
    {
        size_t value;

        Eager get_return_object()
        {
            std::cout << "get_return_object\n";
            return Eager{handle_t::from_promise(*this)};
        }

        auto initial_suspend() { 
            std::cout << "initial suspend\n";
            return std::suspend_never{}; //
        }
        auto final_suspend() noexcept 
        {
            std::cout << "final suspend\n";
            return std::suspend_always{}; 
        }

        void return_value(size_t value) {
            std::cout << "return value\n";
            this->value = value;
        }

        void unhandled_exception() {}
    };
    
    handle_t _handle;
    
    explicit Eager(handle_t h) : _handle(h) {}
public:
    using promise_type = Promise;
    Eager(Eager&& r) : _handle{std::exchange(r._handle, {})} {}
    ~Eager() { if(this->_handle) this->_handle.destroy(); }

    bool resume()
    {
        if(!this->_handle.done())
            this->_handle.resume();
        else
            std::cout << this->_handle.promise().value << std::endl;

        return !this->_handle.done();
    }

    bool await_ready() const noexcept 
    {
        std::cout << "Eager::await_ready\n";
        return true;
    }

    void await_suspend(std::coroutine_handle<>) const noexcept {
        std::cout << "Eager::await_suspend\n";
    }

    void await_resume() const noexcept {
        std::cout << "Eager::await_resume\n";
    }

};

Eager nested()
{
    std::cout << "4\n";

    co_await Awaitable{};

    std::cout << "5\n";

    co_return 20;
}


Resumable coroutine()
{
    std::cout << "3" << "\n";

    co_await nested();

    std::cout << "6" << "\n";
    co_return 42;
}

int main()
{
    auto coro = coroutine();

    std::cout << "1" << std::endl;
    std::cout << "2" << std::endl;

    coro.resume();

    std::cout << "7" << std::endl;

    coro.resume();

    coro.resume();

    std::cout << "finished!\n";

}