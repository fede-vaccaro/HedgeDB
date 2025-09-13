
#include <cassert>
#include <chrono>
#include <coroutine>
#include <iostream>
#include <random>
#include <utility>
#include <chrono>

template <typename PROMISE>
struct Awaitable
{
    std::string name{};

    bool await_ready() const noexcept 
    {
        std::cout<< name << "::await_ready\n";
        return false; 
    }

    void await_suspend(std::coroutine_handle<PROMISE> h) const noexcept {
        std::cout << name << "::await_suspend on handle: " << h.address() << std::endl;
    }

    void await_resume() const noexcept {
        std::cout << name << "::await_resume\n";
    }
};

struct yield
{
    std::string name{};
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();

    auto static constexpr elapsed = std::chrono::seconds{2};

    bool await_ready() const noexcept 
    {
        std::cout<< name << "::await_ready\n";
        return false; 
    }

    void await_suspend(std::coroutine_handle<>) const noexcept {
        std::cout << name << "::await_suspend\n";
    }

    void await_resume() const noexcept {
        std::cout << name << "::await_resume\n";
    }
};

int counter = 0;

class Resumable
{

    struct Promise;
    using handle_t = std::coroutine_handle<Promise>;
    handle_t _handle;

    struct Promise
    {
        size_t value;
        int id = counter++;
        std::coroutine_handle<Promise> _continuation;
        std::coroutine_handle<Promise> _entry_handle;

        Resumable get_return_object()
        {
            auto h = handle_t::from_promise(*this);
            std::cout << id << "-Resumable::promise::get_return_object: " << h.address() << "\n";
            return Resumable{std::move(h)};
        }

        auto initial_suspend() { 
            std::cout << id << "-Resumable::promise::initial_suspend\n";
            return std::suspend_always{}; 
        }

        auto final_suspend() noexcept 
        {
            std::cout << id << "-Resumable::promise::final_suspend\n";
            
            struct Awaitable_{
                int id;
                bool await_ready() noexcept { return false; }
                void await_resume() noexcept {}
                std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept { // itself!
                    std::cout << id << "-Resumable::final_suspend::await_suspend on address " << h.address() << std::endl;
                    
                    if (auto& c = h.promise()._continuation;  h.promise()._entry_handle && !c.done())
                    {   
                        std::cout << "resuming continuation: " << c.address() << "\n";

                        if(c == h.promise()._entry_handle)
                            c.promise()._continuation = nullptr;

                        return c;
                    }

                    return std::noop_coroutine();
                }
            };
            return Awaitable_{id};
        }

        void return_value(size_t value) {
            std::cout << id << "-Resumable::promise::return_value " << value << "\n";
            this->value = value;
        }

        void unhandled_exception() {}
    };
        
    explicit Resumable(handle_t h) : _handle(h) {
    }
public:
    using promise_type = Promise;
    Resumable(Resumable&& r) : _handle{std::exchange(r._handle, {})} {}
    ~Resumable() { if(this->_handle) this->_handle.destroy(); }

    bool resume()
    {
        if(this->_handle.promise()._continuation && !this->_handle.promise()._continuation.done())
        {
            std::cout << this->_handle.promise().id << "-Resumable::resume::continuation address: " << this->_handle.promise()._continuation.address() << std::endl;
            this->_handle.promise()._continuation.resume();
        }
        else if(!this->_handle.done())
            this->_handle.resume();
        else
            std::cout << this->_handle.promise().value << std::endl;

        return !this->_handle.done();
    }

    bool await_ready() 
    {
        std::cout << this->_handle.promise().id << "-Resumable::await_ready\n";
        return false;
    }

    void await_suspend(std::coroutine_handle<Promise> caller_handle)
    {
        std::cout << this->_handle.promise().id <<  "-Resumable::await_suspend on handle: " << caller_handle.address() << std::endl;
        
        auto& caller_promise = caller_handle.promise();

        // caller_handle is entrypoint
        if(!caller_promise._entry_handle)
        {
            caller_promise._continuation = this->_handle;
            this->_handle.promise()._entry_handle = caller_handle;
        }

        // caller handle IS NOT entry point
        if(caller_promise._entry_handle)
        {
            this->_handle.promise()._entry_handle = caller_promise._entry_handle;
            caller_promise._entry_handle.promise()._continuation = this->_handle;
        }

        // where to return
        this->_handle.promise()._continuation = caller_handle;
        return;
    }

    size_t await_resume()
    {
        std::cout << this->_handle.promise().id << "-Resumable::await_resume\n";

        if(this->_handle.done())
        {   
            std::cout << this->_handle.promise().id << "-Resumable::done\n";        
            return this->_handle.promise().value;
        }

        return 0;
    }

};


Resumable coroutine_c()
{
    std::cout << "c1" << "\n";

    co_await Awaitable<Resumable::promise_type>("coroutine_c_awaitable"); // <---- HOW DO I BLOCK HERE????

    co_return 424242;
}

Resumable coroutine_b()
{
    std::cout << "b1" << "\n";

    auto x = co_await coroutine_c();

    std::cout << "b2 is: " << x << "\n";

    co_return 4242;
}


Resumable coroutine()
{
    std::cout << "Entering 'coroutine()'\n";

    std::cout << "a1" << "\n";
    auto x = co_await coroutine_b();

    std::cout << "a2 is: " << x << std::endl;

    std::cout << "a3" << "\n";
    co_return 42;
}

int main()
{
    auto coro = coroutine();

    std::cout << "resume1\n";

    coro.resume();

    std::cout << "resume2\n";

    coro.resume();

    std::cout << "resume3\n";

    coro.resume();

    std::cout << "resume4\n";

    coro.resume();

    std::cout << "resume5\n";

    coro.resume();

    std::cout << "resume6\n";

    coro.resume();

    std::cout << "finished!\n";

}