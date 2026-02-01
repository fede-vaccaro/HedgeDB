#pragma once

#include <coroutine>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

namespace hedge::async
{
    template <typename GENERATOR, typename YIELD_VALUE>
    struct generator_promise
    {
        using reference_type = std::conditional_t<std::is_reference_v<YIELD_VALUE>, YIELD_VALUE, YIELD_VALUE&>;
        using value_type = std::remove_reference_t<YIELD_VALUE>;
        using handle_t = std::coroutine_handle<generator_promise>;

        value_type* _value;

        auto get_return_object()
        {
            return GENERATOR{handle_t::from_promise(*this)};
        }

        std::suspend_always initial_suspend() { return {}; }

        std::suspend_always final_suspend() noexcept { return {}; }

        void return_void() {}

        void unhandled_exception() { throw; }

        template <typename U = YIELD_VALUE,
                  std::enable_if_t<!std::is_rvalue_reference<U>::value, int> = 0>
        std::suspend_always yield_value(std::remove_reference_t<YIELD_VALUE>& value) noexcept
        {
            this->_value = std::addressof(value);
            return {};
        }

        std::suspend_always yield_value(std::remove_reference_t<YIELD_VALUE>&& value) noexcept
        {
            this->_value = std::addressof(value);
            return {};
        }

        reference_type value() const noexcept
        {
            return static_cast<reference_type>(*this->_value);
        }
    };

    template <typename GENERATOR, typename YIELD_VALUE>
    struct generator_iterator
    {
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = YIELD_VALUE;
        using pointer = YIELD_VALUE*;
        using reference = YIELD_VALUE&;

        using promise_t = typename GENERATOR::promise_type;

        std::coroutine_handle<promise_t> _coroutine;

        explicit generator_iterator(std::coroutine_handle<promise_t> coroutine) : _coroutine(std::exchange(coroutine, {}))
        {
        }

        generator_iterator& operator++()
        {
            this->_coroutine.resume();
            return *this;
        }

        bool operator!=(std::default_sentinel_t) const
        {
            return !this->_coroutine.done();
        }

        bool operator==(std::default_sentinel_t) const
        {
            return this->_coroutine.done();
        }

        reference operator*() const
        {
            return this->_coroutine.promise().value();
        }

        pointer operator->() const
        {
            return std::addressof(this->_coroutine.promise().value());
        }
    };

    template <typename YIELD_VALUE>
    class generator
    {
    public:
        using iterator = generator_iterator<generator, YIELD_VALUE>;
        using promise_type = generator_promise<generator, YIELD_VALUE>;

        generator(std::coroutine_handle<promise_type> h) noexcept : _coroutine(std::exchange(h, {}))
        {
        }

        ~generator()
        {
            if(_coroutine)
                _coroutine.destroy();
        }

        generator& operator=(generator other) noexcept;

    iterator begin()
        {
            this->_coroutine.resume();
            return iterator(this->_coroutine);
        }

        std::default_sentinel_t end() { return {}; };

    private:
        std::coroutine_handle<promise_type> _coroutine;
    };

} // namespace hedge::async