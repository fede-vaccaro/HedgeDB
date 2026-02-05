#pragma once

#include <error.hpp>

#include "qf.h"

namespace hedge::db
{
    // C++ Wrapper for the quotient filter implementation
    class quotient_filter
    {
        third_party::quotient_filter _qf_impl{};

        quotient_filter() = default;

    public:
        static hedge::expected<quotient_filter> make(uint32_t q, uint32_t r);

        quotient_filter(quotient_filter&& other) noexcept
        {
            this->_qf_impl = std::exchange(other._qf_impl, {});
        }

        quotient_filter& operator=(quotient_filter&& other) noexcept
        {
            this->~quotient_filter();

            this->_qf_impl = std::exchange(other._qf_impl, {});

            return *this;
        }

        explicit quotient_filter(const quotient_filter& other) = delete;
        quotient_filter& operator=(const quotient_filter& other) = delete;

        ~quotient_filter();

        bool insert(uint64_t hash);

        bool may_contain(uint64_t hash);

        bool remove(uint64_t hash);

        void clear();
    };
} // namespace hedge::db