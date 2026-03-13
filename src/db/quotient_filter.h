#pragma once

#include <error.hpp>
#include <span>

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

        static hedge::expected<quotient_filter> load(const uint8_t* header_data,
                                                     const uint8_t* table_data,
                                                     size_t table_size);

        quotient_filter(quotient_filter&& other) noexcept
        {
            this->_qf_impl = std::exchange(other._qf_impl, {});
        }

        quotient_filter& operator=(quotient_filter&& other) noexcept
        {
            this->_free();

            this->_qf_impl = std::exchange(other._qf_impl, {});

            return *this;
        }

        explicit quotient_filter(const quotient_filter& other) = delete;
        quotient_filter& operator=(const quotient_filter& other) = delete;

        ~quotient_filter();

        bool insert(uint64_t hash);

        bool may_contain(uint64_t hash) const;

        bool remove(uint64_t hash);

        void clear();

        [[nodiscard]] std::span<const uint8_t> data_as_byte_span() const;

        [[nodiscard]] std::span<const uint8_t> header_as_byte_span() const;

    private:
        void _free();
    };
} // namespace hedge::db