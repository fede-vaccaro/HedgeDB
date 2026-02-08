#pragma once

#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "outcome.hpp"

namespace hedge
{

    enum class errc
    {
        GENERIC_ERROR = 0,
        VALUE_TABLE_NOT_ENOUGH_SPACE,
        KEY_NOT_FOUND,
        DELETED,
        BUSY,
        BUFFER_FULL,
    };

    class error
    {
        static inline std::string DEFAULT_MSG = "Unknown err";

        std::string _error_msg{DEFAULT_MSG};
        errc _error_code{errc::GENERIC_ERROR};

    public:
        explicit error(std::string msg, errc error_code = errc::GENERIC_ERROR) : _error_msg(std::move(msg)), _error_code(error_code) {}

        [[nodiscard]] const auto& to_string() const { return _error_msg; }
        [[nodiscard]] const auto& code() const { return _error_code; }
    };

    inline std::ostream& operator<<(std::ostream& os, const error& err)
    {
        os << err.to_string();
        return os;
    }

    inline std::error_code make_error_code(const error& err) { return {static_cast<int32_t>(err.code()), std::generic_category()}; }

    inline void outcome_throw_as_system_error_with_payload(error err)
    {
        outcome_v2::try_throw_std_exception_from_error(std::error_code(0, std::generic_category()));

        throw std::runtime_error(err.to_string());
    }

    template <typename T>
    using expected = outcome_v2::result<T, error>;

    struct status
    {
        using error_t = std::variant<std::monostate, hedge::error>;

        error_t err = std::monostate{};

        status() = default;
        status(hedge::error err_) : err(err_) {}

        operator bool() const
        {
            return std::holds_alternative<std::monostate>(err);
        }

        hedge::error error()
        {
            if(!*this)
                return std::get<hedge::error>(this->err);

            throw std::runtime_error("not an error, check operator bool() before calling this method");
        }
    };

    inline hedge::status ok()
    {
        return status{};
    }

} // namespace hedge
