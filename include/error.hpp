#pragma once

#include <ostream>
#include <stdexcept>
#include <string>
#include <variant>

#include "outcome.hpp"

// Ensure the correct namespace alias for Outcome

namespace hedgehog
{

    enum class errc
    {
        GENERIC_ERROR = 0,
    };

    class error
    {
        static inline std::string kDefaultMsg = "Unknown error";

        std::string mErrorMsg{kDefaultMsg};
        errc mErrorCode{errc::GENERIC_ERROR};

    public:
        explicit error(const std::string& msg) : mErrorMsg(msg) {}

        [[nodiscard]] const auto& to_string() const { return mErrorMsg; }
        [[nodiscard]] const auto& code() const { return mErrorCode; }
    };

    inline std::ostream& operator<<(std::ostream& os, const error& err)
    {
        os << err.to_string();
        return os;
    }
    inline std::error_code make_error_code(const error&) { return {static_cast<int32_t>(errc::GENERIC_ERROR), std::generic_category()}; }

    inline void outcome_throw_as_system_error_with_payload(error err)
    {
        outcome_v2::try_throw_std_exception_from_error(std::error_code(0, std::generic_category()));

        throw std::runtime_error(err.to_string());
    }

    template <typename T>
    using expected = outcome_v2::result<T, error>;

    struct status
    {
        using error_t = std::variant<std::monostate, hedgehog::error>;

        error_t err = std::monostate{};

        status() = default;
        status(hedgehog::error err_) : err(err_) {}

        inline operator bool() const
        {
            return std::holds_alternative<std::monostate>(err);
        }

        inline hedgehog::error error()
        {
            if(!*this)
                return std::get<hedgehog::error>(this->err);

            throw std::runtime_error("not an error, check before calling this method!");

            return hedgehog::error{""};
        }
    };

    inline hedgehog::status ok()
    {
        return status{};
    }

} // namespace hedgehog
