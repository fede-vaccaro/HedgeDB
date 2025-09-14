#pragma once

#include <iostream>
#include <sstream>
#include <utility>

constexpr bool ENABLE_LOGGING = true;

template <typename... Args>
void log(const Args&... args)
{
    if(!ENABLE_LOGGING)
        return;

    std::ostringstream oss;
    (oss << ... << args);

    std::cout << "[LOGGER] " << oss.str() << std::endl;
}

template <typename... Args>
void log_always(const Args&... args)
{
    std::ostringstream oss;
    (oss << ... << args);

    std::cout << "[LOGGER] " << oss.str() << std::endl;
}

class logger
{
public:
    explicit logger(std::string  name) : _name(std::move(name)) {}

    template <typename... Args>
    void log(const Args&... args) const
    {
        if(!ENABLE_LOGGING)
            return;

        std::ostringstream oss;
        (oss << ... << args);

        std::cout << "[" << this->_name << "] " << oss.str() << std::endl;
    }

private:
    std::string _name;
};