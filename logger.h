#pragma once 

#include <iostream>
#include <sstream>

constexpr bool ENABLE_LOGGING = false;

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

    std::cout << "[!LOGGER!] " << oss.str() << std::endl;
}
