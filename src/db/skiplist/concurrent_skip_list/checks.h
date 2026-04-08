#pragma once

#ifndef FOLLY_LIKELY
#define FOLLY_LIKELY(x) __builtin_expect(!!(x), 1)
#endif

#ifndef FOLLY_UNLIKELY
#define FOLLY_UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif

#ifdef CHECKS_ENABLED

#include <iostream>
#include <ostream>
struct CheckStream
{
    bool condition;
    explicit CheckStream(bool cond) : condition(cond) {}
    template <typename T>
    CheckStream& operator<<(const T& msg)
    {
        if(!condition)
            std::cerr << msg;
        return *this;
    }
    ~CheckStream()
    {
        if(!condition)
        {
            std::cerr << std::endl;
            std::abort();
        }
    }
};
#ifndef DCHECK
#define DCHECK(x) CheckStream(!!(x))
#endif
#ifndef DCHECK_GT
#define DCHECK_GT(a, b) CheckStream((a) > (b))
#endif
#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) CheckStream((a) == (b))
#endif
#ifndef DCHECK_LE
#define DCHECK_LE(a, b) CheckStream((a) <= (b))
#endif
#ifndef DCHECK_LT
#define DCHECK_LT(a, b) CheckStream((a) < (b))
#endif

#else // CHECKS_ENABLED

struct NoopStream
{
    template <typename T>
    constexpr NoopStream operator<<(const T&) const { return {}; }
};
#ifndef DCHECK
#define DCHECK(x) \
    NoopStream {}
#endif
#ifndef DCHECK_GT
#define DCHECK_GT(a, b) \
    NoopStream {}
#endif
#ifndef DCHECK_EQ
#define DCHECK_EQ(a, b) \
    NoopStream {}
#endif
#ifndef DCHECK_LE
#define DCHECK_LE(a, b) \
    NoopStream {}
#endif
#ifndef DCHECK_LT
#define DCHECK_LT(a, b) \
    NoopStream {}
#endif

#endif // CHECKS_ENABLED
