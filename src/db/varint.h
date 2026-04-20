#pragma once

#include <cstdint>
#include <span>
#include <type_traits>

#include <error.hpp>

namespace hedge
{

    constexpr size_t MAX_VARINT_LENGTH_32 = 5;
    constexpr size_t MAX_VARINT_LENGTH_64 = 10;
    constexpr int32_t VALUE_MASK = 0x7f;
    constexpr int32_t CONTINUATION_BIT = 0x80;

    // Encode value as varint (implementation from protobuf)
    template <typename T>
        requires std::is_unsigned_v<T>
    std::byte* unsafe_varint(T value, std::byte* ptr)
    {
        while(value >= CONTINUATION_BIT) [[likely]]
        {
            *ptr = static_cast<std::byte>(value | CONTINUATION_BIT);
            value >>= 7;
            ++ptr;
        }
        *ptr++ = static_cast<std::byte>(value);
        return ptr;
    }

    // Returns how many bytes are needed for encoding value as varint
    template <typename T>
        requires std::is_unsigned_v<T>
    uint32_t varint_length(T value)
    {
        if(value == T{})
            return 1U;

        return (std::bit_width(value) + 6U) / 7U;
    }

    // Decode from varint to uint64 (implementation from folly)
    template <class T>
        requires std::is_same_v<std::remove_cv_t<T>, char> ||
                 std::is_same_v<std::remove_cv_t<T>, unsigned char> ||
                 std::is_same_v<std::remove_cv_t<T>, std::byte>
    inline hedge::expected<std::pair<uint64_t, size_t>> try_decode_varint(const std::span<T>& data) // NOLINT: clangtidy(readability-function-cognitive-complexity)
    {
        const auto* begin = reinterpret_cast<const std::byte*>(data.begin().base());
        const auto* end = reinterpret_cast<const std::byte*>(data.end().base());
        const std::byte* p = begin;
        uint64_t val = 0;

        // end is always greater than or equal to begin, so this subtraction is safe
        if(size_t(end - begin) >= MAX_VARINT_LENGTH_64) [[likely]]
        { // fast path
            int64_t b;
            do
            {
                b = static_cast<int64_t>(*p++);
                val = (b & VALUE_MASK);
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 7;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 14;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 21;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 28;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 35;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 42;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 49;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & VALUE_MASK) << 56;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                b = static_cast<int64_t>(*p++);
                val |= (b & 0x01) << 63;
                if((b & CONTINUATION_BIT) == 0)
                {
                    break;
                }
                return hedge::error("too many bytes");
            } while(false);
        }
        else
        {
            int shift = 0;
            while(p != end && (static_cast<uint8_t>(*p) & CONTINUATION_BIT))
            {
                val |= static_cast<uint64_t>(static_cast<uint8_t>(*p++) & VALUE_MASK) << shift;
                shift += 7;
            }
            if(p == end)
            {
                return hedge::error("too few bytes");
            }
            val |= static_cast<uint64_t>(static_cast<uint8_t>(*p++)) << shift;
        }

        return {val, std::distance(begin, p)};
    }

} // namespace hedge