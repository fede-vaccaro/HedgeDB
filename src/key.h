#pragma once

#include <algorithm>
#include <array>
#include <compare>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <span>
#include <sstream>
#include <string_view>
#include <sys/types.h>
#include <type_traits>
#include <utility>

namespace hedge
{
    std::string to_hex_string(std::span<const uint8_t> key);

    // key is a custom immutable string type implementing short string optimization up to 31 inlined bytes
    template <size_t MAX_INLINE_LENGTH = 31>
        requires(MAX_INLINE_LENGTH < 128)
    struct key
    {
    private:
        static constexpr size_t LONG_STRING_FLAG = 0x80;
        constexpr static size_t LAST_BYTE_IDX = MAX_INLINE_LENGTH;

        static uint8_t& _flag(key& k)
        {
            static_assert(sizeof(key) == 32);
            return reinterpret_cast<uint8_t*>(&k)[LAST_BYTE_IDX];
        }

        static uint8_t _flag_byte(const key& k)
        {
            static_assert(sizeof(key) == 32);
            return reinterpret_cast<const uint8_t*>(&k)[LAST_BYTE_IDX];
        }

        // The last byte of the Key struct represents:
        // - If the MSB == 1, it flags that the represented string is long
        // - If the MSB == 0, the key is inlined and it also represents the short string length
        static void _set_flag(key& k, uint8_t v)
        {
            static_assert(sizeof(key) == 32);
            key::_flag(k) = v;
        }

        static bool _is_long_str(const key& k)
        {
            return (key::_flag_byte(k) & LONG_STRING_FLAG) != 0;
        }

        static void _long_str_copy_unsafe(key& dest, const key& src)
        {
            key::_set_flag(dest, LONG_STRING_FLAG);

            const auto len = src._storage.variable.long_key_length;

            dest._storage.variable.buf = new uint8_t[len];
            dest._storage.variable.long_key_length = len;

            std::memcpy(dest._storage.variable.buf, src._storage.variable.buf, len);
        }

        static void _short_str_copy_unsafe(key& dest, const key& src)
        {
            std::memcpy((void*)&dest, (void*)&src, sizeof(key));
        }

        static void _free_if_long_str(key& k)
        {
            if(key::_is_long_str(k) && k._storage.variable.buf != nullptr) [[unlikely]] // Just for trying moving the needle towards the inlined string path
            {
                delete[] k._storage.variable.buf;
                key::_set_flag(k, 0);
                k._storage.variable.buf = nullptr;
            }
        }

        template <bool STORAGE_INITIALIZED>
        static key& _move(key& dst, key&& src)
        {
            if constexpr(STORAGE_INITIALIZED)
                key::_free_if_long_str(dst);

            if(key::_is_long_str(src))
            {
                dst._storage.variable.buf = std::exchange(src._storage.variable.buf, nullptr);
                dst._storage.variable.long_key_length = std::exchange(src._storage.variable.long_key_length, 0UL);
                key::_set_flag(dst, LONG_STRING_FLAG);
                key::_set_flag(src, 0); // Reset other's flag
            }
            else
            {
                key::_short_str_copy_unsafe(dst, src);
            }

            return dst;
        }

        template <bool STORAGE_INITIALIZED>
        static key& _copy(key& dst, const key& src)
        {
            if constexpr(STORAGE_INITIALIZED)
                key::_free_if_long_str(dst);

            if(key::_is_long_str(src)) [[unlikely]]
                key::_long_str_copy_unsafe(dst, src);
            else
                key::_short_str_copy_unsafe(dst, src);

            return dst;
        }

        static void _init(key& dst, const void* ptr, size_t len)
        {
            // Short string optimization
            if(len <= MAX_INLINE_LENGTH) [[likely]]
            {
                dst._storage.fixed.short_key_length = static_cast<uint8_t>(len);
                std::memcpy(dst._storage.fixed.buf.data(), ptr, len);
                return;
            }

            // Is long string
            key::_set_flag(dst, LONG_STRING_FLAG);
            dst._storage.variable.buf = new uint8_t[len];
            dst._storage.variable.long_key_length = len;
            std::memcpy(dst._storage.variable.buf, ptr, len);
        }

        struct long_key_t
        {
            uint8_t* buf;
            size_t long_key_length;
            // size_t capacity; // There is room for "capacity" field here and reusing the allocated memory over assignment of shorter (long) key
        };

        struct short_key_t
        {
            std::array<uint8_t, MAX_INLINE_LENGTH> buf;
            uint8_t short_key_length;
        };

        union key_data
        {
            long_key_t variable;
            short_key_t fixed;
        };

        key_data _storage;

        explicit key(size_t /* private */)
        {
            // This constructor is only used in the static make_with_length factory method,
            // which initializes the key in-place, so we can skip zero-initialization of the storage here
        }

    public:
        static size_t constexpr capacity()
        {
            return MAX_INLINE_LENGTH;
        }

        key()
        {
            std::memset((void*)this, 0, sizeof(key));
        }

        key(key&& other) noexcept // Move ctor is not that trivial, be careful with performance
        {
            key::_move<false>(*this, std::move(other));
        }

        key(const key& other)
        {
            key::_copy<false>(*this, other);
        }

        key(const void* ptr, size_t len)
        {
            key::_init(*this, ptr, len);
        }

        key(std::span<const uint8_t> span)
        {
            key::_init(*this, span.data(), span.size());
        }

        key(std::string_view sv)
        {
            key::_init(*this, sv.data(), sv.length());
        }

        static key make_with_length(size_t len)
        {
            key k(0UL); // Private constructor that doesn't initialize storage, since we'll do it in-place in the factory method
            if(len <= MAX_INLINE_LENGTH) [[likely]]
            {
                k._storage.fixed.short_key_length = static_cast<uint8_t>(len);
            }
            else
            {
                key::_set_flag(k, LONG_STRING_FLAG);
                k._storage.variable.buf = new uint8_t[len];
                k._storage.variable.long_key_length = len;
            }
            return k;
        }

        key& operator=(key&& other) noexcept
        {
            return key::_move<true>(*this, std::move(other));
        }

        key& operator=(const key& other)
        {
            if(this == &other)
            {
                return *this;
            }
            return key::_copy<true>(*this, other);
        }

        // Convert to std::string_view, then perform every needed operation
        // TODO: That's convenient for sure, but what's faster between this
        // and "unrolling" every combination?
        // Or maybe branch-free?
        operator std::string_view() const
        {
            std::array<const char*, 2> ptrs{(char*)this->_storage.fixed.buf.data(), (char*)this->_storage.variable.buf};
            std::array<size_t, 2> sizes{static_cast<size_t>(key::_flag_byte(*this)), this->_storage.variable.long_key_length};

            const auto idx = static_cast<size_t>(key::_is_long_str(*this));
            return {ptrs[idx], sizes[idx]};
        }

        operator std::span<const uint8_t>() const
        {
            std::array<const uint8_t*, 2> ptrs{this->_storage.fixed.buf.data(), this->_storage.variable.buf};
            std::array<size_t, 2> sizes{static_cast<size_t>(key::_flag_byte(*this)), this->_storage.variable.long_key_length};

            const auto idx = static_cast<size_t>(key::_is_long_str(*this));
            return {ptrs[idx], sizes[idx]};
        }

        operator std::span<uint8_t>()
        {
            std::array<uint8_t*, 2> ptrs{this->_storage.fixed.buf.data(), this->_storage.variable.buf};
            std::array<size_t, 2> sizes{static_cast<size_t>(key::_flag_byte(*this)), this->_storage.variable.long_key_length};

            const auto idx = static_cast<size_t>(key::_is_long_str(*this));
            return {ptrs[idx], sizes[idx]};
        }

        [[nodiscard]] std::span<const uint8_t> as_bytes() const
        {
            return static_cast<std::span<const uint8_t>>(*this);
        }

        std::span<uint8_t> as_bytes()
        {
            return static_cast<std::span<uint8_t>>(*this);
        }

        auto operator<=>(const key& other) const
        {
            return static_cast<std::string_view>(*this) <=> static_cast<std::string_view>(other);
        }

        bool operator==(const key& other) const
        {
            return static_cast<std::string_view>(*this) == static_cast<std::string_view>(other);
        }

        bool operator!=(const key& other) const
        {
            return static_cast<std::string_view>(*this) != static_cast<std::string_view>(other);
        }

        bool operator<(const key& other) const
        {
            return static_cast<std::string_view>(*this) < static_cast<std::string_view>(other);
        }

        bool operator<=(const key& other) const
        {
            return static_cast<std::string_view>(*this) <= static_cast<std::string_view>(other);
        }

        bool operator>=(const key& other) const
        {
            return static_cast<std::string_view>(*this) >= static_cast<std::string_view>(other);
        }

        [[nodiscard]] uint8_t* data()
        {
            // Branch-free
            std::array<uint8_t*, 2> ptrs{this->_storage.fixed.buf.data(), this->_storage.variable.buf};
            return ptrs[static_cast<size_t>(key::_is_long_str(*this))];
        }

        [[nodiscard]] const uint8_t* data() const
        {
            // Branch-free
            std::array<const uint8_t*, 2> ptrs{this->_storage.fixed.buf.data(), this->_storage.variable.buf};
            return ptrs[static_cast<size_t>(key::_is_long_str(*this))];
        }

        [[nodiscard]] size_t size() const
        {
            // Branch-free
            std::array<size_t, 2> sizes{static_cast<size_t>(key::_flag_byte(*this)), this->_storage.variable.long_key_length};
            return sizes[static_cast<size_t>(key::_is_long_str(*this))];
        }

        ~key()
        {
            // std::cout << "~key()\n";
            key::_free_if_long_str(*this);
        }
    };

    inline std::string to_hex_string(std::span<const uint8_t> key)
    {
        if(key.empty())
            return "";

        std::stringstream ss;
        ss << std::hex << std::setfill('0');

        for(size_t i = 0; i < key.size(); ++i)
        {
            // 1. Format the byte as 2-digit hex
            ss << std::setw(2) << static_cast<int>(key[i]);

            // 2. Add the dash ONLY if it's not the last byte
            if(i < key.size() - 1 && (i % 2) == 1)
            {
                ss << "-";
            }
        }

        return ss.str();
    }

    inline uint8_t encode_key_size(size_t key_size)
    {
        return static_cast<uint8_t>(key_size - 1);
    }

    inline size_t decode_key_size(uint8_t encoded_key_size)
    {
        return static_cast<size_t>(encoded_key_size) + 1;
    }

    template <typename T>
        requires std::is_same_v<std::remove_cv_t<T>, std::span<const uint8_t>> ||
                 std::is_same_v<std::remove_cv_t<T>, key<>>
    inline void write_key_unsafe(uint8_t* dst, const T& k)
    {
        dst[0] = encode_key_size(k.size());
        std::ranges::copy(std::span<const uint8_t>(k.data(), k.size()), dst + 1);
    }

    inline key<> read_key_unsafe(const uint8_t* src)
    {
        const size_t key_length = decode_key_size(src[0]);
        auto k = key<>::make_with_length(key_length);
        std::ranges::copy(std::span<const uint8_t>(src + 1, key_length), k.data());
        return k;
    }

    constexpr size_t key_len_bytes = 1;

    inline size_t serialized_key_total_length(const key<>& k)
    {
        return k.size() + key_len_bytes;
    }

    inline size_t deserialized_key_total_length(size_t l)
    {
        return l - key_len_bytes;
    }

} // namespace hedge

namespace std
{
    template <>
    struct hash<hedge::key<>>
    {
        size_t operator()(const hedge::key<>& k) const
        {
            return std::hash<std::string_view>{}(static_cast<std::string_view>(k));
        }
    };
} // namespace std