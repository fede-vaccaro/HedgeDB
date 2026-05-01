#pragma once
#include "types.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <xxh64.hpp>

#include "common.h"

namespace hedge::db
{
    inline key_t make_key(size_t i)
    {
        uint64_t h = xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED);
        key_t k = key_t::make_with_length(KEY_SIZE);
        auto span = k.as_bytes();
        std::memset(span.data(), 0, KEY_SIZE);
        std::memcpy(span.data(), &h, std::min(sizeof(h), KEY_SIZE));
        return k;
    }

    inline size_t value_slot(size_t i)
    {
        return xxh64::hash(reinterpret_cast<const char*>(&i), sizeof(i), KEY_SEED) % NVALUES;
    }

    inline values_t pregenerate_values(size_t vsize)
    {
        values_t values(NVALUES);
        for(size_t slot = 0; slot < NVALUES; ++slot)
        {
            values[slot].resize(vsize);
            std::mt19937 gen(static_cast<uint32_t>(slot));
            std::uniform_int_distribution<uint8_t> dist(0, 255);
            for(auto& b : values[slot])
                b = static_cast<std::byte>(dist(gen));
        }
        return values;
    }

    inline uint64_t xorshift64(uint64_t& state)
    {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

} // namespace hedge::db
