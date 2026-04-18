#pragma once

#include <memory>
#include <stdexcept>
#include <xxhash.h>

namespace hedge::third_party
{
    uint64_t xxh64(const void* input, size_t length, uint64_t seed)
    {
        return XXH64(input, length, seed);
    }

    XXH128_hash_t xxh128(const void* input, size_t length, uint64_t seed)
    {
        return XXH3_128bits_withSeed(input, length, seed);
    }

    uint64_t xxh3(const void* input, size_t length)
    {
        return XXH3_64bits(input, length);
    }

    // Stateful hasher from xxhash library
    class hasher64
    {
        struct _hasher_free_fn
        {

            void operator()(XXH64_state_t* ptr)
            {
                int32_t res = XXH64_freeState(ptr);
                if(res != 0) [[unlikely]]
                    throw std::runtime_error("error freeing hasher");
            }
        };

        using hasher_t = std::unique_ptr<XXH64_state_t, _hasher_free_fn>;

        std::unique_ptr<XXH64_state_t, _hasher_free_fn> _hasher = hasher_t(XXH64_createState());

        void _reset()
        {
            constexpr uint64_t SEED = 0x9E3779B97F4A7C15ULL;
            int32_t res = XXH64_reset(this->_hasher.get(), SEED);
            if(res != 0) [[unlikely]]
                throw std::runtime_error("error resetting hasher");
        }

    public:
        explicit hasher64()
        {
            this->_reset();
        }

        void update(const void* input, size_t length)
        {
            int32_t res = XXH64_update(this->_hasher.get(), input, length);
            if(res != 0) [[unlikely]]
                throw std::runtime_error("error updating hasher");
        }

        uint64_t sum()
        {
            uint64_t hash = XXH64_digest(this->_hasher.get());
            this->_reset();
            return hash;
        }
    };

} // namespace hedge::third_party