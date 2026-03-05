#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <emmintrin.h>
#include <new>
#include <thread>

namespace folly
{

    namespace detail
    {

        inline void asm_volatile_pause() noexcept
        {
            _mm_pause();
        }

        /*
         * A helper object for the contended case. Starts off with eager
         * spinning, and falls back to sleeping for small quantums.
         */
        class Sleeper
        {
            const std::chrono::nanoseconds delta;

            static constexpr uint32_t kMaxActiveSpin = 4096;
            // static constexpr bool useBackOff = kIsArchAArch64 && !kIsMobile;
            static constexpr bool useBackOff = true;

            uint32_t spinCount = 0;

            uint32_t spinCountTarget = 1;

        public:
            static constexpr std::chrono::nanoseconds kMinYieldingSleep =
                std::chrono::microseconds(500);

            constexpr Sleeper() noexcept : delta(kMinYieldingSleep) {}

            explicit Sleeper(std::chrono::nanoseconds d) noexcept : delta(d) {}

            void wait() noexcept
            {
                bool doSpin = useBackOff
                                  ? spinCountTarget <= kMaxActiveSpin
                                  : spinCount < kMaxActiveSpin;
                if(doSpin)
                {
                    if constexpr(useBackOff)
                    {
                        do
                        {
                            asm_volatile_pause();
                        } while(++spinCount < spinCountTarget);
                        spinCountTarget <<= 1;
                    }
                    else
                    {
                        ++spinCount;
                        asm_volatile_pause();
                    }
                }
                else
                {
                    /* sleep override */
                    std::this_thread::sleep_for(delta);
                }
            }
        };

    } // namespace detail

    /*
     * A really, *really* small spinlock for fine-grained locking of lots
     * of teeny-tiny data.
     *
     * Zero initializing these is guaranteed to be as good as calling
     * init(), since the free state is guaranteed to be all-bits zero.
     *
     * This class should be kept a POD, so we can used it in other packed
     * structs (gcc does not allow __attribute__((__packed__)) on structs that
     * contain non-POD data).  This means avoid adding a constructor, or
     * making some members private, etc.
     */
    struct MicroSpinLock
    {
        enum
        {
            FREE = 0,
            LOCKED = 1
        };
        // lock_ can't be std::atomic<> to preserve POD-ness.
        uint8_t lock_;

        // Initialize this MSL.  It is unnecessary to call this if you
        // zero-initialize the MicroSpinLock.
        void init() noexcept { payload()->store(FREE); }

        bool try_lock() noexcept
        {
            bool ret = xchg(LOCKED) == FREE;
            return ret;
        }

        void lock() noexcept
        {
            detail::Sleeper sleeper;
            while(xchg(LOCKED) != FREE)
            {
                do
                {
                    sleeper.wait();
                } while(payload()->load(std::memory_order_relaxed) == LOCKED);
            }
            assert(payload()->load() == LOCKED);
        }

        void unlock() noexcept
        {
            assert(payload()->load() == LOCKED);
            payload()->store(FREE, std::memory_order_release);
        }

    private:
        std::atomic<uint8_t>* payload() noexcept
        {
            return reinterpret_cast<std::atomic<uint8_t>*>(&this->lock_);
        }

        uint8_t xchg(uint8_t newVal) noexcept
        {
            return std::atomic_exchange_explicit(
                payload(), newVal, std::memory_order_acq_rel);
        }
    };

    static_assert(
        std::is_standard_layout<MicroSpinLock>::value &&
            std::is_trivial<MicroSpinLock>::value,
        "MicroSpinLock must be kept a POD type.");

    //////////////////////////////////////////////////////////////////////

    /**
     * Array of spinlocks where each one is padded to prevent false sharing.
     * Useful for shard-based locking implementations in environments where
     * contention is unlikely.
     */

    template <class T, size_t N>
    struct alignas(std::max_align_t) SpinLockArray
    {
        T& operator[](size_t i) noexcept { return data_[i].lock; }

        const T& operator[](size_t i) const noexcept { return data_[i].lock; }

        constexpr size_t size() const noexcept { return N; }

    private:
        struct PaddedSpinLock
        {
            PaddedSpinLock() : lock() {}
            T lock;
            char padding[64 - sizeof(T)];
        };
        static_assert(
            sizeof(PaddedSpinLock) == 64,
            "Invalid size of PaddedSpinLock");

        // Check if T can theoretically cross a cache line.
        static_assert(
            sizeof(std::max_align_t) > 0 &&
                64 % sizeof(std::max_align_t) == 0 &&
                sizeof(T) <= sizeof(std::max_align_t),
            "T can cross cache line boundaries");

        static constexpr size_t PADDING_SIZE = 64;
        char padding_[PADDING_SIZE];
        std::array<PaddedSpinLock, N> data_;
    };

    //////////////////////////////////////////////////////////////////////

    using MSLGuard = std::lock_guard<MicroSpinLock>;

    //////////////////////////////////////////////////////////////////////

} // namespace folly
