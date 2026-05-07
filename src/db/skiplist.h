#pragma once

#include "single_buffer_arena_allocator.h"
#include "types.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
// Folly's ConcurrentSkiplist
#include "db/skiplist/concurrent_skip_list/concurrent_skiplist.h"
#pragma GCC diagnostic pop

namespace hedge::db
{
    // Adapter for single_buffer_arena_allocator to std::allocator interface
    // *** Currently unused ***
    template <typename T>
    class StdArenaAllocator
    {
    public:
        using value_type = T;

        StdArenaAllocator(single_buffer_arena_allocator& arena) : arena_(&arena) {}

        template <typename U>
        StdArenaAllocator(const StdArenaAllocator<U>& other) : arena_(other.arena_) {}

        T* allocate(size_t n)
        {
            auto span = arena_->allocate(n * sizeof(T));
            if(span.empty())
            {
                throw std::bad_alloc();
            }
            return reinterpret_cast<T*>(span.data());
        }

        void deallocate(T* /* ptr */, size_t /* size */) noexcept
        {
            // No-op for arena
        }

        bool operator==(const StdArenaAllocator& other) const { return arena_ == other.arena_; }
        bool operator!=(const StdArenaAllocator& other) const { return arena_ != other.arena_; }

        template <typename U>
        friend class StdArenaAllocator;

    private:
        single_buffer_arena_allocator* arena_;
    };

    struct memtable_entry
    {
        key_t _key;
        uint64_t seq = 0;
        std::span<const std::byte> _value;

        memtable_entry() = default;
        memtable_entry(key_t k, uint64_t s, std::span<const std::byte> v) : _key(std::move(k)), seq(s), _value(v) {}

        [[nodiscard]] std::span<const std::byte> key() const
        {
            return this->_key;
        }

        [[nodiscard]] std::span<const std::byte> value() const
        {
            return this->_value;
        }
    };

    struct memtable_cmp
    {
        bool operator()(const memtable_entry& a, const memtable_entry& b) const
        {
            auto cmp = a._key <=> b._key;
            if(cmp != 0)
                return cmp < 0;
            return a.seq > b.seq; // higher seq = newer = comes first
        }
    };

    using skiplist_t = third_party::folly::ConcurrentSkipList<memtable_entry, memtable_cmp, std::allocator<std::byte>>;

} // namespace hedge::db