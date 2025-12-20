#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

// Page cache
// Implemented using a reference-counted CLOCK algorithm; 
class page_cache
{
    std::shared_mutex _m{};
    size_t _clock_head{};
    size_t _size{};
    size_t _max_capacity{};

    // The key is not stored here to obtain the correct cache alignement
    struct alignas(64) _metadata
    {
        std::shared_mutex m{};

        std::atomic_uint32_t reference_count{0};
        std::atomic_uint8_t recently_used{0};
    };

    std::unique_ptr<_metadata> _frames;      // Metadata about each pages - ~16 MB overhead for 1 GB worth of pages
    std::vector<size_t> _keys;               // idx -> keys mapping, needed on eviction - ~2MB overhead for 1 GB worth of pages
    std::unordered_map<size_t, size_t> _lut; // key -> idx mapping - IDK how to estimate the overhead here, damn you std::unordered_map!

    std::unique_ptr<uint8_t> _data; // Memory arena

public:
    using lock_t = std::variant<std::shared_lock<std::shared_mutex>, std::unique_lock<std::shared_mutex>>;

    page_cache(size_t pages) : _max_capacity(pages), _keys(pages)
    {
        this->_frames = std::unique_ptr<_metadata>(new _metadata[pages]); // NOLINT
        this->_lut.reserve(pages);

        auto* ptr = new uint8_t[pages * 4096];
        this->_data = std::unique_ptr<uint8_t>(ptr);
    }

    struct guarded_page
    {
        guarded_page() = default;
        guarded_page(lock_t lk, uint8_t* page, std::atomic_uint32_t* counter)
            : page(page),
              _lock(std::move(lk)),
              _counter_ptr(counter)
        {
            assert(counter != nullptr);
            this->_counter_ptr->fetch_add(1, std::memory_order::relaxed);
        }

        explicit guarded_page(guarded_page&& other) noexcept
            : page(other.page),
              _lock(std::move(other._lock)),
              _counter_ptr(std::exchange(other._counter_ptr, nullptr))
        {
        }

        guarded_page& operator=(guarded_page&& other) noexcept
        {
            this->_lock = std::move(other._lock);
            this->page = other.page;
            this->_counter_ptr = std::exchange(this->_counter_ptr, nullptr);

            return *this;
        }

        uint8_t* page;

        ~guarded_page()
        {
            // this->confirm_written();

            if(this->_counter_ptr != nullptr)
                this->_counter_ptr->fetch_sub(1, std::memory_order::relaxed);
        }

    private:
        std::variant<std::shared_lock<std::shared_mutex>, std::unique_lock<std::shared_mutex>> _lock;
        std::atomic_uint32_t* _counter_ptr;
    };

    guarded_page get_write_slot(size_t hash)
    {
        // std::lock_guard lk(this->_m);
        this->_find_frame();

        auto& frame = this->_frames.get()[this->_clock_head];

        // Set 'recently_used' bit (also known as 'reference_bit' in literature)
        frame.recently_used.store(1, std::memory_order::relaxed);

        // set lut(s) to point to the correct position
        this->_keys[this->_clock_head] = hash;
        this->_lut[hash] = this->_clock_head;

        // It should always possible to lock as the reference count is zero
        // Otherwise we could not have evicted this page before
        // Also, here we have the cache lock, so it is not possible that some other thread
        // Has any (shared) ownership of this frame
        auto slk = std::unique_lock(frame.m);

        return {
            std::move(slk),
            this->_data.get() + (this->_clock_head * 4096),
            &frame.reference_count};
    }

    std::optional<guarded_page> lookup(size_t hash)
    {
        std::shared_lock lk(this->_m);

        auto it = this->_lut.find(hash);
        if(it == this->_lut.end())
            return std::nullopt;

        auto idx = it->second;

        auto& frame = this->_frames.get()[idx];

        frame.recently_used.store(1, std::memory_order::relaxed);

        return std::make_optional<guarded_page>(guarded_page(
            std::shared_lock(frame.m),
            this->_data.get() + (idx * 4096),
            &frame.reference_count));
    }

    size_t capacity() const
    {
        return this->_max_capacity;
    }

    size_t size() const
    {
        return this->_size;
    }

    inline static size_t s_total_duration = 0;

private:
    void _find_frame()
    {
        if(this->_size < this->_max_capacity)
        {
            this->_clock_head = ++this->_size % this->_max_capacity;
            return;
        }

        // Search for an evictable page
        // If no page is found, this procedure behaves basically as a spin-lock
        while(true)
        {
            // Advance clock head
            this->_clock_head = (this->_clock_head + 1) % this->_max_capacity;

            auto& frame = this->_frames.get()[this->_clock_head];

            // a non-referenced page will be evicted
            if(frame.recently_used.load(std::memory_order_relaxed) == 0U &&
               frame.reference_count.load(std::memory_order_relaxed) == 0U)
            {
                this->_lut.erase(this->_keys[this->_clock_head]);
                return;
            }

            // Give the page a second chance, set the ref to 0
            // It makes the page associated to the frame "evictable", once the reference count goes to zero
            frame.recently_used.store(0, std::memory_order::relaxed);
        }
    }
};