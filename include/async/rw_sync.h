#pragma once

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <vector>

namespace hedge::async
{

    // This structure is needed for coordinating writers and readers using multiple atomic reference counters.
    // Using a single counter is a bottleneck due to cache invalidation protocols (MESI) between multiple threads being slow.
    // Currently it's only needed for lightweight memtable read/write synchronization between the writer and the flusher threads.
    // Executing std::atomic<std::shared_ptr<>>::load every time resulted slow due to the reference counter continuous update.
    //
    // This class allows to:
    // - Acquire the underlying resource for write 
    // - Check whether the underlying resource is been used from any writer
    // - Freeze the underlying resource, blocking any following acquisition by a writer
    template <typename T>
    class rw_sync
    {

        struct alignas(64) counter_t
        {
            std::atomic_int64_t c;
        };

        T _obj;
        alignas(64) std::atomic_bool _frozen;
        std::vector<counter_t> _counters;

    public:
        rw_sync() = default;

        rw_sync(size_t thread_count) : _frozen(false), _counters(thread_count)
        {
        }

        template <typename... Args>
        rw_sync(size_t thread_count, Args... args) : _obj(std::forward<Args>(args)...), _frozen(false), _counters(thread_count)
        {
        }

        // RAII style for decreasing the reference counter when a writer has acquired the object
        class acquired_writer
        {
            T* _obj;
            std::atomic_int64_t* _counter;
            acquired_writer(T* obj, std::atomic_int64_t* cnt) : _obj(obj), _counter(cnt)
            {
            }

            friend class rw_sync<T>;

        public:
            acquired_writer(acquired_writer&&) = default;
            acquired_writer(const acquired_writer&) = delete;

            operator bool() const
            {
                return this->_obj != nullptr;
            }

            T* operator->()
            {
                return this->_obj;
            }

            T& operator*()
            {
                return *this->_obj;
            }

            void release()
            {
                if(this->_counter == nullptr)
                    return;

                this->_counter->store(0, std::memory_order::seq_cst);

                this->_obj = nullptr;
                this->_counter = nullptr;
            }

            ~acquired_writer()
            {
                this->release();
            }
        };

        T* ptr()
        {
            return &this->_obj;
        }

        // 'acquire_writer' is needed for acquiring the underlying resource for write
        // Returns a "false" acquired_writer if the resource has been frozen
        // The user is responsible for this_thread_idx correctness
        acquired_writer acquire_writer(size_t this_thread_idx)
        {
            // Cannot write if it has been sealed
            if(this->_frozen.load(std::memory_order::seq_cst)) // Most of the time this will be fast; no cache invalidated
                return acquired_writer{nullptr, nullptr};

            this->_counters[this_thread_idx].c.store(1, std::memory_order::seq_cst);

            return acquired_writer{&this->_obj, &this->_counters[this_thread_idx].c};
        }

        // Self explaining
        [[nodiscard]] bool any_active_writer() const
        {
            return std::any_of(this->_counters.begin(), this->_counters.end(), [](const counter_t& counter)
                               { return counter.c.load(std::memory_order_seq_cst) > 0; });
        }

        // After freeze, the writer cannot acquire the resource anymore 
        void freeze_writes()
        {
            this->_frozen.store(true, std::memory_order::seq_cst);
        }
    };

} // namespace hedge::async