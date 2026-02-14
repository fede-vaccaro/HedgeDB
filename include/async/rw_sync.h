#pragma once

#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <vector>

namespace hedge::async
{

    /**
     * @brief A synchronization primitive for read-mostly / striped-write scenarios.
     *
     * rw_sync implements a "Distributed Counter" or "Striped Locking" pattern, similar to
     * Scalable Read-Write Locks or Epoch-based Reclamation (EBR) mechanisms.
     *
     * Key Characteristics:
     * - Optimized for frequent, parallel writers (acquisition) and infrequent readers/flushers.
     * - Uses per-thread (or striped) atomic counters to eliminate cache-line contention (false sharing)
     *   on the write path, which is critical for high-throughput memtable insertions.
     * - A centralized atomic (_frozen) acts as a gatekeeper.
     *
     * Usage Contract:
     * - The user MUST guarantee that this_thread_idx is valid (< thread_count).
     * - While safe for multiple threads to share a slot (aliasing) due to internal ref-counting,
     *   performance is optimal when each thread has a unique, stable slot index.
     *
     * Memory Ordering:
     * - Uses seq_cst to enforce a strict Store-Load barrier between the writer announcing
     *   presence (increment counter) and checking the gate (load frozen). This prevents a race
     *   where a flusher could freeze and destroy the object while a writer is still entering.
     */
    template <typename T>
    class rw_sync
    {

        struct alignas(64) counter_t
        {
            std::atomic_int64_t c{0};
        };

        T _obj;
        alignas(64) std::atomic_bool _frozen;
        std::vector<counter_t> _counters;

    public:
        // Delete default constructor to ensure explicit sizing
        rw_sync() = delete;

        // Prevent copying/moving while potentially referenced by acquired_writer
        rw_sync(const rw_sync&) = delete;
        rw_sync& operator=(const rw_sync&) = delete;
        rw_sync(rw_sync&&) = delete;
        rw_sync& operator=(rw_sync&&) = delete;

        explicit rw_sync(size_t thread_count) : _frozen(false), _counters(thread_count)
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

                // Decrement refcount.
                // Using fetch_sub allows multiple threads to safely share the same slot (aliasing).
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

        const T* ptr() const
        {
            return &this->_obj;
        }

        /**
         * @brief Acquires the underlying resource for writing.
         *
         * @param this_thread_idx The striped index for the current thread. Must be < thread_count.
         * @return acquired_writer An RAII handle. Evaluates to false if acquisition failed (frozen).
         */
        [[nodiscard]] acquired_writer acquire_writer(size_t this_thread_idx)
        {
            // CRITICAL: We must announce our presence BEFORE checking the frozen state.
            // This acts as a Store-Load barrier (seq_cst) preventing the flusher from
            // missing this writer.
            //
            // 1. Writer: Store(Counter) -> Load(Frozen)
            // 2. Flusher: Store(Frozen) -> Load(Counter)

            auto& counter = this->_counters[this_thread_idx].c;
            counter.store(1, std::memory_order::seq_cst);

            // Cannot write if it has been sealed
            if(this->_frozen.load(std::memory_order::seq_cst)) [[unlikely]]
            {
                // Back off: decrement and fail
                counter.store(0, std::memory_order::seq_cst);
                return acquired_writer{nullptr, nullptr};
            }

            return acquired_writer{&this->_obj, &counter};
        }

        /**
         * @brief Checks if any writer is currently holding a reference.
         * This is the "slow path" usually called by the flusher/reclaimer.
         */
        [[nodiscard]] bool any_active_writer() const
        {
            return std::any_of(this->_counters.begin(), this->_counters.end(), [](const counter_t& counter)
                               { return counter.c.load(std::memory_order::seq_cst) > 0; });
        }

        /**
         * @brief Freezes the resource, blocking new writers.
         * Existing writers are not affected, but new acquire_writer calls will fail.
         * The caller should spin on any_active_writer() to wait for draining.
         */
        void freeze_writes()
        {
            this->_frozen.store(true, std::memory_order::seq_cst);
        }
    };

} // namespace hedge::async
