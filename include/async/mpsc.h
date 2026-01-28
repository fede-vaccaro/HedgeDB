#include <array>
#include <atomic>
#include <bit>
#include <optional>
#include <thread>
#include <type_traits>

namespace hedge::async
{

    template <typename T, size_t CAPACITY>
        requires(std::popcount(CAPACITY) == 1 && // CAPACITY is a power of two
                 std::is_default_constructible_v<T> &&
                 std::is_move_assignable_v<T> &&
                 std::is_move_assignable_v<T>)
    class mpsc_queue
    {
        static auto constexpr MASK = CAPACITY - 1;

        struct alignas(64) queue_item
        {
            T data;
            std::atomic_bool committed{false};
        };

        alignas(64) std::array<queue_item, CAPACITY> _data;
        alignas(64) std::atomic_size_t _write{0};
        alignas(64) std::atomic_size_t _read{0};

    public:
        explicit mpsc_queue()
        {
            for(auto& [data, committed] : this->_data)
            {
                data = T{};
                committed.store(false, std::memory_order_relaxed);
            }
        }

        template <typename U>
        bool push_back(U&& obj)
        {
            size_t write = this->_write.load(std::memory_order_relaxed);

            while(true)
            {
                if(write - this->_read.load(std::memory_order_acquire) == CAPACITY)
                    return false;

                if(!this->_write.compare_exchange_weak(write, write + 1, std::memory_order_relaxed))
                    continue;

                auto& slot = this->_data[write & MASK];

                slot.data = std::move(obj);

                slot.committed.store(true, std::memory_order_release);
                return true;
            }
        }

        std::optional<T> pop_front()
        {
            // Queue is empty
            size_t curr_read_pos = this->_read.load(std::memory_order_acquire);

            if(curr_read_pos == this->_write.load(std::memory_order_relaxed))
                return std::nullopt;

            auto& slot = this->_data[curr_read_pos & MASK];

            bool expected_commit = true;
            int32_t try_count = 0;

            while(!slot.committed.compare_exchange_weak(expected_commit, false, std::memory_order_acquire))
            {
                // Spin wait for the item to be ready
                expected_commit = true;
                constexpr int32_t MAX_WAIT_COUNT = 16;
                if(try_count++ == MAX_WAIT_COUNT)
                {
                    try_count = 0;
                    std::this_thread::yield(); // TODO: better backoff strategy;
                }
            }

            auto obj = std::move(slot.data);

            this->_read.fetch_add(1, std::memory_order_release);

            return obj;
        }

        [[nodiscard]] constexpr size_t capacity() const
        {
            return CAPACITY;
        }

        [[nodiscard]] size_t size() const
        {
            return this->_write.load(std::memory_order_relaxed) -
                   this->_read.load(std::memory_order_relaxed);
        }

        [[nodiscard]] bool empty() const
        {
            return this->_read.load(std::memory_order_relaxed) ==
                   this->_write.load(std::memory_order_relaxed);
        }
    };

} // namespace hedge::async