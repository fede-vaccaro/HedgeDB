#include <algorithm>
#include <atomic>
#include <cassert>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <vector>

#include "async/generator.h"

namespace hedge
{

    // inlined key-value
    template <typename T>
    struct node
    {
        constexpr static size_t EMPTY_IDX = std::numeric_limits<uint32_t>::max();

        T data;
        uint32_t next[1]; // trick for C++ variable length arrays

        explicit node(size_t height)
        {
            for(auto* it = next; it < next + height + 1; ++it)
                *it = EMPTY_IDX;
        }

        static size_t sizeof_at_level(size_t target_level)
        {
            // Key size + V size + level pointers (indices)
            return sizeof(T) + ((target_level + 1) * sizeof(uint32_t));
        }
    };
    class arena
    {
        size_t _head{0};
        size_t _capacity{0};
        std::unique_ptr<uint8_t[], decltype(&std::free)> _data = std::unique_ptr<uint8_t[], decltype(&std::free)>(nullptr, nullptr);

    public:
        explicit arena() = default;

        explicit arena(size_t size_bytes) : _capacity(size_bytes)
        {
            this->_data = std::unique_ptr<uint8_t[], decltype(&std::free)>((uint8_t*)std::aligned_alloc(8, size_bytes), &std::free);

            std::fill_n(this->_data.get(), this->_capacity, 0);
        }

        arena(arena&&) = default;
        arena& operator=(arena&&) = default;

        [[nodiscard]] void* at(size_t idx)
        {
            return this->_data.get() + idx;
        }

        [[nodiscard]] const void* at(size_t idx) const
        {
            return this->_data.get() + idx;
        }

        size_t allocate(size_t bytes)
        {
            size_t cur = this->_head;

            if(this->_head + bytes > _capacity)
                throw std::runtime_error("arena out of mem");

            this->_head += bytes;

            return cur;
        }

        ~arena()
        {
            std::cout << "Arena allocated bytes: " << this->_capacity / 1024 << " KB\n";
            std::cout << "Arena used bytes: " << this->_head / 1024 << " KB\n";
        }
    };

    // This is a concurrent single-writer multiple-reader skiplist
    template <typename T>
    struct skiplist
    {
    public:
        struct config
        {
            double p{0.33};
            size_t item_capacity{200'000};
            std::optional<size_t> level_cap{12};
        };

    private:
        using node_t = node<T>;

        config _cfg;

        arena _node_ptr_pool;
        node_t* _head;
        uint32_t _max_levels;
        std::atomic_size_t _size{0};

        std::vector<uint8_t> _precomputed_random_levels;

        node_t* _node_at(uint32_t node_idx)
        {
            return static_cast<node_t*>(this->_node_ptr_pool.at(node_idx));
        }

        [[nodiscard]] const node_t* _node_at(uint32_t node_idx) const
        {
            return static_cast<const node_t*>(this->_node_ptr_pool.at(node_idx));
        }

    public:
        explicit skiplist(const config& cfg) : _cfg(cfg)
        {
            size_t expected_height = std::log((double)this->_cfg.item_capacity) / std::log(1.0 / this->_cfg.p);

            this->_max_levels = std::min(this->_cfg.level_cap.value_or(std::numeric_limits<uint32_t>::max()), expected_height);

            double avg_ptr_per_node = 1.0 / (1.0 - this->_cfg.p);

            // compute needed capacity
            constexpr double margin = 1.1;
            size_t bytes_capacity = this->_cfg.item_capacity * (sizeof(T) + avg_ptr_per_node * sizeof(uint32_t)) * margin;

            this->_node_ptr_pool = arena(bytes_capacity);

            auto head_idx = this->_node_ptr_pool.allocate(node_t::sizeof_at_level(this->_max_levels - 1));

            void* mem = this->_node_ptr_pool.at(head_idx);

            ::new(mem) node_t(this->_max_levels);

            this->_head = (node_t*)mem;

            this->_precomputed_random_levels.resize(this->_cfg.item_capacity);

            thread_local std::random_device rd{};
            thread_local std::mt19937 gen{rd()};
            thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);

            for(size_t i = 0; i < this->_cfg.item_capacity; ++i)
            {
                this->_precomputed_random_levels[i] = 0;

                uint8_t count{0};

                while(dist(gen) < this->_cfg.p)
                    ++count;

                this->_precomputed_random_levels[i] = std::min(count, (uint8_t)(this->_max_levels - 1));
            }
        }

        [[nodiscard]] size_t capacity() const
        {
            return this->_cfg.item_capacity;
        }

        [[nodiscard]] size_t size() const
        {
            return this->_size.load(std::memory_order::relaxed);
        }

        template <typename COMPARATOR>
        void insert(const T& data, COMPARATOR&& C)
        {
            size_t dest_level = skiplist::_random_level();

            // build the new node
            auto new_node_idx = this->_node_ptr_pool.allocate(node_t::sizeof_at_level(dest_level));
            auto* new_node = static_cast<node_t*>(this->_node_ptr_pool.at(new_node_idx));
            ::new(new_node) node_t(dest_level);

            new_node->data = data;

            int k = this->_max_levels - 1;

            node_t* x{this->_head}; // X is the node we are currently reading
            node_t* z{};            // Z is the one after at the same level (if any)

            // Starts from max level
            // Position search and insertion happen at the same time
            thread_local std::vector<std::pair<node_t*, uint32_t>> updates;
            updates.clear();
            updates.resize(dest_level + 1);

            for(; k > -1; --k)
            {
                uint32_t z_idx = x->next[k];
                z = this->_node_at(z_idx);

                // Iterate and find the position at the current level
                while(z_idx != node_t::EMPTY_IDX && !(C(data, z->data)))
                {
                    // std::cout << "Traversing at level " << k << ": " << z.to_string(k) << "\n";
                    x = z;
                    z_idx = z->next[k];

                    // [[maybe_unused]] auto check_z_idx_is_valued = [&]()
                    // {
                    // std::cout << "next_z_idx: " << z_idx << " z: " << z->to_string(dest_level) << std::endl;
                    // std::cout << "x: " << x->to_string(dest_level) << std::endl;

                    // return z_idx != 0;
                    // };

                    // assert(check_z_idx_is_valued());
                    z = this->_node_at(z_idx);
                }

                // When the position is found
                if(k <= static_cast<int>(dest_level))
                    updates[k] = std::pair<node_t*, uint32_t>{x, z_idx};
            }

            // It is crucial to publish the update bottom-up
            for(int k = 0; k < (int)updates.size(); ++k)
            {
                auto [x, z_idx] = updates[k];

                std::atomic_ref<uint32_t>(new_node->next[k]).store(z_idx, std::memory_order::release);
                std::atomic_ref<uint32_t>(x->next[k]).store(new_node_idx, std::memory_order::release); // <-- publish operation
            }

            this->_size.fetch_add(1, std::memory_order::relaxed);
        }

        template <typename COMPARATOR, typename EQUIVALENCE>
        [[nodiscard]] std::optional<T> find_if(T data, COMPARATOR&& C, EQUIVALENCE&& EQ) const
        {

            const node_t* x{this->_head}; // X is the current node
            const node_t* z{};            // Z is the next

            for(int k = this->_max_levels - 1; k > -1; --k)
            {
                uint32_t z_idx = std::atomic_ref<const uint32_t>(x->next[k]).load(std::memory_order::acquire);
                z = this->_node_at(z_idx);

                while(z_idx != node_t::EMPTY_IDX && !(C(data, z->data)))
                {
                    if(EQ(z->data, data))
                        return data;

                    x = z;
                    z_idx = z->next[k];
                    z = this->_node_at(z_idx);
                }
            }

            return std::nullopt;
        }

        template <typename COMPARATOR>
        [[nodiscard]] std::optional<T> find(T data, COMPARATOR&& C) const
        {
            return this->find_if(std::move(data), std::forward<COMPARATOR>(C),
                                 [](const T& a, const T& b)
                                 { return a == b; });
        }

        [[nodiscard]] size_t levels() const
        {
            return this->_max_levels;
        }

        hedge::async::generator<T> reader(size_t level = 0)
        {
            uint32_t curr_node_idx = this->_head->next[level];
            const node_t* curr = this->_node_at(curr_node_idx);

            while(curr_node_idx != node_t::EMPTY_IDX)
            {
                T data = curr->data;
                co_yield data;

                curr_node_idx = curr->next[level];
                curr = this->_node_at(curr_node_idx);
            }

            co_return;
        }

    private:
        size_t _random_level()
        {
            return this->_precomputed_random_levels[this->_size];
        }
    };

} // namespace hedge
