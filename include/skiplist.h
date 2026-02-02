#pragma once

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
    template <typename K, typename V>
    struct node
    {
        constexpr static size_t EMPTY_IDX = std::numeric_limits<uint32_t>::max();

        K key;
        V value;
        uint32_t next[1]; // trick for C++ variable length arrays

        explicit node(size_t height)
        {
            for(auto* it = next; it < next + height + 1; ++it)
                *it = EMPTY_IDX;
        }

        static size_t sizeof_at_level(size_t target_level)
        {
            // sizeof(node) includes K, V, padding, and next[0]
            // We need space for next[1]...next[target_level]
            // So we add target_level * sizeof(uint32_t)
            return sizeof(node) + (target_level * sizeof(uint32_t));
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
            // Allocate with 16-byte alignment to support aligned types (like value_ptr_t with atomic_ref)
            this->_data = std::unique_ptr<uint8_t[], decltype(&std::free)>((uint8_t*)std::aligned_alloc(16, size_bytes), &std::free);

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
            // Align allocation to 8 bytes (or 16 if needed, but 8 is usually enough for uint64_t/pointers)
            // Skiplist uses 32-bit indices (uint32_t) for pointers, but data types might need 8-byte alignment.
            // Let's use 8-byte alignment for safety.
            constexpr size_t ALIGNMENT = 8;
            size_t padding = (ALIGNMENT - (this->_head % ALIGNMENT)) % ALIGNMENT;

            size_t cur = this->_head + padding;

            if(cur + bytes > _capacity)
                throw std::runtime_error("arena out of mem");

            this->_head = cur + bytes;

            return cur;
        }

        ~arena()
        {
            // std::cout << "Arena allocated bytes: " << this->_capacity / 1024 << " KB\n";
            // std::cout << "Arena used bytes: " << this->_head / 1024 << " KB\n";
        }
    };

    // This is a concurrent single-writer multiple-reader skiplist
    template <typename K, typename V>
    struct alignas(64) skiplist
    {
    public:
        struct config
        {
            double p{0.33};
            size_t item_capacity{200'000};
            std::optional<size_t> level_cap{12};
        };

    private:
        using node_t = node<K, V>;

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
            // Increase margin to 1.25 to account for alignment padding overhead (8 bytes per alloc)
            constexpr double margin = 1.25;
            size_t node_overhead = sizeof(K) + sizeof(V) + avg_ptr_per_node * sizeof(uint32_t);
            // Add alignment slack per node (8 bytes worst case)
            size_t bytes_capacity = this->_cfg.item_capacity * (node_overhead + 8) * margin;

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

        template <typename KC = std::less<K>, typename VC = std::less<V>>
        void insert(const K& key, const V& value, KC&& kc = KC(), VC&& vc = VC())
        {
            int k = this->_max_levels - 1;

            node_t* x{this->_head}; // X is the node we are currently reading
            node_t* z{};            // Z is the one after at the same level (if any)

            // Position search and insertion happen at the same time
            thread_local std::vector<std::pair<node_t*, uint32_t>> updates;
            updates.clear();
            updates.resize(this->_max_levels);

            for(; k > -1; --k)
            {
                uint32_t z_idx = x->next[k];
                z = this->_node_at(z_idx);

                // Iterate and find the position at the current level
                // Move forward if z is strictly less than (key, value)
                // z < (key, value) <=> z.key < key || (z.key == key && z.value < value)
                while(z_idx != node_t::EMPTY_IDX &&
                      (kc(z->key, key) || (!kc(key, z->key) && vc(z->value, value))))
                {
                    x = z;
                    z_idx = z->next[k];
                    z = this->_node_at(z_idx);
                }

                updates[k] = std::pair<node_t*, uint32_t>{x, z_idx};
            }

            size_t dest_level = skiplist::_random_level();

            // build the new node
            auto new_node_idx = this->_node_ptr_pool.allocate(node_t::sizeof_at_level(dest_level));
            auto* new_node = static_cast<node_t*>(this->_node_ptr_pool.at(new_node_idx));
            ::new(new_node) node_t(dest_level);

            new_node->key = key;
            new_node->value = value;

            // It is crucial to publish the update bottom-up
            for(int k = 0; k <= (int)dest_level; ++k)
            {
                auto [update_x, z_idx] = updates[k];

                std::atomic_ref<uint32_t>(new_node->next[k]).store(z_idx, std::memory_order::release);
                std::atomic_ref<uint32_t>(update_x->next[k]).store(new_node_idx, std::memory_order::release); // <-- publish operation
            }

            this->_size.fetch_add(1, std::memory_order::relaxed);
        }

        template <typename COMPARATOR>
        [[nodiscard]] std::optional<std::pair<K, V>> find_if(const K& key, COMPARATOR&& C) const
        {

            const node_t* x{this->_head}; // X is the current node
            const node_t* z{};            // Z is the next

            for(int k = this->_max_levels - 1; k > -1; --k)
            {
                uint32_t z_idx = std::atomic_ref<const uint32_t>(x->next[k]).load(std::memory_order::acquire);
                z = this->_node_at(z_idx);

                while(z_idx != node_t::EMPTY_IDX && C(z->key, key))
                {
                    x = z;
                    z_idx = z->next[k];
                    z = this->_node_at(z_idx);
                }
            }

            // x is now the predecessor of the first node >= key
            uint32_t z_idx = std::atomic_ref<const uint32_t>(x->next[0]).load(std::memory_order::acquire);
            if(z_idx != node_t::EMPTY_IDX)
            {
                z = this->_node_at(z_idx);
                // We know z->key >= key (from loop termination condition)
                // Check if key >= z->key. If so, they are equal.
                if(!C(key, z->key))
                    return std::make_pair(z->key, z->value);
            }

            return std::nullopt;
        }

        template <typename COMPARATOR>
        [[nodiscard]] std::optional<std::pair<K, V>> find(const K& key, COMPARATOR&& C) const
        {
            return this->find_if(key, std::forward<COMPARATOR>(C));
        }

        [[nodiscard]] size_t levels() const
        {
            return this->_max_levels;
        }

        hedge::async::generator<std::pair<K, V>> reader(size_t level = 0)
        {
            uint32_t curr_node_idx = this->_head->next[level];
            const node_t* curr = this->_node_at(curr_node_idx);

            while(curr_node_idx != node_t::EMPTY_IDX)
            {
                std::pair<K, V> data = {curr->key, curr->value};
                co_yield data;

                curr_node_idx = curr->next[level];
                curr = this->_node_at(curr_node_idx);
            }

            co_return;
        }

    public:
        // Forward Iterator Implementation
        struct const_iterator
        {
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::pair<K, V>;
            using difference_type = std::ptrdiff_t;
            using pointer = const std::pair<K, V>*;
            using reference = const std::pair<K, V>&;

        private:
            const skiplist<K, V>* _list;
            uint32_t _curr_idx;
            mutable std::optional<value_type> _cached_val;

        public:
            const_iterator(const skiplist<K, V>* list, uint32_t idx)
                : _list(list), _curr_idx(idx) {}

            reference operator*() const
            {
                const node_t* node = _list->_node_at(_curr_idx);
                _cached_val = {node->key, node->value};
                return *_cached_val;
            }

            pointer operator->() const
            {
                const node_t* node = _list->_node_at(_curr_idx);
                _cached_val = {node->key, node->value};
                return &(*_cached_val);
            }

            const_iterator& operator++()
            {
                if(_curr_idx != node_t::EMPTY_IDX)
                {
                    const node_t* node = _list->_node_at(_curr_idx);
                    // Standard traversal uses level 0 for full enumeration
                    // Use acquire memory order to ensure visibility of published nodes
                    _curr_idx = std::atomic_ref<const uint32_t>(node->next[0])
                                    .load(std::memory_order::acquire);
                }
                return *this;
            }

            const_iterator operator++(int)
            {
                const_iterator tmp = *this;
                ++(*this);
                return tmp;
            }

            bool operator==(const const_iterator& other) const
            {
                return _curr_idx == other._curr_idx;
            }

            bool operator!=(const const_iterator& other) const
            {
                return !(*this == other);
            }
        };

        using iterator = const_iterator; // Readers only for this implementation

        [[nodiscard]] const_iterator begin() const
        {
            // Level 0 contains all elements in order
            uint32_t first = std::atomic_ref<const uint32_t>(this->_head->next[0])
                                 .load(std::memory_order::acquire);
            return const_iterator(this, first);
        }

        [[nodiscard]] const_iterator end() const
        {
            return const_iterator(this, node_t::EMPTY_IDX);
        }

        // Overloads for non-const instances
        [[nodiscard]] iterator begin()
        {
            uint32_t first = std::atomic_ref<uint32_t>(this->_head->next[0])
                                 .load(std::memory_order::acquire);
            return iterator(this, first);
        }

        [[nodiscard]] iterator end()
        {
            return iterator(this, node_t::EMPTY_IDX);
        }

    private:
        size_t _random_level()
        {
            return this->_precomputed_random_levels[this->_size % this->_cfg.item_capacity];
        }
    };

} // namespace hedge
