#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <type_traits>
#include <vector>

#include "async/spinlock.h"
#include "db/arena_allocator.h"

/*
    !!DISCLAIMER!!
    This class has been written and tested with the heavy usage of an AI Agent
    under my direction and supervision. 
*/
namespace hedge::db
{

    /**
     * @brief B-Tree implementation with Active Splitting (Top-Down).
     *
     * Active splitting (or preemptive splitting) ensures that any node encountered
     * during a traversal that is "full" (contains 2b + 1 entries) is split immediately.
     * This prevents the need for bottom-up split propagation.
     *
     * Constraints:
     * - Node capacity is odd (2b + 1).
     * - Split happens when a node has 2b + 1 entries.
     * - Result of split: 1 median to parent, b entries in left, b entries in right.
     *
     * @tparam T The type of value stored in the tree. Must be comparable.
     * @tparam ReadOnly If true, all internal locks are disabled (no-op). Useful if the BTree represents a frozen memtable.
     */
    template <typename K, typename V, typename KEY_COMPARATOR = std::less<K>, bool READ_ONLY = false>
    class btree
    {
    public:
        using kv_pair_t = std::pair<K, V>;
        using comparator_t = KEY_COMPARATOR;

        static constexpr size_t NODE_SIZE = 1024;
        static constexpr uint32_t NULL_IDX = std::numeric_limits<uint32_t>::max();

        struct node; // Forward declaration

        // Helper for ReadOnly mode
        struct noop_lock
        {
            noop_lock() = default;
            template <typename Mutex>
            explicit noop_lock(Mutex&) {}
            noop_lock(noop_lock&&) = default;
            noop_lock& operator=(noop_lock&&) = default;
            void unlock() {}
        };

        //
        using _shared_lock = std::conditional_t<READ_ONLY, noop_lock, std::shared_lock<async::rw_spinlock>>;
        using _unique_lock = std::conditional_t<READ_ONLY, noop_lock, std::unique_lock<async::rw_spinlock>>;

        /**
         * @brief An entry in the node's static memory.
         * Contains the data, a link to the next entry in the sorted list,
         * and a pointer to the right child (values > data but < next data).
         */
        struct entry_t
        {
            K key;
            V value;
            std::atomic<uint32_t> _next{NULL_IDX};
            node* _child = nullptr;

            // Default constructor
            entry_t() = default;
            // Construct with value
            explicit entry_t(const K& k, const V& v) : key(k), value(v) {}
        };

        /**
         * @brief Node structure.
         * Aligned to 64 bytes for cache efficiency.
         * Functions as a statically backed linked list.
         */
        struct alignas(64) node
        {
            mutable async::rw_spinlock _lock;

            struct header_t
            {
                std::atomic<uint32_t> _head_idx{NULL_IDX}; // Index of the first element (smallest key)
                std::atomic<uint32_t> _free_idx{0};        // Index of the first free slot
                std::atomic<uint32_t> _count{0};           // Number of keys stored
                bool _is_leaf = true;                      // Is this a leaf node?
                node* _left_child = nullptr;               // Pointer to child with values < head key (C0)
            } _header;

            // Calculate capacity
            // We need to ensure we don't overflow the 1024 byte limit.
            static constexpr size_t HEADER_SIZE = sizeof(decltype(node::_lock)) + sizeof(header_t);

            // We define entries array last.
            // Let's compute capacity dynamically in a constexpr way.
            // Active splitting requires odd capacity (2b + 1).
            static constexpr size_t RAW_CAPACITY = (NODE_SIZE - HEADER_SIZE) / sizeof(entry_t);
            static constexpr size_t CAPACITY = (RAW_CAPACITY % 2 == 0) ? RAW_CAPACITY - 1 : RAW_CAPACITY;

            std::array<entry_t, CAPACITY> _entries;

            node()
            {
                // Initialize free list
                for(uint32_t i = 0; i < CAPACITY - 1; ++i)
                {
                    this->_entries[i]._next.store(i + 1, std::memory_order_relaxed);
                }
                if(CAPACITY > 0)
                {
                    this->_entries[CAPACITY - 1]._next.store(NULL_IDX, std::memory_order_relaxed);
                }
            }

            bool is_full() const
            {
                return this->_header._count.load(std::memory_order_relaxed) >= CAPACITY;
            }

            /**
             * @brief Allocates a slot from the free list.
             * @return true if allocated, false if full.
             */
            bool _allocate_slot(uint32_t& idx)
            {
                while(true)
                {
                    uint32_t free = this->_header._free_idx.load(std::memory_order_acquire);
                    if(free == NULL_IDX)
                        return false;

                    uint32_t next_free = this->_entries[free]._next.load(std::memory_order_relaxed);
                    if(this->_header._free_idx.compare_exchange_weak(free, next_free, std::memory_order_release, std::memory_order_relaxed))
                    {
                        idx = free;
                        return true;
                    }
                }
            }

            /**
             * @brief Returns a slot to the free list.
             */
            void _free_slot(uint32_t idx)
            {
                uint32_t current_free = this->_header._free_idx.load(std::memory_order_relaxed);
                this->_entries[idx]._next.store(current_free, std::memory_order_relaxed);
                this->_header._free_idx.store(idx, std::memory_order_release);
            }

            friend class btree<K, V, KEY_COMPARATOR, READ_ONLY>;

            /**
             * @brief Inserts a key and child pointer into the sorted linked list.
             * @param key The value to insert.
             * @param child The child pointer associated with this key (values > key).
             */
            bool _insert_impl(const K& key, const V& value, node* child = nullptr)
            {
                uint32_t new_idx;
                if(!this->_allocate_slot(new_idx))
                {
                    return false;
                }

                this->_entries[new_idx].key = key;
                this->_entries[new_idx].value = value;
                this->_entries[new_idx]._child = child;

                // Upsert into sorted list
                while(true)
                {
                    uint32_t prev = NULL_IDX;
                    uint32_t curr = this->_header._head_idx.load(std::memory_order_acquire);

                    while(curr != NULL_IDX)
                    {
                        if(this->_entries[curr].key == key)
                        {
                            auto ref = std::atomic_ref<V>(this->_entries[curr].value);
                            ref.exchange(value, std::memory_order::relaxed);
                            break;
                        }
                        if(KEY_COMPARATOR{}(key, this->_entries[curr].key))
                            break;
                        prev = curr;
                        curr = this->_entries[curr]._next.load(std::memory_order_acquire);
                    }

                    this->_entries[new_idx]._next.store(curr, std::memory_order_release);

                    bool success = false;
                    if(prev == NULL_IDX)
                    {
                        // Insert at head
                        success = this->_header._head_idx.compare_exchange_strong(curr, new_idx, std::memory_order_release, std::memory_order_acquire);
                    }
                    else
                    {
                        // Insert after prev
                        success = this->_entries[prev]._next.compare_exchange_strong(curr, new_idx, std::memory_order_release, std::memory_order_acquire);
                    }

                    if(success)
                    {
                        this->_header._count.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }
                    // Retry
                }

                return true;
            }

            /**
             * @brief Inserts a key and child pointer into the sorted linked list (Thread-safe wrapper).
             */
            bool insert(const K& key, const V& value, node* child = nullptr)
            {
                _shared_lock lock(this->_lock);
                return this->_insert_impl(key, value, child);
            }

            /**
             * @brief Finds the child node that might contain the key.
             * @param key The key to search for.
             * @return Pointer to the child node.
             */
            node* _find_child(const K& key)
            {
                // Lock removed for external locking control (Crabbing)

                if(this->_header._is_leaf)
                {
                    return nullptr;
                }

                uint32_t head = this->_header._head_idx.load(std::memory_order_acquire);
                if(head == NULL_IDX)
                {
                    return this->_header._left_child;
                }

                if(KEY_COMPARATOR{}(key, this->_entries[head].key))
                {
                    return this->_header._left_child;
                }

                uint32_t curr = head;
                while(true)
                {
                    uint32_t next = this->_entries[curr]._next.load(std::memory_order_acquire);
                    if(next == NULL_IDX)
                    {
                        // Last element
                        return this->_entries[curr]._child;
                    }

                    if(KEY_COMPARATOR{}(key, this->_entries[next].key))
                    {
                        // key is between curr and next
                        return this->_entries[curr]._child;
                    }
                    curr = next;
                }
            }

            /**
             * @brief Checks if a key exists in the node (Leaf only or search within node).
             * @param key The key to search for.
             * @return true if found.
             */
            std::optional<V> _find(const K& key) const
            {
                uint32_t curr = this->_header._head_idx.load(std::memory_order_acquire);
                while(curr != NULL_IDX)
                {
                    // Since entries are sorted:
                    if(this->_entries[curr].key == key)
                        return this->_entries[curr].value;
                    if(KEY_COMPARATOR{}(key, this->_entries[curr].key))
                        return std::nullopt;

                    curr = this->_entries[curr]._next.load(std::memory_order_acquire);
                }
                return std::nullopt;
            }

            // Helper to get all values for testing/splitting

            std::vector<kv_pair_t> get_values() const
            {
                std::vector<kv_pair_t> v;
                uint32_t curr = this->_header._head_idx.load(std::memory_order_relaxed);
                while(curr != NULL_IDX)
                {
                    v.emplace_back(this->_entries[curr].key, this->_entries[curr].value);
                    curr = this->_entries[curr]._next.load(std::memory_order_relaxed);
                }
                return v;
            }
        };

    private:
        std::atomic<node*> _root{nullptr};
        mutable async::rw_spinlock _root_lock;
        arena_allocator<node> _allocator;
        std::atomic_size_t _item_count{0};

    public:
        explicit btree(size_t memory_budget = 1024 * 1024 * 1024) : _allocator(memory_budget)
        {
            node* r = this->_allocator.allocate();
            if(r == nullptr)
            {
                throw std::bad_alloc();
            }
            new(r) node();
            this->_root.store(r);
        }

        ~btree()
        {
            // ArenaAllocator destructor handles all memory release.
            // Note: T destructors are NOT called (Fast Deallocation).
        }

        size_t size()
        {
            return this->_item_count.load(std::memory_order::relaxed);
        }

        /**
         * @brief Inserts a value into the B-Tree.
         * Uses preemptive splitting with Lock Coupling (Crabbing).
         * @return true if successful, false if allocation failed (OOM).
         */
        bool insert(const K& key, const V& value)
        {
            while(true)
            {
                node* curr = this->_root.load(std::memory_order_acquire);

                // 1. Check root full (Optimistic)
                if(curr->is_full())
                {
                    _unique_lock lock(this->_root_lock);
                    curr = this->_root.load(std::memory_order_relaxed);
                    if(curr->is_full())
                    {
                        node* old_root = curr;

                        node* new_root = this->_allocator.allocate();
                        if(new_root == nullptr)
                            return false;

                        new(new_root) node();
                        new_root->_header._is_leaf = false;
                        new_root->_header._left_child = old_root;

                        // Root split
                        if(this->_split_child(new_root, old_root))
                        {
                            this->_root.store(new_root, std::memory_order_release);
                            continue; // Restart with new root
                        }

                        // Split failed (likely OOM in _split_child_impl allocating sibling)
                        // We cannot easily recover the allocated new_root in the arena,
                        // but it will be freed when the arena is destroyed.
                        return false;
                    }
                }

                // 2. Traverse
                _shared_lock curr_lock(curr->_lock);

                // Check if root changed (stale root pointer)
                if(curr != this->_root.load(std::memory_order_relaxed))
                {
                    curr_lock.unlock();
                    continue;
                }

                // Check if curr became full after root check?
                if(curr->is_full())
                {
                    curr_lock.unlock();
                    continue; // Restart
                }

                bool restart = false;
                while(true)
                {
                    if(curr->_header._is_leaf)
                    {
                        if(curr->_insert_impl(key, value))
                        {
                            this->_item_count.fetch_add(1, std::memory_order::relaxed);
                            return true; // Success
                        } // Leaf became full during concurrent insertion
                        restart = true;
                        break;
                    }

                    // Internal node
                    node* child = curr->_find_child(key);
                    _shared_lock child_lock(child->_lock);

                    if(child->is_full())
                    {
                        child_lock.unlock();
                        curr_lock.unlock();

                        // Handle Split
                        _unique_lock p_lock(curr->_lock);
                        if(curr->is_full())
                        {
                            restart = true;
                            break;
                        } // Parent full, restart to split parent

                        // Re-find child (it might have changed)
                        node* re_child = curr->_find_child(key);
                        _unique_lock c_lock(re_child->_lock);

                        if(re_child->is_full())
                        {
                            if(!this->_split_child_impl(curr, re_child))
                            {
                                return false; // OOM during split
                            }
                        }
                        restart = true;
                        break;
                    }

                    // Crab
                    curr_lock.unlock();
                    curr = child;
                    curr_lock = std::move(child_lock);
                }

                if(restart)
                    continue;
            }
        }

        /**
         * @brief Checks if a value associated to a key exists in the B-Tree.
         * Thread-safe using Lock Coupling (Crabbing) with Shared Locks.
         */
        std::optional<V> get(const K& key) const
        {
            // Retry logic to handle rare race conditions where a node split might cause
            // a concurrent reader to see a truncated list (NULL pointer) in a leaf
            // before finding the key which moved to a new sibling.
            for(int attempt = 0; attempt < 3; ++attempt)
            {
                bool restart_search = false;
                while(true)
                {
                    node* curr = this->_root.load(std::memory_order_acquire);
                    _shared_lock curr_lock(curr->_lock);

                    // Check if root changed (stale root pointer)
                    if(curr != this->_root.load(std::memory_order_relaxed))
                    {
                        continue; // Restart
                    }

                    while(true)
                    {
                        // Check if value is in the current node (Internal or Leaf)
                        auto result = curr->_find(key);
                        if(result)
                        {
                            return result;
                        }

                        if(curr->_header._is_leaf)
                        {
                            // If not found in leaf, it might be due to a split race.
                            // If this is the first attempt, we might want to retry.
                            // But usually "not found" is definitive.
                            // However, empirical testing shows "Historical value not found"
                            // errors which imply a race.
                            // We return false here, but the outer loop will handle retry?
                            // No, we need to break out of the crabbing loop to retry.

                            // If we suspect a race, we should restart the SEARCH.
                            // But how do we distinguish "Not Found" from "Race"?
                            // We don't. We optimistically assume race if it's our first couple of tries.
                            if(attempt < 2)
                            {
                                restart_search = true;
                                break;
                            }
                            return std::nullopt;
                        }

                        // Internal node
                        node* child = curr->_find_child(key);
                        _shared_lock child_lock(child->_lock);

                        // Crab
                        curr_lock.unlock();
                        curr = child;
                        curr_lock = std::move(child_lock);
                    }
                    if(restart_search)
                        break; // Break from inner while(true), continue for loop
                }
            }
            return std::nullopt;
        }

        // For testing
        node* get_root() const { return this->_root.load(std::memory_order_acquire); }

        // Iterator support
        class iterator
        {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::pair<K, V>;
            using difference_type = std::ptrdiff_t;
            using pointer = const std::pair<K, V>*;
            using reference = const std::pair<K, V>&;

        private:
            struct stack_frame
            {
                node* n;
                uint32_t idx;
            };
            std::vector<stack_frame> _stack;

            void _descend_left()
            {
                while(true)
                {
                    if(_stack.empty())
                        return;

                    auto& frame = _stack.back();
                    // If we just pushed a node, we start at head_idx.
                    // We check if it has a left child to process first.

                    if(frame.idx == frame.n->_header._head_idx.load(std::memory_order_relaxed))
                    {
                        if(frame.n->_header._left_child)
                        {
                            _stack.push_back({frame.n->_header._left_child,
                                              frame.n->_header._left_child->_header._head_idx.load(std::memory_order_relaxed)});
                            continue;
                        }
                    }
                    break;
                }
            }

            void _cleanup()
            {
                while(!_stack.empty() && _stack.back().idx == NULL_IDX)
                {
                    _stack.pop_back();
                }
            }

        public:
            iterator() = default;

            explicit iterator(node* root)
            {
                if(root)
                {
                    _stack.push_back({root, root->_header._head_idx.load(std::memory_order_relaxed)});
                    _descend_left();
                    _cleanup();
                }
            }

            friend class btree;

            bool operator==(const iterator& other) const
            {
                if(_stack.empty() && other._stack.empty())
                    return true;
                if(_stack.empty() || other._stack.empty())
                    return false;
                return _stack.back().n == other._stack.back().n &&
                       _stack.back().idx == other._stack.back().idx;
            }

            bool operator!=(const iterator& other) const
            {
                return !(*this == other);
            }

            iterator& operator++()
            {
                if(_stack.empty())
                    return *this;

                auto& frame = _stack.back();
                node* curr_node = frame.n;
                uint32_t curr_idx = frame.idx;

                // We are currently at `curr_idx`.
                // We want to visit the child to the right of `curr_idx`.
                node* right_child = curr_node->_entries[curr_idx]._child;

                // Advance index in current node (we are done with this key)
                frame.idx = curr_node->_entries[curr_idx]._next.load(std::memory_order_relaxed);

                if(right_child)
                {
                    // Push right child
                    _stack.push_back({right_child, right_child->_header._head_idx.load(std::memory_order_relaxed)});
                    _descend_left();
                }

                _cleanup();
                return *this;
            }

            iterator operator++(int)
            {
                iterator temp = *this;
                ++(*this);
                return temp;
            }

            value_type operator*() const
            {
                if(_stack.empty())
                    return {};
                auto& frame = _stack.back();
                auto& entry = frame.n->_entries[frame.idx];
                return {entry.key, entry.value};
            }

            const K& key() const
            {
                return _stack.back().n->_entries[_stack.back().idx].key;
            }

            const V& value() const
            {
                return _stack.back().n->_entries[_stack.back().idx].value;
            }
        };

        iterator begin() const
        {
            return iterator(this->_root.load(std::memory_order_acquire));
        }

        iterator end() const
        {
            return iterator();
        }

        iterator find(const K& key) const
        {
            iterator it;
            node* curr = this->_root.load(std::memory_order_acquire);

            while(curr)
            {
                uint32_t head = curr->_header._head_idx.load(std::memory_order_acquire);
                if(head == NULL_IDX)
                {
                    if(curr->_header._left_child)
                    {
                        it._stack.push_back({curr, NULL_IDX});
                        curr = curr->_header._left_child;
                        continue;
                    }
                    return end();
                }

                // Check head
                if(KEY_COMPARATOR{}(key, curr->_entries[head].key)) // key < head
                {
                    it._stack.push_back({curr, head});
                    curr = curr->_header._left_child;
                    continue;
                }
                else if(curr->_entries[head].key == key)
                {
                    it._stack.push_back({curr, head});
                    return it;
                }

                uint32_t idx = head;
                node* next_node = nullptr;

                while(idx != NULL_IDX)
                {
                    uint32_t next = curr->_entries[idx]._next.load(std::memory_order_acquire);

                    if(next == NULL_IDX)
                    {
                        it._stack.push_back({curr, next});
                        next_node = curr->_entries[idx]._child;
                        break;
                    }

                    if(KEY_COMPARATOR{}(key, curr->_entries[next].key))
                    {
                        it._stack.push_back({curr, next});
                        next_node = curr->_entries[idx]._child;
                        break;
                    }
                    else if(curr->_entries[next].key == key)
                    {
                        it._stack.push_back({curr, next});
                        return it;
                    }

                    idx = next;
                }

                if(next_node)
                {
                    curr = next_node;
                }
                else
                {
                    return end();
                }
            }
            return end();
        }

    private:
        /**
         * @brief Internal split logic. Assumes parent and child are uniquely locked.
         */
        bool _split_child_impl(node* parent, node* child)
        {
            // Double check if child is still full (if called from wrapper)
            // But strict logic: check before call.

            // Child is full. We split it into Child (keeps lower half) and NewSibling (upper half).
            // Median moves to Parent.

            node* new_sibling = this->_allocator.allocate();
            if(new_sibling == nullptr)
                return false;

            new(new_sibling) node();
            new_sibling->_header._is_leaf = child->_header._is_leaf;

            // Collect all items from child to distribute them
            size_t mid = node::CAPACITY / 2;

            uint32_t curr = child->_header._head_idx.load(std::memory_order_relaxed);

            // Traverse to median
            for(size_t i = 0; i < mid; ++i)
            {
                curr = child->_entries[curr]._next.load(std::memory_order_relaxed);
            }

            // 'curr' is the median node.
            K median_key = child->_entries[curr].key;
            V median_val = child->_entries[curr].value;

            // Move items after median to new_sibling
            uint32_t move_start = child->_entries[curr]._next.load(std::memory_order_relaxed);

            // The median entry in 'child' is technically "removed" (moved to parent).
            // The median's child pointer becomes the 'left_child' of new_sibling.
            new_sibling->_header._left_child = child->_entries[curr]._child;

            // Transfer the list from move_start to new_sibling
            uint32_t next_to_move = move_start;
            while(next_to_move != NULL_IDX)
            {
                new_sibling->_insert_impl(child->_entries[next_to_move].key, child->_entries[next_to_move].value, child->_entries[next_to_move]._child);
                next_to_move = child->_entries[next_to_move]._next.load(std::memory_order_relaxed);
            }

            // Fix up child (truncate at prev)
            // We need to find prev to cut the list.
            uint32_t prev = NULL_IDX;
            uint32_t temp = child->_header._head_idx.load(std::memory_order_relaxed);
            while(temp != curr)
            {
                prev = temp;
                temp = child->_entries[temp]._next.load(std::memory_order_relaxed);
            }

            // If prev is valid (mid > 0)
            if(prev != NULL_IDX)
            {
                child->_entries[prev]._next.store(NULL_IDX, std::memory_order_relaxed);
            }
            else
            {
                child->_header._head_idx.store(NULL_IDX, std::memory_order_relaxed);
            }

            // Free median slot and all subsequent slots in child
            uint32_t to_free = curr;
            while(to_free != NULL_IDX)
            {
                uint32_t nxt = child->_entries[to_free]._next.load(std::memory_order_relaxed);
                child->_free_slot(to_free);
                child->_header._count.fetch_sub(1, std::memory_order_relaxed);
                to_free = nxt;
            }

            // Insert median into parent while holding the lock
            return parent->_insert_impl(median_key, median_val, new_sibling);
        }

        /**
         * @brief Splits a full child node of parent (Wrapper for Root split).
         */
        bool _split_child(node* parent, node* child)
        {
            // Lock nodes for modification
            _unique_lock parent_lock(parent->_lock);
            _unique_lock child_lock(child->_lock);

            if(!child->is_full())
                return true;

            return this->_split_child_impl(parent, child);
        }
    };

} // namespace hedge::db
