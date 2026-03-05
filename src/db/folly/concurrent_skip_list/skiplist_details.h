/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// NOLINTBEGIN

#pragma once

#include <algorithm>
#include <atomic>
#include <climits>
#include <cmath>
#include <memory>
#include <mutex>
#include <random>
#include <type_traits>
#include <vector>

#include <boost/random.hpp>

#include "micro_spin_lock.h"

namespace folly
{
    namespace detail
    {

        // This works the same as constexpr_clamp, but the comparison are done in Src
        // to prevent any implicit promotions.
        template <typename D, typename S>
        constexpr D constexpr_clamp_cast_helper(S src, S sl, S su, D dl, D du)
        {
            return src < sl ? dl : (src > su ? du : D(src));
        }

        constexpr double kClampCastLowerBoundDoubleToInt64F = -9223372036854774784.0;
        constexpr double kClampCastUpperBoundDoubleToInt64F = 9223372036854774784.0;
        constexpr double kClampCastUpperBoundDoubleToUInt64F = 18446744073709549568.0;

        constexpr float kClampCastLowerBoundFloatToInt32F = -2147483520.0f;
        constexpr float kClampCastUpperBoundFloatToInt32F = 2147483520.0f;
        constexpr float kClampCastUpperBoundFloatToUInt32F = 4294967040.0f;

        template <typename T>
        constexpr bool constexpr_isnan(T const t)
        {
            return t != t; // NOLINT
        }

        template <typename T, typename Less>
        constexpr T const& constexpr_clamp(
            T const& v, T const& lo, T const& hi, Less less)
        {
            T const& a = less(v, lo) ? lo : v;
            T const& b = less(hi, a) ? hi : a;
            return b;
        }
        template <typename T>
        constexpr T const& constexpr_clamp(T const& v, T const& lo, T const& hi)
        {
            return constexpr_clamp(v, lo, hi, std::less<T>{});
        }

        // When a and b are equivalent objects, we return a to
        // make sorting stable.
        template <typename T, typename... Ts>
        constexpr T constexpr_min(T a, Ts... ts)
        {
            T list[] = {ts..., a}; // 0-length arrays are illegal
            for(auto i = 0u; i < sizeof...(Ts); ++i)
            {
                a = list[i] < a ? list[i] : a;
            }
            return a;
        }

        // clamp_cast<> provides sane numeric conversions from float point numbers to
        // integral numbers, and between different types of integral numbers. It helps
        // to avoid unexpected bugs introduced by bad conversion, and undefined behavior
        // like overflow when casting float point numbers to integral numbers.
        //
        // When doing clamp_cast<Dst>(value), if `value` is in valid range of Dst,
        // it will give correct result in Dst, equal to `value`.
        //
        // If `value` is outside the representable range of Dst, it will be clamped to
        // MAX or MIN in Dst, instead of being undefined behavior.
        //
        // Float NaNs are converted to 0 in integral type.
        //
        // Here's some comparison with static_cast<>:
        // (with FB-internal gcc-5-glibc-2.23 toolchain)
        //
        // static_cast<int32_t>(NaN) = 6
        // clamp_cast<int32_t>(NaN) = 0
        //
        // static_cast<int32_t>(9999999999.0f) = -348639895
        // clamp_cast<int32_t>(9999999999.0f) = 2147483647
        //
        // static_cast<int32_t>(2147483647.0f) = -348639895
        // clamp_cast<int32_t>(2147483647.0f) = 2147483647
        //
        // static_cast<uint32_t>(4294967295.0f) = 0
        // clamp_cast<uint32_t>(4294967295.0f) = 4294967295
        //
        // static_cast<uint32_t>(-1) = 4294967295
        // clamp_cast<uint32_t>(-1) = 0
        //
        // static_cast<int16_t>(32768u) = -32768
        // clamp_cast<int16_t>(32768u) = 32767

        template <typename Dst, typename Src>
        constexpr typename std::enable_if<std::is_integral<Src>::value, Dst>::type
        constexpr_clamp_cast(Src src)
        {
            static_assert(
                std::is_integral<Dst>::value && sizeof(Dst) <= sizeof(int64_t),
                "constexpr_clamp_cast can only cast into integral type (up to 64bit)");

            using L = std::numeric_limits<Dst>;
            // clang-format off
  return
    // Check if Src and Dst have same signedness.
    std::is_signed<Src>::value == std::is_signed<Dst>::value
    ? (
      // Src and Dst have same signedness. If sizeof(Src) <= sizeof(Dst),
      // we can safely convert Src to Dst without any loss of accuracy.
      sizeof(Src) <= sizeof(Dst) ? Dst(src) :
      // If Src is larger in size, we need to clamp it to valid range in Dst.
      Dst(constexpr_clamp(src, Src(L::min()), Src(L::max()))))
    // Src and Dst have different signedness.
    // Check if it's signed -> unsigned cast.
    : std::is_signed<Src>::value && std::is_unsigned<Dst>::value
    ? (
      // If src < 0, the result should be 0.
      src < 0 ? Dst(0) :
      // Otherwise, src >= 0. If src can fit into Dst, we can safely cast it
      // without loss of accuracy.
      sizeof(Src) <= sizeof(Dst) ? Dst(src) :
      // If Src is larger in size than Dst, we need to ensure the result is
      // at most Dst MAX.
      Dst(constexpr_min(src, Src(L::max()))))
    // It's unsigned -> signed cast.
    : (
      // Since Src is unsigned, and Dst is signed, Src can fit into Dst only
      // when sizeof(Src) < sizeof(Dst).
      sizeof(Src) < sizeof(Dst) ? Dst(src) :
      // If Src does not fit into Dst, we need to ensure the result is at most
      // Dst MAX.
      Dst(constexpr_min(src, Src(L::max()))));
            // clang-format on
        }

        template <typename Dst, typename Src>
        constexpr typename std::enable_if<std::is_floating_point<Src>::value, Dst>::type
        constexpr_clamp_cast(Src src)
        {
            static_assert(
                std::is_integral<Dst>::value && sizeof(Dst) <= sizeof(int64_t),
                "constexpr_clamp_cast can only cast into integral type (up to 64bit)");

            using L = std::numeric_limits<Dst>;
            // clang-format off
  return
    // Special case: cast NaN into 0.
    constexpr_isnan(src) ? Dst(0) :
    // using `sizeof(Src) > sizeof(Dst)` as a heuristic that Dst can be
    // represented in Src without loss of accuracy.
    // see: https://en.wikipedia.org/wiki/Floating-point_arithmetic
    sizeof(Src) > sizeof(Dst) ?
      constexpr_clamp_cast_helper(
          src, Src(L::min()), Src(L::max()), L::min(), L::max()) :
    // sizeof(Src) < sizeof(Dst) only happens when doing cast of
    // 32bit float -> u/int64_t.
    // Losslessly promote float into double, change into double -> u/int64_t.
    sizeof(Src) < sizeof(Dst) ? (
      src >= 0.0
      ? constexpr_clamp_cast<Dst>(
            constexpr_clamp_cast<std::uint64_t>(double(src)))
      : constexpr_clamp_cast<Dst>(
            constexpr_clamp_cast<std::int64_t>(double(src)))) :
    // The following are for sizeof(Src) == sizeof(Dst).
    std::is_same<Src, double>::value && std::is_same<Dst, int64_t>::value ?
      constexpr_clamp_cast_helper(
          double(src),
          kClampCastLowerBoundDoubleToInt64F,
          kClampCastUpperBoundDoubleToInt64F,
          L::min(),
          L::max()) :
    std::is_same<Src, double>::value && std::is_same<Dst, uint64_t>::value ?
      constexpr_clamp_cast_helper(
          double(src),
          0.0,
          kClampCastUpperBoundDoubleToUInt64F,
          L::min(),
          L::max()) :
    std::is_same<Src, float>::value && std::is_same<Dst, int32_t>::value ?
      constexpr_clamp_cast_helper(
          float(src),
          kClampCastLowerBoundFloatToInt32F,
          kClampCastUpperBoundFloatToInt32F,
          L::min(),
          L::max()) :
      constexpr_clamp_cast_helper(
          float(src),
          0.0f,
          kClampCastUpperBoundFloatToUInt32F,
          L::min(),
          L::max());
            // clang-format on
        }

        template <typename ValT, typename NodeT>
        class csl_iterator;

        template <typename T>
        class SkipListNode
        {
            enum : uint16_t
            {
                IS_HEAD_NODE = 1,
                MARKED_FOR_REMOVAL = (1 << 1),
                FULLY_LINKED = (1 << 2),
            };

        public:
            using value_type = T;

            SkipListNode(const SkipListNode&) = delete;
            SkipListNode& operator=(const SkipListNode&) = delete;

            template <
                typename NodeAlloc,
                typename U,
                typename = typename std::enable_if<std::is_convertible<U, T>::value>::type> // NOLINT
            static SkipListNode* create(
                NodeAlloc& alloc, int height, U&& data, bool isHead = false)
            {
                size_t size =
                    sizeof(SkipListNode) + (height * sizeof(std::atomic<SkipListNode*>));
                auto storage = std::allocator_traits<NodeAlloc>::allocate(alloc, size);
                // do placement new
                return new(storage)
                    SkipListNode(uint8_t(height), std::forward<U>(data), isHead);
            }

            template <typename NodeAlloc>
            static void destroy(NodeAlloc& alloc, SkipListNode* node)
            {
                size_t size = sizeof(SkipListNode) +
                              (node->height_ * sizeof(std::atomic<SkipListNode*>));
                node->~SkipListNode();
                std::allocator_traits<NodeAlloc>::deallocate(
                    alloc, typename std::allocator_traits<NodeAlloc>::pointer(node), size);
            }

            template <typename NodeAlloc>
            struct DestroyIsNoOp :
                // : StrictConjunction<
                //   AllocatorHasTrivialDeallocate<NodeAlloc>,
                std::is_trivially_destructible<SkipListNode>
            {
            };

            // copy the head node to a new head node assuming lock acquired
            SkipListNode* copyHead(SkipListNode* node)
            {
                setFlags(node->getFlags());
                for(uint8_t i = 0; i < node->height_; ++i)
                {
                    setSkip(i, node->skip(i));
                }
                return this;
            }

            inline SkipListNode* skip(int layer) const
            {
                return skip_[layer].load(std::memory_order_acquire);
            }

            // next valid node as in the linked list
            SkipListNode* next()
            {
                SkipListNode* node;
                for(node = skip(0); (node != nullptr && node->markedForRemoval());
                    node = node->skip(0))
                {
                }
                return node;
            }

            void setSkip(uint8_t h, SkipListNode* next)
            {
                skip_[h].store(next, std::memory_order_release);
            }

            value_type& data() { return data_; }
            const value_type& data() const { return data_; }
            int maxLayer() const { return height_ - 1; }
            int height() const { return height_; }

            std::unique_lock<MicroSpinLock> acquireGuard()
            {
                return std::unique_lock<MicroSpinLock>(spinLock_);
            }

            bool fullyLinked() const { return getFlags() & FULLY_LINKED; }
            bool markedForRemoval() const { return getFlags() & MARKED_FOR_REMOVAL; }
            bool isHeadNode() const { return getFlags() & IS_HEAD_NODE; }

            void setIsHeadNode() { setFlags(uint16_t(getFlags() | IS_HEAD_NODE)); }
            void setFullyLinked() { setFlags(uint16_t(getFlags() | FULLY_LINKED)); }
            void setMarkedForRemoval()
            {
                setFlags(uint16_t(getFlags() | MARKED_FOR_REMOVAL));
            }

        private:
            // Note! this can only be called from create() as a placement new.
            template <typename U>
            SkipListNode(uint8_t height, U&& data, bool isHead)
                : height_(height), data_(std::forward<U>(data))
            {
                spinLock_.init();
                setFlags(0);
                if(isHead)
                {
                    setIsHeadNode();
                }
                // need to explicitly init the dynamic atomic pointer array
                for(uint8_t i = 0; i < height_; ++i)
                {
                    new(&skip_[i]) std::atomic<SkipListNode*>(nullptr);
                }
            }

            ~SkipListNode()
            {
                for(uint8_t i = 0; i < height_; ++i)
                {
                    skip_[i].~atomic();
                }
            }

            uint16_t getFlags() const { return flags_.load(std::memory_order_acquire); }
            void setFlags(uint16_t flags)
            {
                flags_.store(flags, std::memory_order_release);
            }

            // TODO(xliu): on x86_64, it's possible to squeeze these into
            // skip_[0] to maybe save 8 bytes depending on the data alignments.
            // NOTE: currently this is x86_64 only anyway, due to the
            // MicroSpinLock.
            std::atomic<uint16_t> flags_;
            const uint8_t height_;
            MicroSpinLock spinLock_;

            value_type data_;

            std::atomic<SkipListNode*> skip_[0];
        };

        class SkipListRandomHeight
        {
            enum
            {
                kMaxHeight = 64
            };

        public:
            // make it a singleton.
            static SkipListRandomHeight* instance()
            {
                static SkipListRandomHeight instance_;
                return &instance_;
            }

            int getHeight(int maxHeight) const
            {
                double p = randomProb();
                for(int i = 0; i < maxHeight; ++i)
                {
                    if(p < lookupTable_[i])
                    {
                        return i + 1;
                    }
                }
                return maxHeight;
            }

            size_t getSizeLimit(int height) const
            {
                return sizeLimitTable_[height];
            }

        private:
            SkipListRandomHeight() { initLookupTable(); }

            void initLookupTable()
            {
                // set skip prob = 1/E
                static const double kProbInv = exp(1);
                static const double kProb = 1.0 / kProbInv;
                static const size_t kMaxSizeLimit = std::numeric_limits<size_t>::max();

                double sizeLimit = 1;
                double p = lookupTable_[0] = (1 - kProb);
                sizeLimitTable_[0] = 1;
                for(int i = 1; i < kMaxHeight - 1; ++i)
                {
                    p *= kProb;
                    sizeLimit *= kProbInv;
                    lookupTable_[i] = lookupTable_[i - 1] + p;
                    sizeLimitTable_[i] = constexpr_clamp_cast<size_t>(sizeLimit);
                }
                lookupTable_[kMaxHeight - 1] = 1;
                sizeLimitTable_[kMaxHeight - 1] = kMaxSizeLimit;
            }

            static double randomProb()
            {
                static thread_local std::mt19937 generator{std::random_device{}()};
                static thread_local std::uniform_real_distribution<double> distribution(0.0, 1.0);
                return distribution(generator);
            }

            double lookupTable_[kMaxHeight];
            size_t sizeLimitTable_[kMaxHeight];
        };

        template <typename NodeType, typename NodeAlloc, typename = void>
        class NodeRecycler;

        template <typename NodeType, typename NodeAlloc>
        class NodeRecycler<
            NodeType,
            NodeAlloc,
            typename std::enable_if<
                !NodeType::template DestroyIsNoOp<NodeAlloc>::value>::type>
        {
        public:
            explicit NodeRecycler(const NodeAlloc& alloc)
                : refs_(0), dirty_(false), alloc_(alloc)
            {
                lock_.init();
            }

            explicit NodeRecycler() : refs_(0), dirty_(false) { lock_.init(); }

            ~NodeRecycler()
            {
                CHECK_EQ(refs(), 0);
                if(nodes_)
                {
                    for(auto& node : *nodes_)
                    {
                        NodeType::destroy(alloc_, node);
                    }
                }
            }

            void add(NodeType* node)
            {
                std::lock_guard g(lock_);
                if(nodes_.get() == nullptr)
                {
                    nodes_ = std::make_unique<std::vector<NodeType*>>(1, node);
                }
                else
                {
                    nodes_->push_back(node);
                }
                DCHECK_GT(refs(), 0);
                dirty_.store(true, std::memory_order_relaxed);
            }

            int addRef() { return refs_.fetch_add(1, std::memory_order_acq_rel); }

            int releaseRef()
            {
                // This if statement is purely an optimization. It's possible that this
                // misses an opportunity to delete, but that's OK, we'll try again at
                // the next opportunity. It does not harm the thread safety. For this
                // reason, we can use relaxed loads to make the decision.
                if(!dirty_.load(std::memory_order_relaxed) || refs() > 1)
                {
                    return refs_.fetch_add(-1, std::memory_order_acq_rel);
                }

                std::unique_ptr<std::vector<NodeType*>> newNodes;
                int ret;
                {
                    // The order at which we lock, add, swap, is very important for
                    // correctness.
                    std::lock_guard g(lock_);
                    ret = refs_.fetch_add(-1, std::memory_order_acq_rel);
                    if(ret == 1)
                    {
                        // When releasing the last reference, it is safe to remove all the
                        // current nodes in the recycler, as we already acquired the lock here
                        // so no more new nodes can be added, even though new accessors may be
                        // added after this.
                        newNodes.swap(nodes_);
                        dirty_.store(false, std::memory_order_relaxed);
                    }
                }
                // TODO(xliu) should we spawn a thread to do this when there are large
                // number of nodes in the recycler?
                if(newNodes)
                {
                    for(auto& node : *newNodes)
                    {
                        NodeType::destroy(alloc_, node);
                    }
                }
                return ret;
            }

            NodeAlloc& alloc() { return alloc_; }

        private:
            int refs() const { return refs_.load(std::memory_order_relaxed); }

            std::unique_ptr<std::vector<NodeType*>> nodes_;
            std::atomic<int32_t> refs_; // current number of visitors to the list
            std::atomic<bool> dirty_;   // whether *nodes_ is non-empty
            MicroSpinLock lock_;        // protects access to *nodes_
            NodeAlloc alloc_;
        };

        // In case of arena allocator, no recycling is necessary, and it's possible
        // to save on ConcurrentSkipList size.
        template <typename NodeType, typename NodeAlloc>
        class NodeRecycler<
            NodeType,
            NodeAlloc,
            typename std::enable_if<
                NodeType::template DestroyIsNoOp<NodeAlloc>::value>::type>
        {
        public:
            explicit NodeRecycler(const NodeAlloc& alloc) : alloc_(alloc) {}

            void addRef() {}
            void releaseRef() {}

            void add(NodeType* /* node */) {}

            NodeAlloc& alloc() { return alloc_; }

        private:
            NodeAlloc alloc_;
        };

    } // namespace detail
} // namespace folly

// NOLINTEND