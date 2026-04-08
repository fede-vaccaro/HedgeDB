#pragma once

#include <cstdint>
#include <optional>

#include <error.hpp>

#include "types.h"

namespace hedge::db
{
    using value_buffer_t = hedge::key<>;

    struct merge_entry_t
    {
        key_t key;
        value_buffer_t value;
        uint64_t epoch;
    };

    class deduplicator
    {
        std::optional<merge_entry_t> _to_be_checked_item{};
        std::optional<merge_entry_t> _ready_item{};
        size_t _deduplicated_keys{0};

    public:
        deduplicator() = default;

        void push(merge_entry_t&& new_item)
        {
            if(!this->_to_be_checked_item)
            {
                this->_to_be_checked_item = std::move(new_item);
            }
            else if(this->_to_be_checked_item->key != new_item.key)
            {
                this->_ready_item = std::exchange(this->_to_be_checked_item, std::move(new_item));
            }
            else
            {
                // Duplicate key: keep the existing one (from the newer SST, which was pushed first
                // due to heap ordering by epoch), discard the older duplicate.
                ++this->_deduplicated_keys;
            }
        }

        [[nodiscard]] size_t deduplicated_keys() const { return this->_deduplicated_keys; }

        /**
         * @brief Checks if an item is finalized and ready to be popped.
         * @return `true` if `this->_ready_item` holds a value, `false` otherwise.
         */
        [[nodiscard]] bool ready() const // Added const qualifier
        {
            return this->_ready_item.has_value();
        }

        merge_entry_t force_pop()
        {
            if(this->_ready_item.has_value()) // Ensure no item is waiting in the ready slot. In case, the ready item should be properly handled.
                throw std::runtime_error("Ready item still present, cannot force_pop last buffered item");

            if(!this->_to_be_checked_item.has_value()) // Ensure there is actually an item buffered.
                throw std::runtime_error("No buffered item to force_pop");

            return std::exchange(this->_to_be_checked_item, std::nullopt).value();
        }

        /**
         * @brief Retrieves the finalized item from the ready slot.
         * @return The `merge_entry_t` that was stored in `_ready_item`.
         * @throws std::runtime_error If `ready()` is false (no item is ready to be popped).
         */
        merge_entry_t pop()
        {
            if(this->_ready_item.has_value())
                return std::exchange(this->_ready_item, std::nullopt).value();

            throw std::runtime_error("No ready item to pop"); // Throw if pop() is called when not ready().
        }
    };

} // namespace hedge::db