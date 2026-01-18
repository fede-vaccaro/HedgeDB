#pragma once

#include <cstdint>
#include <optional>

#include <error.hpp>

#include "fs/file_reader.h"
#include "mailbox_impl.h"
#include "types.h"
#include "utils.h"

namespace hedge::db
{

    class rolling_buffer
    {
        // Type aliases for clarity
        using byte_buffer_t = std::vector<uint8_t>;            ///< Underlying storage type (raw bytes).
        using span_t = std::span<index_entry_t>;               ///< A non-owning view of the buffer interpreted as index entries.
        using iterator_t = std::span<index_entry_t>::iterator; ///< Iterator over the index entry view.
        using buffer_iterator_t = byte_buffer_t::iterator;     ///< Iterator over the underlying byte buffer.

        byte_buffer_t _buffer; ///< The internal buffer holding raw byte data read from the file.
        span_t _view;          ///< A span providing typed access (`index_entry_t`) to the current `_buffer`.
        iterator_t _it;        ///< The current read position within the `_view`.

    public:
        /**
         * @brief Constructs a rolling_buffer.
         * @param reader An rvalue reference to an `fs::file_reader` which will be used to fetch data.
         */
        rolling_buffer(size_t initial_capacity)
        {
            this->_buffer.reserve(initial_capacity);
            this->_view = view_as<index_entry_t>(this->_buffer);
            this->_it = this->_view.begin();
        }

        /**
         * @brief Gets a reference to the current iterator position within the index entry view.
         * @return Reference to the `iterator_t`.
         */
        iterator_t& it()
        {
            return this->_it;
        }

        /**
         * @brief Gets the end iterator for the current view of index entries.
         * @return `iterator_t` pointing past the last valid entry in the current buffer view.
         */
        iterator_t end()
        {
            return this->_view.end();
        }

        /**
         * @brief Checks if the buffer is empty AND the underlying file reader has reached EOF.
         * @return `true` if all data has been read and consumed, `false` otherwise.
         */
        bool empty()
        {
            // Only truly EOF if the buffer is empty AND the reader confirms no more data in the file.
            return this->_buffer.empty();
        }

        void consume_and_push(std::span<uint8_t> read_response)
        {
            // Append the newly read data to the rolling buffer.
            this->_consume_and_push(read_response);
        }

        [[nodiscard]] size_t items_left() const
        {
            return this->_view.end() - this->_it;
        }

    private:
        /**
         * @brief Calculates the corresponding iterator position in the underlying byte buffer.
         * @return `buffer_iterator_t` pointing to the start byte of the element `_it` points to.
         */
        buffer_iterator_t _buffer_it()
        {
            return buffer_iterator_t(reinterpret_cast<uint8_t*>(this->_it.base()));
        }

        /**
         * @brief Updates the internal buffer: removes consumed bytes and appends new bytes.
         * @param new_buffer An rvalue reference to the newly read byte buffer.
         */
        void _consume_and_push(std::span<uint8_t> new_buffer_view)
        {
            prof::get<"consume_and_push">().start();
            /*
                What happens here:
                1. Erase the portion of the buffer that has already been processed (from begin up to the current byte iterator position).
                2. Append the newly read data to the end of the buffer.
                3. Update the span `_view` to reflect the potentially changed buffer content and size.
                4. Reset the read iterator `this->_it`, since this->_view has changed.
            */

            // size_t distance_to_end = std::distance(this->_it, this->_view.end());
            this->_buffer.erase(this->_buffer.begin(), this->_buffer_it());
            this->_buffer.insert(this->_buffer.end(), new_buffer_view.begin(), new_buffer_view.end());
            this->_view = view_as<index_entry_t>(this->_buffer);

            // if(!std::is_sorted(this->_view.begin(), this->_view.begin() + distance_to_end + 1))
            // {
            //     std::cout << "ROLLING BUFFER NOT SORTED!\n";
            //     std::cout << "Distance to end: " << distance_to_end << "\n";
            //     // for(auto& entry : std::span<index_entry_t>{this->_view.begin(), this->_view.begin() + distance_to_end + 1})
            //     //     std::cout << entry.key.to_string() << "\n";
            //     myassert(false, "Rolling buffer is not sorted!");
            // }

            this->_it = this->_view.begin();
            prof::get<"consume_and_push">().stop();
        }
    };

    /**
     * @brief A state machine designed to handle key deduplication during a merge of two sorted streams.
     * @details This class acts as a 1-item lookahead buffer. When merging two sorted sources (e.g., `left` and `right`),
     * the same key might appear in both. This class ensures that only the entry with the highest "priority"
     * (determined by `value_ptr_t::operator<`, which prioritizes newer entries) for a given key is emitted.
     *
     * Logic:
     * 1. `push(item)`: Accepts a new item from the merged stream.
     * - If it's the first item ever, it's stored internally (`this->_to_be_checked_item`).
     * - If its key matches the stored item's key, the one with higher priority (lower `value_ptr_t`) is kept.
     * - If its key is *different* from the stored item's key, the stored item is considered complete for its key group.
     *   Then, the stored item is moved to `this->_ready_item`, making `ready()` return true, and the new item becomes the `this->_to_be_checked_item`.
     * 2. `ready()`: Returns true if a key group has been finalized and moved to `this->_ready_item`.
     * 3. `pop()`: Returns the finalized item from `this->_ready_item` and clears it. Throws if not `ready()`.
     * 4. `force_pop()`: Returns the item currently stored in `this->_to_be_checked_item`. Used at the very end of the merge
     *     to retrieve the last processed item. Throws if `this->_ready_item` still holds data or if `this->_to_be_checked_item` is empty.
     *
     * This ensures correct deduplication even when key groups span across buffer boundaries handled by `rolling_buffer`.
     */
    class merge_iterator
    {
        std::optional<index_entry_t> _to_be_checked_item{}; ///< Holds the current lowest-priority candidate for the ongoing key group.
        std::optional<index_entry_t> _ready_item{};         ///< Holds the finalized item from the *previous* key group, ready to be emitted.

    public:
        /** @brief Default constructor. Initializes with empty state. */
        merge_iterator() = default;

        /**
         * @brief Pushes a new item into the deduplicator, updating state based on key comparison.
         * @param new_item The next `index_entry_t` from the merged input stream.
         */
        void push(index_entry_t new_item)
        {
            if(!this->_to_be_checked_item) // If this is the very first item pushed, just store it.
            {
                this->_to_be_checked_item = new_item;
            }
            else if(this->_to_be_checked_item->key != new_item.key) // If the new item's key is different from the buffered item's key...
            {
                // ...it means the key group for `this->_to_be_checked_item` is complete.
                // Move the completed item to `this->_ready_item` using std::exchange,
                // and store the `new_item` as the start of the next potential key group.
                this->_ready_item = std::exchange(this->_to_be_checked_item, new_item);
            }
            else if(new_item.value_ptr < this->_to_be_checked_item->value_ptr) // If the new item's key is the SAME as the buffered item's key...
            {
                this->_to_be_checked_item = new_item; // Keep the one with higher priority (lower value_ptr means higher priority/newer).
            }
            // else: new_item has same key but lower priority, so we discard it by doing nothing.
        }

        /**
         * @brief Checks if an item is finalized and ready to be popped.
         * @return `true` if `this->_ready_item` holds a value, `false` otherwise.
         */
        [[nodiscard]] bool ready() const // Added const qualifier
        {
            return this->_ready_item.has_value();
        }

        /**
         * @brief Retrieves the last buffered item, intended for use only at the very end of the stream.
         * @details This should only be called after processing all input and after checking/popping any `this->_ready_item`.
         * It retrieves the item representing the final key group.
         * @return The `index_entry_t` stored in `this->_to_be_checked_item`.
         * @throws std::runtime_error If `this->_ready_item` still contains data (should have been popped)
         * or if `this->_to_be_checked_item` is unexpectedly empty.
         */
        index_entry_t force_pop()
        {
            if(this->_ready_item.has_value()) // Ensure no item is waiting in the ready slot. In case, the ready item should be properly handled.
                throw std::runtime_error("Ready item still present, cannot force_pop last buffered item");

            if(!this->_to_be_checked_item.has_value()) // Ensure there is actually an item buffered.
                throw std::runtime_error("No buffered item to force_pop");

            return this->_to_be_checked_item.value();
        }

        /**
         * @brief Retrieves the finalized item from the ready slot.
         * @return The `index_entry_t` that was stored in `_ready_item`.
         * @throws std::runtime_error If `ready()` is false (no item is ready to be popped).
         */
        index_entry_t pop()
        {
            if(this->_ready_item.has_value())
                return std::exchange(this->_ready_item, std::nullopt).value();

            throw std::runtime_error("No ready item to pop"); // Throw if pop() is called when not ready().
        }
    };

} // namespace hedge::db