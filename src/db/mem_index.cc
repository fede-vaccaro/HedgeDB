#include <atomic>
#include <chrono>
#include <future>
#include <ranges>
#include <thread>

#include "index_ops.h"
#include "mem_index.h"
namespace hedge::db
{

    memtable::memtable(const memtable_config& cfg,
                       size_t num_partition_exponent,
                       std::filesystem::path indices_path,
                       std::atomic_size_t* flush_epoch_ptr,
                       std::function<void(std::vector<sorted_index>)> push_new_indices,
                       std::function<void()> trigger_compaction_callback,
                       std::shared_ptr<db::shared_page_cache> page_cache)
        : _cfg(cfg),
          _num_partition_exponent(num_partition_exponent),
          _indices_path(std::move(indices_path)),
          _flush_epoch(flush_epoch_ptr),
          _push_new_indices(std::move(push_new_indices)),
          _trigger_compaction_callback(std::move(trigger_compaction_callback)),
          _cache(std::move(page_cache)),
          _logger("MEMTABLE")
    {
        this->_table = new memtable_impl_t(this->_cfg.memory_budget_cap);
        this->_pipelined_table = new memtable_impl_t(this->_cfg.memory_budget_cap);
    }

    void memtable::put(const key_t& key, const value_ptr_t& value)
    {
        while(true)
        {
            auto* table = this->_table.load(std::memory_order::relaxed);

            if(table == nullptr)
                continue;

            bool ok = table->insert(key, value); // returns false if Out of Memory Budget

            if(!ok || table->size() >= this->_cfg.max_inserts_cap)
            {
                bool flush_ok = this->_flush();
                if(!flush_ok)
                    std::this_thread::yield();
            }
            else
                return;
        }
    }

    std::optional<value_ptr_t> memtable::get(const key_t& key)
    {
        auto v = this->_table.load(std::memory_order::relaxed)->get(key);

        if(v)
            return v;

        // Check pending flushes
        decltype(this->_pending_flushes) pending_flushes;

        {
            std::shared_lock lk(this->_pending_flushes_mutex);
            pending_flushes = this->_pending_flushes;
        }

        // Check pending flushes starting from most recent
        for(auto& pending_flush : std::ranges::reverse_view(pending_flushes))
        {
            auto& pending_memtable = pending_flush.second;
            v = pending_memtable->get(key);
            if(v)
                return v;
        }

        return std::nullopt;
    }

    std::future<void> memtable::wait_for_flush()
    {
        auto promise_ptr = std::make_shared<std::promise<void>>();
        auto future = promise_ptr->get_future();

        size_t num_pending_flushes;
        {
            std::shared_lock lk(this->_pending_flushes_mutex);
            num_pending_flushes = this->_pending_flushes.size();
        }

        if(num_pending_flushes != 0)
        {
            // The worker has a FIFO queue; so when the future will be set,
            // every flush until this submission will be done
            this->_flusher.submit([promise_ptr]() mutable
                                  { promise_ptr->set_value(); });
        }
        else
        {
            promise_ptr->set_value();
        }

        return future;
    }

    bool memtable::_flush()
    {
        // this->_pipelined_table.wait(nullptr, std::memory_order::relaxed);
        memtable_impl_t* next_in_pipeline = this->_pipelined_table.load(std::memory_order::relaxed);

        // CAS loop
        if(next_in_pipeline != nullptr && this->_pipelined_table.compare_exchange_strong(next_in_pipeline, nullptr))
        {
            size_t curr_flush_epoch = this->_flush_epoch->fetch_add(1, std::memory_order::relaxed);
            memtable_impl_t* memtable_to_flush{};

            { // Won the race, actually trigger flush
                std::unique_lock lk(this->_pending_flushes_mutex);

                // Using raw pointers could be unsafe as there might be some writers holding the pointer
                // I could use a custom solution or atomic shared ptrs
                auto curr_shared_ptr = std::shared_ptr<frozen_memtable_impl_t>(
                    // Also, using frozen_memtable_implt_t is potentially dangerous too:
                    // What if there is still an active writer while someone else triggers a flush?
                    reinterpret_cast<frozen_memtable_impl_t*>(memtable_to_flush = this->_table.exchange(next_in_pipeline)) // That's the "swap chain": in flush <- (curr) _table <- _pipelined_table; now this->_table is usable again
                );

                this->_pending_flushes.insert_or_assign(curr_flush_epoch, std::move(curr_shared_ptr));
            }

            // Launch flush job
            this->_flusher.submit(
                [this, curr_flush_epoch, frozen_memtable_to_flush = reinterpret_cast<frozen_memtable_impl_t*>(memtable_to_flush)]()
                {
                    this->_logger.log("Flushing mem index");
                    auto t0 = std::chrono::high_resolution_clock::now();
                    auto* new_next = new memtable_impl_t(this->_cfg.memory_budget_cap);

                    // Update pipelined
                    // But skip if it's not needed
                    memtable_impl_t* expected = nullptr;
                    if(!this->_pipelined_table.compare_exchange_strong(expected, new_next))
                        delete new_next;

                    // index_ops::flush_memindex
                    auto partitioned_sorted_indices = index_ops::flush_mem_index(this->_indices_path,
                                                                                 frozen_memtable_to_flush,
                                                                                 this->_num_partition_exponent,
                                                                                 curr_flush_epoch,
                                                                                 this->_cache,
                                                                                 this->_cfg.use_odirect);

                    if(!partitioned_sorted_indices.has_value())
                    {
                        this->_logger.log("could not flush memtable: ", partitioned_sorted_indices.error().to_string());
                        return;
                    }

                    // Push the new indices to the DB, to make them visible as Sorted indices
                    {
                        std::unique_lock lk(this->_pending_flushes_mutex);
                        this->_push_new_indices(std::move(partitioned_sorted_indices.value()));
                        auto it = this->_pending_flushes.find(curr_flush_epoch);
                        assert(it->second.get() == frozen_memtable_to_flush);
                        this->_pending_flushes.erase(it);
                    }

                    if(this->_cfg.auto_compaction)
                        this->_trigger_compaction_callback();

                    auto t1 = std::chrono::high_resolution_clock::now();
                    [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                    this->_logger.log("Mem index flushed in ", (double)duration.count() / 1000.0, " ms");
                });

            return true;
        }

        return false;
    }

} // namespace hedge::db
