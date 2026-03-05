#include <atomic>
#include <chrono>
#include <cstdint>
#include <emmintrin.h>
#include <future>
#include <memory>
#include <ranges>
#include <thread>

#include "index_ops.h"
#include "memtable.h"
#include "types.h"

namespace hedge::db
{

    memtable::memtable(const memtable_config& cfg,
                       size_t num_partition_exponent,
                       std::filesystem::path indices_path,
                       std::atomic_size_t* flush_epoch_ptr,
                       std::function<void(std::vector<sst>)> push_new_indices,
                       std::function<void()> trigger_compaction_callback,
                       std::shared_ptr<db::sharded_page_cache> page_cache)
        : _cfg(cfg),
          _num_partition_exponent(num_partition_exponent),
          _indices_path(std::move(indices_path)),
          _flush_epoch(flush_epoch_ptr),
          _push_new_indices(std::move(push_new_indices)),
          _trigger_compaction_callback(std::move(trigger_compaction_callback)),
          _cache(std::move(page_cache)),
          _logger("memtable")
    {
        this->_table = this->make_memtable();
        this->_pipelined_table = this->make_memtable();

        this->_flush_executor_pool.resize(this->_cfg.flush_io_workers);
        for(auto& ex : this->_flush_executor_pool)
            ex = async::executor_context::make_new(32);
    }

    void memtable::put(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type)
    {
        // Loading from an atomic shared every time is slow AF
        // Basically a thread-local cache
        thread_local std::shared_ptr<rw_sync_table_t> local_memtable_ref = this->_table.load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        while(true)
        {
            auto attempts = 0;

            // Using modulo to map the thread index to the number of available counters.
            // THIS_THREAD_IDX is a monotonic counter, so this handles the case where
            // the number of lifetime threads exceeds the number of writer slots.
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]] // The memtable has been freezen, load the new one and retry
            {
                local_memtable_ref = this->_table.load(std::memory_order::relaxed);
                continue;
            }

            // auto* data_ptr = memtable->value_arenas[THIS_THREAD_IDX % this->_cfg.num_writer_threads]->allocate_many(value.size() + 1);
            auto value_span = memtable->value_arena.allocate(value.size() + 1);
            // std::span<uint8_t> value_span(data_ptr, value.size() + 1);
            bool ok = !value_span.empty(); // nullptr means out of memory budget

            if(ok)
            {
                value_span[0] = static_cast<uint8_t>(value_type);
                std::memcpy(value_span.data() + 1, value.data(), value.size());
                ok = memtable->insert(key, value_span); // returns false if memtable run out of memory (budget)
            }

            if(!ok) [[unlikely]]
            {
                bool this_thread_flushed = this->_flush();
                if(!this_thread_flushed)
                {
                    attempts++;
                    if(attempts < 16)
                        std::this_thread::yield();
                    else
                        this->_pipelined_table.wait(nullptr, std::memory_order::relaxed); // <-- backpressure here
                }
                continue;
            }

            return;
        }
    }

    std::optional<value_t> memtable::get(const key_t& key) const
    {
        const auto local_memtable_ref = this->_table.load(std::memory_order_relaxed);

        auto v = local_memtable_ref->ptr()->get(key);

        if(v)
        {
            auto value = value_from_span(v.value());
            if(value.has_error())
                throw std::runtime_error("Corrupted value in memtable for key " + to_hex_string(key) + ": " + value.error().to_string());

            return std::move(value.value());
        }
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
            v = pending_memtable->ptr()->get(key); // TODO: the pointer can be casted to the read only version of the memtable if we are sure that every writer is done
            if(v)
            {
                auto value = value_from_span(v.value());
                if(value.has_error())
                    throw std::runtime_error("Corrupted value in memtable for key " + to_hex_string(key) + ": " + value.error().to_string());

                return std::move(value.value());
            }
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

        if(num_pending_flushes > 0)
        {
            // The worker process jobs in a FIFO queue; so when the future will be set,
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
        auto next_in_pipeline = this->_pipelined_table.load(std::memory_order::relaxed);

        // CAS test
        if(next_in_pipeline != nullptr &&
           this->_pipelined_table.compare_exchange_strong(
               next_in_pipeline,
               nullptr,
               std::memory_order::relaxed,
               std::memory_order::relaxed))
        {
            size_t curr_flush_epoch = this->_flush_epoch->fetch_add(1, std::memory_order::relaxed);
            rw_sync_table_ptr_t memtable_to_flush{};

            { // Won the race, actually trigger flush
                std::unique_lock lk(this->_pending_flushes_mutex);

                this->_table.load(std::memory_order::relaxed)->freeze_writes();
                memtable_to_flush = this->_table.exchange(next_in_pipeline, std::memory_order::relaxed);

                this->_pending_flushes.insert_or_assign(curr_flush_epoch, memtable_to_flush);
            }

            // Launch flush job
            this->_flusher.submit(
                [this, curr_flush_epoch, memtable_to_flush]()
                {
                    // this->_logger.log("Flushing mem index, epoch: ", curr_flush_epoch, " size: ", memtable_to_flush->ptr()->size());
                    auto t0 = std::chrono::high_resolution_clock::now();
                    auto new_next = this->make_memtable();
                    auto t_new_next = std::chrono::high_resolution_clock::now();
                    [[maybe_unused]] auto duration_new_next = std::chrono::duration_cast<std::chrono::microseconds>(t_new_next - t0);
                    // this->_logger.log("Created new memtable for flush e
                    // poch ", curr_flush_epoch, " in ", (double)duration_new_next.count() / 1000.0, " ms");
                    // Update pipelined
                    // But skip if not needed
                    std::shared_ptr<rw_sync_table_t> expected = nullptr;
                    if(this->_pipelined_table.compare_exchange_strong(expected, new_next))
                    {
                        this->_pipelined_table.notify_all();
                    }
                    else
                    {
                        expected.reset();
                    }

                    while(memtable_to_flush->any_active_writer()) // Wait until every writer is done with the object
                        std::this_thread::yield();

                    auto pool = this->_flush_executor_pool;

                    auto partitioned_sorted_indices = index_ops::flush_mem_index2_parallel(
                        this->_indices_path,
                        memtable_to_flush.get()->ptr(),
                        this->_num_partition_exponent,
                        curr_flush_epoch,
                        this->_cache,
                        this->_cfg.use_odirect,
                        this->_flush_executor_pool);

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
                        assert(it->second == memtable_to_flush);
                        this->_pending_flushes.erase(it);
                    }

                    if(this->_cfg.auto_compaction)
                        this->_trigger_compaction_callback();

                    auto t1 = std::chrono::high_resolution_clock::now();
                    [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                    [[maybe_unused]] double throughput = (double)memtable_to_flush->ptr()->size() / (duration.count() / 1'000'000.0);
                    // this->_logger.log("Mem index flushed ", memtable_to_flush->ptr()->size(), " keys in ", (double)duration.count() / 1000.0, " ms", " throughput: ", throughput, " keys/s");
                });

            return true;
        }

        return false;
    }

} // namespace hedge::db
