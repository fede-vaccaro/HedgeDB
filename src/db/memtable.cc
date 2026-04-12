#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <emmintrin.h>
#include <filesystem>
#include <future>
#include <memory>
#include <ranges>
#include <shared_mutex>
#include <thread>

#include "db/skiplist/concurrent_skip_list/micro_spin_lock.h"
#include "error.hpp"
#include "index_ops.h"
#include "io/io_executor.h"
#include "key.h"
#include "memtable.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/aw_resume_on.hpp"
#include "tmc/ex_cpu_st.hpp"
#include "tmc/spawn.hpp"
#include "types.h"
#include "wal.h"

namespace hedge::db
{

    std::pair<bool, uint64_t> memtable_impl3_t::insert(const key_t& key, std::span<const uint8_t> value)
    {
        try
        {
            auto seq = this->_seq_nr->fetch_add(1, std::memory_order::relaxed);
            this->_accessor.insert(memtable_entry(key, seq, value));
            return {true, seq};
        }
        catch(const std::bad_alloc&)
        {
            return {false, 0};
        }
    }

    std::optional<std::span<const uint8_t>> memtable_impl3_t::get(const key_t& key) const
    {
        Accessor acc(const_cast<memtable_impl3_t*>(this));
        auto it = acc.lower_bound(memtable_entry(key, UINT64_MAX, {}));
        if(it != acc.end() && it->_key == key)
            return it->_value;
        return std::nullopt;
    }

    memtable::memtable(const memtable_config& cfg,
                       size_t num_partition_exponent,
                       std::filesystem::path indices_path,
                       std::atomic_size_t* flush_epoch_ptr,
                       std::shared_ptr<io::io_executor> flusher_executor,
                       std::function<tmc::task<void>(std::vector<sst>)> push_new_indices,
                       std::function<void()> trigger_compaction_callback,
                       std::shared_ptr<db::sharded_page_cache> page_cache,
                       tmc::atomic_condvar<bool>* compaction_backpressure)
        : _cfg(cfg),
          _num_partition_exponent(num_partition_exponent),
          _indices_path(std::move(indices_path)),
          _flush_epoch(flush_epoch_ptr),
          _push_new_ssts_callback(std::move(push_new_indices)),
          _schedule_compaction_callback(std::move(trigger_compaction_callback)),
          _compaction_backpressure(compaction_backpressure),
          _cache(std::move(page_cache)),
          _wal_epoch(cfg.starting_wal_epoch),
          _pending_flush_slots(cfg.max_pending_flushes),
          _flusher(std::make_unique<tmc::ex_cpu_st>()),
          _flush_executor(std::move(flusher_executor)),
          _logger("memtable")
    {
        this->_flusher->init();
        // this->_flush_executors = std::make_unique<io::io_executor>(this->_cfg.flush_io_workers, 32);

        this->_table.ref()
            .store(this->_make_memtable());
        this->_pipelined_table = this->_make_memtable();
    }

    memtable::~memtable()
    {
        this->_running.store(false, std::memory_order::relaxed);
        this->_pipelined_table.store(nullptr, std::memory_order::relaxed);
        this->_pipelined_table.notify_one();
    }

    std::shared_ptr<memtable::rw_sync_table_t> memtable::_make_memtable()
    {
        return std::make_shared<rw_sync_table_t>(
            this->_cfg.num_writer_threads,
            &this->_seq_nr,
            this->_indices_path,
            this->_cfg.use_wal,
            this->_wal_epoch.fetch_add(1, std::memory_order::relaxed),
            this->_cfg.memory_budget_cap,
            this->_cfg.num_writer_threads,
            this->_cfg.memory_budget_cap);
    }

    tmc::task<hedge::status> memtable::put_async(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type)
    {
        // Loading from an atomic shared every time is slow AF
        // Basically a thread-local cache
        thread_local std::shared_ptr<rw_sync_table_t> local_memtable_ref = this->_table.ref().load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        auto insert_attempts = 0UL;

        while(true)
        {
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]] // The memtable has been frozen (from the flusher), try loading the new one
            {
                auto t = this->_table.ref().load(std::memory_order::relaxed);
                if(t == local_memtable_ref) // Backpressure
                {
                    constexpr size_t ATTEMPTS_BEFORE_BACKPRESSURE = 8;

                    if(insert_attempts++ < ATTEMPTS_BEFORE_BACKPRESSURE)
                    {
                        ::third_party::folly::detail::asm_volatile_pause();
                        continue;
                    }

                    BACKPRESSURE.fetch_add(1, std::memory_order::relaxed);
                    co_await this->_table.await(local_memtable_ref);
                    local_memtable_ref = this->_table.ref().load(std::memory_order::relaxed);
                    insert_attempts = 0;
                    continue;
                }

                local_memtable_ref = std::move(t);
                insert_attempts = 0;
                continue;
            }

            // auto value_span = memtable->value_arena.allocate(value.size() + 1);
            constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;
            auto* value_ptr = memtable->value_arenas[THIS_THREAD_IDX % this->_cfg.num_writer_threads]->allocate_many(value.size() + 1, alignment);
            // bool ok = !value_span.empty(); // nullptr means out of memory budget
            bool ok = value_ptr != nullptr; // nullptr means out of memory budget
            std::span<uint8_t> value_span(value_ptr, value.size() + 1);
            uint64_t seq_nr;

            if(ok)
            {
                value_span[0] = static_cast<uint8_t>(value_type);
                std::memcpy(value_span.data() + 1, value.data(), value.size());
                std::tie(ok, seq_nr) = memtable->insert(key, value_span); // returns false if memtable run out of memory (budget)
            }

            if(!ok || memtable->bytes_written.fetch_add(key.size() + value.size() + 1, std::memory_order_relaxed) > this->_cfg.memory_budget_cap) [[unlikely]]
            {
                bool this_thread_flushed = co_await this->_flush(local_memtable_ref);
                if(!this_thread_flushed)
                    ::third_party::folly::detail::asm_volatile_pause();
                continue;
            }

            // OK
            if(!this->_cfg.use_wal)
                co_return hedge::ok();

            co_return memtable->_wal->append(THIS_THREAD_IDX % this->_cfg.num_writer_threads, seq_nr, key, value_span);
        }
    }

    std::optional<value_t> memtable::get(const key_t& key) const
    {
        thread_local size_t table_switch_epoch = this->_table_switch_epoch.load(std::memory_order_acquire);
        thread_local auto local_memtable_ref = this->_table.ref().load(std::memory_order_relaxed);

        if(auto curr_epoch = this->_table_switch_epoch.load(std::memory_order::acquire); curr_epoch > table_switch_epoch)
        {
            table_switch_epoch = curr_epoch;
            local_memtable_ref = this->_table.ref().load(std::memory_order::relaxed);
        }

        auto v = local_memtable_ref->ptr()->get(key);

        if(v)
        {
            auto value = value_from_span(v.value());
            if(value.has_error())
                throw std::runtime_error("Corrupted value in memtable for key " + to_hex_string(key) + ": " + value.error().to_string());

            return std::move(value.value());
        }
        // // Check pending flushes
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
        std::shared_lock lk(this->_pending_flushes_mutex);
        this->_pending_flushes_cv_sync.wait(lk, [this]
                                            { return this->_pending_flushes.empty(); });

        std::promise<void> promise;
        promise.set_value();
        return promise.get_future();
    }

    tmc::task<bool> memtable::_flush(rw_sync_table_ptr_t expected_table)
    {
        bool expected = false;

        // Only one thread can enter here
        if(this->_flush_mutex.compare_exchange_strong(expected, true))
        {
            // Guard: if the table was already swapped by a concurrent flush, bail out
            if(this->_table.ref().load(std::memory_order::relaxed) != expected_table)
            {
                this->_flush_mutex.store(false);
                co_return false;
            }

            size_t curr_flush_epoch = this->_flush_epoch->fetch_add(1, std::memory_order::relaxed);
            rw_sync_table_ptr_t memtable_to_flush{};

            expected_table->freeze_writes();

            // Acquire a flush slot (blocks if max_pending_flushes in flight)
            co_await this->_pending_flush_slots;

            {
                std::unique_lock lk(this->_pending_flushes_mutex);
                rw_sync_table_ptr_t next_in_pipeline = this->_pipelined_table.exchange(nullptr);
                memtable_to_flush = this->_table.ref().exchange(next_in_pipeline, std::memory_order::relaxed);
                this->_pending_flushes.insert({curr_flush_epoch, memtable_to_flush});
            }

            this->_table.notify_all();

            // Publish new memtable to readers
            this->_table_switch_epoch.fetch_add(1, std::memory_order::release);

            this->_pipelined_table.store(this->_make_memtable(), std::memory_order_relaxed);

            // Launch flush job on the flusher executor
            tmc::spawn(this->_flush_inner(curr_flush_epoch, memtable_to_flush))
                // .with_priority(0)
                .run_on(*this->_flusher)
                .detach();

            // Release mutex
            this->_flush_mutex.store(false);
            co_return true;
        }

        co_return false;
    }

    tmc::task<void> memtable::_flush_inner(size_t curr_flush_epoch, rw_sync_table_ptr_t memtable_to_flush)
    {
        // The braid must be initialized on the same executor it will run on
        if(!this->_braid.has_value())
            this->_braid.emplace();

        co_await tmc::resume_on(*this->_braid);

        // std::cout << "curr_flush_epoch: " << curr_flush_epoch << "\n";

        auto t0 = std::chrono::high_resolution_clock::now();

        while(memtable_to_flush->any_active_writer()) // Wait until every writer is done with the object
            std::this_thread::yield();

        auto partitioned_sorted_indices = co_await index_ops::flush_mem_index2_parallel(
            this->_indices_path,
            memtable_to_flush.get()->ptr(),
            this->_num_partition_exponent,
            curr_flush_epoch,
            this->_cache,
            this->_cfg.use_odirect,
            this->_flush_executor->ex(),
            this->_cfg.fdatasync_flushed_sst);

        if(!partitioned_sorted_indices.has_value())
        {
            this->_logger.log("could not flush memtable: ", partitioned_sorted_indices.error().to_string());
            this->_pending_flush_slots.release();
            co_return;
        }

        tmc::task<void> persist_indices{};

        {
            std::unique_lock lk(this->_pending_flushes_mutex);
            persist_indices = this->_push_new_ssts_callback(std::move(partitioned_sorted_indices.value()));
            auto it = this->_pending_flushes.find(curr_flush_epoch);
            assert(it->second == memtable_to_flush);
            this->_pending_flushes.erase(it);
            this->_pending_flushes_cv_sync.notify_all();
        }

        // Avoids co_awaiting while holding lock
        co_await std::move(persist_indices);

        bool under_pressure = (this->_compaction_backpressure != nullptr) && this->_compaction_backpressure->ref().load(std::memory_order::relaxed);
        if(under_pressure)
        {
            // this->_logger.log("Flush completed for epoch ", curr_flush_epoch, " but compaction backpressure is active, waiting...");
            co_await this->_compaction_backpressure->await(true);
        }

        // Release the flush slot so the next flush can proceed
        this->_pending_flush_slots.release();

        if(this->_cfg.auto_compaction)
            this->_schedule_compaction_callback();

        if(auto& w = memtable_to_flush->ptr()->_wal; w.has_value())
            w->remove();

        auto t1 = std::chrono::high_resolution_clock::now();
        [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
        [[maybe_unused]] double throughput = (double)memtable_to_flush->ptr()->size() / (duration.count() / 1'000'000.0);
        co_return;
    }

    hedge::status memtable::replay_wal()
    {
        auto table_ptr = this->_table.ref().load(std::memory_order::relaxed);
        auto* mt = table_ptr->ptr();

        constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;
        size_t replayed = 0;
        uint64_t max_seq_nr = 0;

        auto status = wal::replay(
            this->_indices_path,
            [&](const key_t& key, std::span<const uint8_t> value, uint64_t seq_nr) -> bool
            {
                auto* ptr = mt->value_arenas[0]->allocate_many(value.size(), alignment);
                if(!ptr)
                    throw std::runtime_error("WAL replay: arena exhausted after " + std::to_string(replayed) + " entries — data loss prevented");
                std::memcpy(ptr, value.data(), value.size());
                mt->insert(key, {ptr, value.size()});
                max_seq_nr = std::max(max_seq_nr, seq_nr);
                ++replayed;
                return true;
            },
            this->_logger);

        if(replayed > 0)
            this->_seq_nr.store(max_seq_nr + 1, std::memory_order::relaxed);

        return status;
    }

    memtable::snapshot memtable::acquire_snapshot()
    {
        auto curr_memtable = this->_table.ref().load(std::memory_order::relaxed);
        size_t curr_seq_nr = curr_memtable->ptr()->seq_nr();

        std::shared_lock lk(this->_pending_flushes_mutex);

        return memtable::snapshot{
            .seq_nr = curr_seq_nr,
            .curr = std::move(curr_memtable),
            .pending_flushes = this->_pending_flushes};
    };

} // namespace hedge::db
