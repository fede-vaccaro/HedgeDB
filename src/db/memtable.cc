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

#include "error.hpp"
#include "index_ops.h"
#include "io/io_executor.h"
#include "key.h"
#include "logger.h"
#include "memtable.h"
#include "tmc/atomic_condvar.hpp"
#include "tmc/channel.hpp"
#include "tmc/ex_cpu_st.hpp"
#include "tmc/semaphore.hpp"
#include "tmc/spawn.hpp"
#include "tmc/sync.hpp"
#include "types.h"
#include "wal.h"

namespace hedge::db
{

    std::pair<bool, uint64_t> skiplist_wrapper::insert(const key_t& key, std::span<const std::byte> value)
    {
        try
        {
            auto seq = this->_seq_nr->fetch_add(1, std::memory_order::relaxed);
            [[maybe_unused]] auto [it, ok] = this->_accessor.insert(memtable_entry(key, seq, value));
            assert(ok);
            return {true, seq};
        }
        catch(const std::bad_alloc&)
        {
            return {false, 0};
        }
    }

    std::optional<std::span<const std::byte>> skiplist_wrapper::get(const key_t& key) const
    {
        Accessor acc(const_cast<skiplist_wrapper*>(this));
        auto it = acc.lower_bound(memtable_entry(key, UINT64_MAX, {}));
        if(it != acc.end() && it->_key == key)
            return it->_value;
        return std::nullopt;
    }

    hedge::expected<std::optional<std::filesystem::path>> move_old_wal_files(const std::filesystem::path& wal_dir)
    {
        auto old_wal_files = wal::collect_wal_filenames(wal_dir);
        if(old_wal_files.empty())
            return std::nullopt;

        std::array<std::byte, 16> uuid_bytes;

        thread_local std::mt19937_64 gen{std::random_device{}()};
        thread_local std::uniform_int_distribution<uint8_t> dist;

        std::ranges::for_each(uuid_bytes, [&](std::byte& b)
                              { b = static_cast<std::byte>(dist(gen)); });

        std::string old_wal_dirname = hedge::to_hex_string(uuid_bytes);
        auto old_wal_dir_path = wal_dir / old_wal_dirname;

        bool ok = std::filesystem::create_directories(old_wal_dir_path);
        if(!ok)
            return hedge::error("could not create directory: " + old_wal_dir_path.string());

        for(auto& wal : old_wal_files)
        {
            std::error_code ec;
            std::filesystem::rename(wal, old_wal_dir_path / wal.filename(), ec);

            if(ec)
                return hedge::error("error while moving wal file " + wal.string() + " :" + ec.message());
        }

        return old_wal_dir_path;
    }

    memtable::memtable(const memtable_config& cfg,
                       size_t num_partition_exponent,
                       std::filesystem::path indices_path,
                       std::atomic_size_t* flush_epoch_ptr,
                       std::shared_ptr<io::io_executor> flusher_executor,
                       std::function<tmc::task<void>(std::vector<sst>, std::optional<compaction_stats>)> push_new_ssts_callback,
                       std::function<void()> schedule_compaction_callback,
                       std::shared_ptr<db::sharded_page_cache> page_cache,
                       tmc::atomic_condvar<bool>* compaction_backpressure)
        : _cfg(cfg),
          _num_partition_exponent(num_partition_exponent),
          _indices_path(std::move(indices_path)),
          _flush_epoch(flush_epoch_ptr),
          _push_new_ssts_callback(std::move(push_new_ssts_callback)),
          _schedule_compaction_callback(std::move(schedule_compaction_callback)),
          _compaction_backpressure(compaction_backpressure),
          _cache(std::move(page_cache)),
          _wal_ch(tmc::make_channel<std::unique_ptr<wal>>()),
          _pending_flush_slots(cfg.max_pending_flushes),
          _flusher(std::make_unique<tmc::ex_cpu_st>()),
          _flush_executor(std::move(flusher_executor)),
          _logger("memtable")
    {
        this->_flusher->init();

        if(cfg.use_wal)
        {
            int fd = ::open(this->_indices_path.c_str(), O_RDONLY | O_DIRECTORY);
            if(fd >= 0)
                this->_wal_dir_fd = fd;
            else
                throw std::runtime_error("Failed to open WAL directory: " + std::string(std::strerror(errno)));

            // Pre-allocate one WAL slot per simultaneously in-flight memtable:
            //   1 active + 1 pipelined + max_pending_flushes pending
            const size_t n_slots = cfg.max_pending_flushes + 2;
            const size_t file_size_hint = cfg.memory_budget_cap / std::max(cfg.num_writer_threads, size_t{1});

            // Before creating the new WALs check if there is anything to recover
            auto maybe_old_wals_path = move_old_wal_files(this->_indices_path);
            if(!maybe_old_wals_path)
                throw std::runtime_error(std::format("Could not read wal files from {}: {}", this->_indices_path.string(), maybe_old_wals_path.error().to_string()));

            this->_old_wals_path = maybe_old_wals_path.value().value_or(std::filesystem::path{});
            if(!this->_old_wals_path.empty())
                this->_logger.log("Old WAL files moved to: ", this->_old_wals_path);

            for(const auto i : std::views::iota(size_t{0}, n_slots))
            {
                auto reusable_wal = std::make_unique<wal>(wal::config{
                    .base_path = this->_indices_path,
                    .slot_idx = i,
                    .n_threads = cfg.num_writer_threads,
                    .file_size_hint = file_size_hint});

                this->_wal_ch.post(std::move(reusable_wal));
            }

            ::fdatasync(*this->_wal_dir_fd);
        }

        tmc::post_waitable(
            *this->_flush_executor,
            [](memtable* self) -> tmc::task<void>
            {
                auto ch_tok = self->_wal_ch.new_token();
                self->_table.ref().store(co_await self->_make_memtable(ch_tok));
                self->_pipelined_table.ref().store(co_await self->_make_memtable(ch_tok));
            }(this))
            .wait();
    }

    memtable::~memtable()
    {
        this->_pipelined_table.ref().store(nullptr, std::memory_order::relaxed);
        this->_pipelined_table.notify_one();

        if(this->_wal_dir_fd)
            ::close(*this->_wal_dir_fd);
    }

    tmc::task<std::shared_ptr<memtable::rw_sync_buffer_t>> memtable::_make_memtable(tmc::chan_tok<std::unique_ptr<wal>>& ch_tok)
    {
        std::unique_ptr<wal> wal_slot;
        if(this->_cfg.use_wal)
        {
            wal_slot = (co_await ch_tok.pull()).value_or(nullptr);
            assert(wal_slot != nullptr);
        }

        co_return std::make_shared<rw_sync_buffer_t>(
            this->_cfg.num_writer_threads,
            &this->_seq_nr,
            this->_cfg.memory_budget_cap,
            this->_cfg.num_writer_threads,
            this->_cfg.memory_budget_cap,
            std::move(wal_slot));
    }

    tmc::task<hedge::status> memtable::put_async(const key_t& key, std::span<const std::byte> value, hedge::value_type value_type)
    {
        // Loading from an atomic shared every time is slow since it is spinlock-base (not lock-free)
        // This is a thread-local cache
        // TODO: thread_local might get tricky with work stealing
        thread_local std::shared_ptr<rw_sync_buffer_t> local_memtable_ref = this->_table.ref().load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        auto insert_attempts = 0UL;

        while(true)
        {
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]] // The memtable has been frozen (from the flusher), try loading the new one
            {
                auto t = this->_table.ref().load(std::memory_order::relaxed);
                if(t == local_memtable_ref) // Backpressure if the table is not ready
                {
                    constexpr size_t ATTEMPTS_BEFORE_BACKPRESSURE = 4;

                    if(insert_attempts++ < ATTEMPTS_BEFORE_BACKPRESSURE)
                    {
                        ::third_party::folly::detail::asm_volatile_pause();
                        continue;
                    }

                    HALT_COUNTER.fetch_add(1, std::memory_order::relaxed);

                    co_await this->_table.await(local_memtable_ref);

                    local_memtable_ref = this->_table.ref().load(std::memory_order::relaxed);
                    insert_attempts = 0;
                    continue;
                }

                local_memtable_ref = std::move(t);
                insert_attempts = 0;
                continue;
            }

            auto* value_ptr = memtable->value_arenas[THIS_THREAD_IDX % this->_cfg.num_writer_threads]->allocate_many(value.size() + 1, VALUE_DATA_ALIGNMENT);
            bool ok = value_ptr != nullptr; // nullptr means out of memory budget
            std::span<std::byte> value_span(value_ptr, value.size() + 1);
            uint64_t seq_nr;

            if(ok)
            {
                value_span[0] = static_cast<std::byte>(value_type);
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

    tmc::task<bool> memtable::_flush(rw_sync_buffer_ptr_t expected_table)
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
            rw_sync_buffer_ptr_t memtable_to_flush{};

            expected_table->freeze_writes();

            // Timing harness for writer-blocking sections (for profiling purposes)
            struct flush_latencies
            {
                std::chrono::microseconds acquire_flush_slot{};
                std::chrono::microseconds wait_pipelined{};
                std::chrono::microseconds lock_for_swap{};
                std::chrono::microseconds total_blocking{};
            } latencies;

            auto t0 = std::chrono::high_resolution_clock::now();
            auto t1 = t0;

            // Section 1: Acquire flush slot (backpressure from max_pending_flushes)
            t0 = std::chrono::high_resolution_clock::now();
            co_await this->_pending_flush_slots;
            t1 = std::chrono::high_resolution_clock::now();
            latencies.acquire_flush_slot = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

            // Section 2: Wait for pipelined table to be ready
            t0 = std::chrono::high_resolution_clock::now();
            co_await this->_pipelined_table.await(nullptr);
            t1 = std::chrono::high_resolution_clock::now();
            latencies.wait_pipelined = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

            // Section 3: Lock for table swap
            t0 = std::chrono::high_resolution_clock::now();
            {
                std::unique_lock lk(this->_pending_flushes_mutex);
                rw_sync_buffer_ptr_t next_in_pipeline = this->_pipelined_table.ref().exchange(nullptr);
                memtable_to_flush = this->_table.ref().exchange(next_in_pipeline, std::memory_order::relaxed);
                this->_pending_flushes.insert({curr_flush_epoch, memtable_to_flush});
            }
            t1 = std::chrono::high_resolution_clock::now();
            latencies.lock_for_swap = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

            // Calculate total blocking time
            latencies.total_blocking = latencies.acquire_flush_slot + latencies.wait_pipelined + latencies.lock_for_swap;

            // Log if total > 100ms or any section > 50ms
            constexpr bool ENABLE_FLUSH_LATENCY_LOGGING = false;
            constexpr auto TOTAL_THRESHOLD = std::chrono::microseconds(10000);  // 10ms
            constexpr auto SECTION_THRESHOLD = std::chrono::microseconds(5000); // 5ms

            if(ENABLE_FLUSH_LATENCY_LOGGING &&
               (latencies.total_blocking > TOTAL_THRESHOLD ||
                latencies.acquire_flush_slot > SECTION_THRESHOLD ||
                latencies.wait_pipelined > SECTION_THRESHOLD ||
                latencies.lock_for_swap > SECTION_THRESHOLD))
            {
                this->_logger.log("Flush #", curr_flush_epoch, " BLOCKING: total=", latencies.total_blocking.count(), "us | ",
                                  "slot=", latencies.acquire_flush_slot.count(), "us | ",
                                  "pipelined=", latencies.wait_pipelined.count(), "us | ",
                                  "lock=", latencies.lock_for_swap.count(), "us");
            }

            this->_table.notify_all();
            this->_table_switch_epoch.fetch_add(1, std::memory_order::release);

            tmc::spawn([](memtable* self) -> tmc::task<void>
                       {
                thread_local auto ch_tok = self->_wal_ch.new_token();

                self->_pipelined_table.ref().store(co_await self->_make_memtable(ch_tok), std::memory_order::relaxed);
                self->_pipelined_table.notify_one();
                co_return; }(this))
                .run_on(*this->_flusher)
                .detach();

            auto next_can_write = std::make_shared<tmc::semaphore>(0);
            auto can_write = std::exchange(this->_can_write, next_can_write);

            tmc::spawn(this->_flush_inner(curr_flush_epoch, memtable_to_flush, can_write, next_can_write))
                .run_on(*this->_flush_executor)
                .detach();

            // Release mutex
            this->_flush_mutex.store(false);
            co_return true;
        }

        co_return false;
    }

    tmc::task<void> memtable::_flush_inner(
        size_t curr_flush_epoch,
        rw_sync_buffer_ptr_t memtable_to_flush,
        std::shared_ptr<tmc::semaphore> can_write,
        std::shared_ptr<tmc::semaphore> next_can_write)
    {
        while(memtable_to_flush->any_active_writer()) // Wait until every writer is done with the object
            std::this_thread::yield();

        auto accessor = memtable_to_flush->ptr()->accessor();

        auto t0 = std::chrono::high_resolution_clock::now();
        // The flush procedure generates 2^num_partition_exponent SSTs (1 per partition)
        auto partitioned_sorted_indices = co_await index_ops::flush_memtable(
            this->_indices_path,
            accessor.cbegin(),
            accessor.cend(),
            this->_num_partition_exponent,
            curr_flush_epoch,
            this->_cache,
            this->_cfg.use_odirect,
            this->_flush_executor->ex());

        auto t1 = std::chrono::high_resolution_clock::now();

        if(!partitioned_sorted_indices.has_value())
        {
            this->_logger.log("could not flush memtable: ", partitioned_sorted_indices.error().to_string());
            this->_pending_flush_slots.release();
            co_return;
        }

        const size_t flushed_bytes = memtable_to_flush.get()->ptr()->bytes_written.load(std::memory_order::relaxed);

        [[maybe_unused]] compaction_stats stats{
            .input_bytes = flushed_bytes,
            .output_bytes = flushed_bytes,
            .num_inputs = 1,
            .items_merged = accessor.size(),
            .time_start = t0,
            .time_end = t1,
        };

        // Acquire permission to push (for respecting new SSTs chronological ordering)
        co_await *can_write;

        // Update LSM-tree with this->_push_new_ssts_callback
        tmc::task<void> update_manifest_callback{};
        {
            std::unique_lock lk(this->_pending_flushes_mutex);
            update_manifest_callback = this->_push_new_ssts_callback(std::move(partitioned_sorted_indices.value()), this->_cfg.acquire_flush_stats ? std::optional{stats} : std::nullopt);
            auto it = this->_pending_flushes.find(curr_flush_epoch);
            assert(it->second == memtable_to_flush);
            this->_pending_flushes.erase(it); // LSM-tree updated, can safely erase the memtable from memory
            this->_pending_flushes_cv_sync.notify_all();
        }

        // Give permission to the next flush
        next_can_write->release();

        // Avoids co_awaiting while holding lock
        co_await std::move(update_manifest_callback);

        // Check if the compactions area is putting pressure
        bool under_pressure = (this->_compaction_backpressure != nullptr) &&
                              this->_compaction_backpressure->ref().load(std::memory_order::relaxed);
        if(under_pressure)
        {
            co_await this->_compaction_backpressure->await(true);
        }

        // Release the flush slot (when there is no pressure from compaction)
        this->_pending_flush_slots.release();

        // Flush done: reset WAL files and return the slot to the pool
        if(auto& w = memtable_to_flush->ptr()->_wal; w)
        {
            w->reset();
            auto ch_tok = this->_wal_ch.new_token();
            [[maybe_unused]] bool ok = co_await ch_tok.push(std::move(w));
            assert(ok);
        }

        // Schedule compaction
        // NB behind the callback, the `sst_manager` might decide to do nothing,
        // if a compaction is not necessary
        if(this->_cfg.auto_compaction)
            this->_schedule_compaction_callback();

        co_return;
    }

    hedge::status memtable::replay_wal(std::optional<uint64_t> skip_up_to_seq_nr)
    {
        tmc::ex_cpu replay_executor;
        replay_executor.set_thread_count(this->_cfg.num_writer_threads);
        replay_executor.init();

        if(this->_old_wals_path.empty())
            return hedge::ok();

        if(skip_up_to_seq_nr.has_value())
            this->_seq_nr.store(skip_up_to_seq_nr.value() + 1);

        size_t i = 0;

        auto status = wal::replay(
            this->_old_wals_path,
            [&](const key_t& key, std::span<const std::byte> value, uint64_t seq_nr) -> bool
            {
                if(skip_up_to_seq_nr && seq_nr <= *skip_up_to_seq_nr)
                    return true; // already persisted in an SST

                auto value_type = static_cast<hedge::value_type>(value[0]);
                auto actual_value = value.subspan(1);

                auto f = tmc::post_waitable(
                             replay_executor,
                             this->put_async(key, actual_value, value_type),
                             0,
                             i++ % replay_executor.thread_count())
                             .get();
                if(!f)
                    throw std::runtime_error("failed to post put_async on wal replay: " + f.error().to_string());

                return true;
            },
            this->_logger);

        std::filesystem::remove_all(this->_old_wals_path);
        this->_logger.log("WAL replayed from ", this->_old_wals_path, "; deleting directory.");
        this->_old_wals_path = std::filesystem::path{};

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
