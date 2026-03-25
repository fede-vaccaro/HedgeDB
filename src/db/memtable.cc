#include <atomic>
#include <bits/types/struct_iovec.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <emmintrin.h>
#include <filesystem>
#include <future>
#include <memory>
#include <queue>
#include <ranges>
#include <thread>
#include <unistd.h>

#include "db/folly/concurrent_skip_list/micro_spin_lock.h"
#include "error.hpp"
#include "fs/fs.hpp"
#include "generator.h"
#include "index_ops.h"
#include "io_executor.h"
#include "key.h"
#include "mailbox_impl.h"
#include "memtable.h"
#include "spinlock.h"
#include "types.h"

namespace
{
    struct wal_entry
    {
        size_t epoch;
        uint32_t seq_nr;
        hedge::key_t key;
        std::vector<uint8_t> value;
    };

    hedge::async::generator<wal_entry> read_wal_file(const std::filesystem::path& path, size_t epoch)
    {
        auto maybe_file = hedge::fs::file::from_path(path, hedge::fs::file::open_mode::read_only, false);
        if(!maybe_file)
            co_return;

        auto& file = maybe_file.value();
        size_t file_size = file.file_size();
        if(file_size == 0)
            co_return;

        std::vector<uint8_t> buffer(file_size);
        ssize_t bytes_read = ::pread(file.fd(), buffer.data(), file_size, 0);
        if(bytes_read <= 0)
            co_return;

        size_t pos = 0;
        size_t end = static_cast<size_t>(bytes_read);

        while(pos < end)
        {
            // Need at least: seq_nr(4) + encoded_key_size(1)
            if(pos + sizeof(uint32_t) + sizeof(uint8_t) > end)
                break;

            uint32_t seq_nr;
            std::memcpy(&seq_nr, buffer.data() + pos, sizeof(uint32_t));
            pos += sizeof(uint32_t);

            uint8_t encoded_key_size = buffer[pos];
            pos += sizeof(uint8_t);

            size_t key_size = hedge::decode_key_size(encoded_key_size);
            if(key_size > hedge::MAX_KEY_LEN || pos + key_size > end)
                break;

            hedge::key_t key(buffer.data() + pos, key_size);
            pos += key_size;

            if(pos + sizeof(uint16_t) > end)
                break;

            uint16_t value_size;
            std::memcpy(&value_size, buffer.data() + pos, sizeof(uint16_t));
            pos += sizeof(uint16_t);

            if(pos + value_size > end)
                break;

            std::vector<uint8_t> value(buffer.data() + pos, buffer.data() + pos + value_size);
            pos += value_size;

            co_yield {.epoch = epoch,
                      .seq_nr = seq_nr,
                      .key = std::move(key),
                      .value = std::move(value)};
        }
    }
} // anonymous namespace

namespace hedge::db
{

    std::pair<bool, uint32_t> memtable_impl3_t::insert(const key_t& key, std::span<const uint8_t> value)
    {
        try
        {
            auto res = this->_accessor.insert(memtable_entry(key, value));
            if(!res.second)
            {
                // Key exists, update value
                // value in memtable_entry is mutable
                std::atomic_ref(res.first->value).store(value, std::memory_order_release);
            }
            return {true, std::atomic_ref<uint32_t>(this->seq_nr).fetch_add(1, std::memory_order::relaxed)};
        }
        catch(const std::bad_alloc&)
        {
            return {false, 0};
        }
    }

    std::optional<std::span<const uint8_t>> memtable_impl3_t::get(const key_t& key) const
    {
        Accessor acc(const_cast<memtable_impl3_t*>(this));
        auto it = acc.find(memtable_entry(key, {}));
        if(it != acc.end())
        {
            auto value = std::atomic_ref<std::span<const uint8_t>>(it->value);
            return value.load(std::memory_order_acquire);
        }
        return std::nullopt;
    }

    memtable::memtable(const memtable_config& cfg,
                       size_t num_partition_exponent,
                       std::filesystem::path indices_path,
                       std::atomic_size_t* flush_epoch_ptr,
                       std::function<void(std::vector<sst>)> push_new_indices,
                       std::function<void()> trigger_compaction_callback,
                       std::shared_ptr<db::sharded_page_cache> page_cache,
                       std::vector<std::shared_ptr<async::executor_context>> flush_executor_pool,
                       std::vector<async::executor_context*> writer_executors,
                       std::atomic_bool* compaction_backpressure)
        : _cfg(cfg),
          _num_partition_exponent(num_partition_exponent),
          _indices_path(std::move(indices_path)),
          _flush_epoch(flush_epoch_ptr),
          _push_new_indices(std::move(push_new_indices)),
          _trigger_compaction_callback(std::move(trigger_compaction_callback)),
          _compaction_backpressure(compaction_backpressure),
          _cache(std::move(page_cache)),
          _table(writer_executors),
          _pending_flushes_cv(writer_executors),
          _flush_executor_pool(std::move(flush_executor_pool)),
          _logger("memtable")
    {
        this->_table.store(this->make_memtable());
        this->_pipelined_table = this->make_memtable();
    }

    memtable::~memtable()
    {
        this->_running.store(false, std::memory_order::relaxed);
        this->_pipelined_table.store(nullptr, std::memory_order::relaxed);
        this->_pipelined_table.notify_one();

        if(this->_table_maker.joinable())
            this->_table_maker.join();
    }

    async::task<hedge::status> memtable::_append_to_wal(int32_t fd, uint32_t seq_nr, const key_t& key, std::span<const uint8_t> value)
    {
        uint8_t encoded_key_size = hedge::encode_key_size(key.size());
        auto value_size = static_cast<uint16_t>(value.size());

        static_assert(sizeof(seq_nr) == sizeof(uint32_t));

        std::array<iovec, 5> wal_entry{
            iovec{
                .iov_base = &seq_nr,
                .iov_len = sizeof(uint32_t)},
            iovec{
                .iov_base = &encoded_key_size,
                .iov_len = sizeof(uint8_t),
            },
            iovec{
                .iov_base = const_cast<uint8_t*>(key.data()),
                .iov_len = key.size(),
            },
            iovec{
                .iov_base = &value_size,
                .iov_len = sizeof(uint16_t),
            },
            iovec{
                .iov_base = const_cast<uint8_t*>(value.data()),
                .iov_len = value.size(),
            }};

        const size_t expected_bytes = sizeof(uint32_t) + sizeof(uint8_t) + key.size() + sizeof(uint16_t) + value.size();

        int32_t res = pwritev2(fd, wal_entry.data(), wal_entry.size(), -1 /* File is opened with O_APPEND */, 0);
        // auto res = co_await async::this_thread_executor()->submit_request(async::writev_request{
        //                                                                       .fd = fd,
        //                                                                       .iovecs = wal_entry.data(),
        //                                                                       .iovecs_count = static_cast<int>(wal_entry.size()),
        //                                                                       .offset = 0, // File is opened with O_APPEND
        //                                                                   },
        //                                                                   async::request_priority::HIGH);



        if(res < 0)
            co_return hedge::error("could not write into wal: " + std::string(strerror(errno)));

        if(size_t(res) != expected_bytes)
            co_return hedge::error("partial write into wal: " + std::to_string(res) + " != " + std::to_string(expected_bytes));

        co_return hedge::ok();
    }

    async::task<hedge::status> memtable::put_async(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type)
    {
        // Loading from an atomic shared every time is slow AF
        // Basically a thread-local cache
        thread_local std::shared_ptr<rw_sync_table_t> local_memtable_ref = this->_table.load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        auto insert_attempts = 0UL;

        while(true)
        {

            // Using modulo to map the thread index to the number of available counters.
            // THIS_THREAD_IDX is a monotonic counter, so this handles the case where
            // the number of lifetime threads exceeds the number of writer slots.
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]] // The memtable has been frozen (from the flusher), try loading the new one
            {
                auto t = this->_table.load(std::memory_order::relaxed);
                if(t == local_memtable_ref) // Backpressure
                {
                    constexpr size_t ATTEMPTS_BEFORE_BACKPRESSURE = 0;

                    if(insert_attempts++ < ATTEMPTS_BEFORE_BACKPRESSURE)
                    {
                        folly::detail::asm_volatile_pause();
                        continue;
                    }

                    BACKPRESSURE.fetch_add(1, std::memory_order::relaxed);
                    co_await this->_table.wait(local_memtable_ref, std::memory_order::relaxed);
                    local_memtable_ref = this->_table.load(std::memory_order::relaxed);
                    insert_attempts = 0;
                    continue;
                }

                local_memtable_ref = t;
                insert_attempts = 0;
                continue;
            }

            // auto value_span = memtable->value_arena.allocate(value.size() + 1);
            constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;
            auto* value_ptr = memtable->value_arenas[THIS_THREAD_IDX % this->_cfg.num_writer_threads]->allocate_many(value.size() + 1, alignment);
            // bool ok = !value_span.empty(); // nullptr means out of memory budget
            bool ok = value_ptr != nullptr; // nullptr means out of memory budget
            std::span<uint8_t> value_span(value_ptr, value.size() + 1);
            uint32_t seq_nr;

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
                    folly::detail::asm_volatile_pause();
                continue;
            }

            // OK
            if(!this->_cfg.use_wal)
                co_return hedge::ok();

            co_return co_await this->_append_to_wal(memtable->per_thread_wals[THIS_THREAD_IDX].fd(), seq_nr, key, value_span);
        }
    }

    hedge::status memtable::put(const key_t& key, std::span<const uint8_t> value, hedge::value_type value_type)
    {
        // Loading from an atomic shared every time is slow AF
        // Basically a thread-local cache
        thread_local std::shared_ptr<rw_sync_table_t> local_memtable_ref = this->_table.load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        auto insert_attempts = 0UL;

        while(true)
        {

            // Using modulo to map the thread index to the number of available counters.
            // THIS_THREAD_IDX is a monotonic counter, so this handles the case where
            // the number of lifetime threads exceeds the number of writer slots.
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]] // The memtable has been frozen (from the flusher), try loading the new one
            {
                auto t = this->_table.load(std::memory_order::relaxed);
                if(t == local_memtable_ref) // Backpressure
                {

                    constexpr size_t ATTEMPTS_BEFORE_BACKPRESSURE = 4;

                    if(insert_attempts++ < ATTEMPTS_BEFORE_BACKPRESSURE)
                    {
                        folly::detail::asm_volatile_pause();
                    }
                    else
                    {
                        BACKPRESSURE.fetch_add(1, std::memory_order::relaxed);
                        // this->_table.wait(t, std::memory_order::relaxed);
                        // std::this_thread::yield();
                        constexpr timespec ns = {.tv_sec = 0, .tv_nsec = 1};
                        nanosleep(&ns, nullptr);
                    }
                    continue;
                }

                local_memtable_ref = t;
                insert_attempts = 0;
                continue;
            }

            // auto value_span = memtable->value_arena.allocate(value.size() + 1);
            constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;
            auto* value_ptr = memtable->value_arenas[THIS_THREAD_IDX % this->_cfg.num_writer_threads]->allocate_many(value.size() + 1, alignment);
            // bool ok = !value_span.empty(); // nullptr means out of memory budget
            bool ok = value_ptr != nullptr; // nullptr means out of memory budget
            std::span<uint8_t> value_span(value_ptr, value.size() + 1);
            uint32_t seq_nr;

            if(ok)
            {
                value_span[0] = static_cast<uint8_t>(value_type);
                std::memcpy(value_span.data() + 1, value.data(), value.size());
                std::tie(ok, seq_nr) = memtable->insert(key, value_span); // returns false if memtable run out of memory (budget)
            }

            if(!ok || memtable->bytes_written.fetch_add(key.size() + value.size() + 1, std::memory_order_relaxed) > this->_cfg.memory_budget_cap) [[unlikely]]
            {
                assert(false && "sync put does not support flush (use put_async)");
                continue;
            }

            // OK
            if(!this->_cfg.use_wal)
                return hedge::ok();

            // return co_await this->_append_to_wal(memtable->per_thread_wals[THIS_THREAD_IDX].fd(), seq_nr, key, value_span);
        }
    }

    hedge::status memtable::_append_batch_to_wal(
        int32_t fd,
        std::span<const std::pair<key_t, std::vector<uint8_t>>> entries,
        std::span<const wal_batch_meta> meta,
        std::span<const std::span<const uint8_t>> value_spans)
    {
        constexpr size_t IOVECS_PER_ENTRY = 5;
        const size_t n = entries.size();

        std::array<iovec, 128 * IOVECS_PER_ENTRY> iovecs;
        size_t expected_bytes = 0;

        for(size_t i = 0; i < n; ++i)
        {
            const size_t base = i * IOVECS_PER_ENTRY;
            const auto& [key, _] = entries[i];

            iovecs[base + 0] = {.iov_base = const_cast<uint32_t*>(&meta[i].seq_nr), .iov_len = sizeof(uint32_t)};
            iovecs[base + 1] = {.iov_base = const_cast<uint8_t*>(&meta[i].encoded_key_size), .iov_len = sizeof(uint8_t)};
            iovecs[base + 2] = {.iov_base = const_cast<uint8_t*>(key.data()), .iov_len = key.size()};
            iovecs[base + 3] = {.iov_base = const_cast<uint16_t*>(&meta[i].value_size), .iov_len = sizeof(uint16_t)};
            iovecs[base + 4] = {.iov_base = const_cast<uint8_t*>(value_spans[i].data()), .iov_len = value_spans[i].size()};

            expected_bytes += sizeof(uint32_t) + sizeof(uint8_t) + key.size() + sizeof(uint16_t) + value_spans[i].size();
        }

        int32_t res = pwritev2(fd, iovecs.data(), static_cast<int>(n * IOVECS_PER_ENTRY), -1, 0);
        if(res < 0)
            return hedge::error("could not write batch into wal: " + std::string(strerror(errno)));

        if(static_cast<size_t>(res) != expected_bytes)
            return hedge::error("partial batch write into wal: " + std::to_string(res) + " != " + std::to_string(expected_bytes));

        return hedge::ok();
    }

    async::task<hedge::status> memtable::put_batch_async(
        std::span<const std::pair<key_t, std::vector<uint8_t>>> entries,
        hedge::value_type value_type)
    {
        thread_local std::shared_ptr<rw_sync_table_t> local_memtable_ref = this->_table.load(std::memory_order::relaxed);

        static std::atomic_size_t THREADS{0};
        thread_local std::atomic_size_t THIS_THREAD_IDX = THREADS.fetch_add(1, std::memory_order::relaxed);
        auto insert_attempts = 0UL;

        const size_t n = entries.size();

        while(true)
        {
            auto memtable = local_memtable_ref->acquire_writer(THIS_THREAD_IDX % this->_cfg.num_writer_threads);

            if(!memtable) [[unlikely]]
            {
                auto t = this->_table.load(std::memory_order::relaxed);
                if(t == local_memtable_ref)
                {
                    constexpr size_t ATTEMPTS_BEFORE_BACKPRESSURE = 4;

                    if(insert_attempts++ < ATTEMPTS_BEFORE_BACKPRESSURE)
                    {
                        folly::detail::asm_volatile_pause();
                        continue;
                    }

                    BACKPRESSURE.fetch_add(1, std::memory_order::relaxed);
                    co_await this->_table.wait(local_memtable_ref, std::memory_order::relaxed);
                    local_memtable_ref = this->_table.load(std::memory_order::relaxed);
                    insert_attempts = 0;
                    continue;
                }

                local_memtable_ref = t;
                insert_attempts = 0;
                continue;
            }

            constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;
            const size_t writer_idx = THIS_THREAD_IDX % this->_cfg.num_writer_threads;

            std::array<uint32_t, 128> seq_nrs{};
            std::array<std::span<const uint8_t>, 128> value_spans{};
            size_t total_bytes = 0;
            bool all_ok = true;

            for(size_t i = 0; i < n; ++i)
            {
                const auto& [key, value] = entries[i];

                auto* value_ptr = memtable->value_arenas[writer_idx]->allocate_many(value.size() + 1, alignment);
                if(!value_ptr)
                {
                    all_ok = false;
                    break;
                }

                std::span<uint8_t> value_span(value_ptr, value.size() + 1);
                value_span[0] = static_cast<uint8_t>(value_type);
                std::memcpy(value_span.data() + 1, value.data(), value.size());

                auto [ok, seq_nr] = memtable->insert(key, value_span);
                if(!ok)
                {
                    all_ok = false;
                    break;
                }

                seq_nrs[i] = seq_nr;
                value_spans[i] = value_span;
                total_bytes += key.size() + value.size() + 1;
            }

            if(!all_ok || memtable->bytes_written.fetch_add(total_bytes, std::memory_order_relaxed) > this->_cfg.memory_budget_cap) [[unlikely]]
            {
                bool this_thread_flushed = co_await this->_flush(local_memtable_ref);
                if(!this_thread_flushed)
                    folly::detail::asm_volatile_pause();
                continue;
            }

            if(!this->_cfg.use_wal)
                co_return hedge::ok();

            std::array<wal_batch_meta, 128> meta{};
            for(size_t i = 0; i < n; ++i)
            {
                meta[i] = {
                    .seq_nr = seq_nrs[i],
                    .encoded_key_size = hedge::encode_key_size(entries[i].first.size()),
                    .value_size = static_cast<uint16_t>(value_spans[i].size()),
                };
            }

            co_return this->_append_batch_to_wal(
                memtable->per_thread_wals[THIS_THREAD_IDX].fd(),
                entries, meta, std::span(value_spans.data(), n));
        }
    }

    std::optional<value_t> memtable::get(const key_t& key) const
    {
        thread_local size_t table_switch_epoch = this->_table_switch_epoch.load(std::memory_order_acquire);
        thread_local auto local_memtable_ref = this->_table.load(std::memory_order_relaxed);

        if(auto curr_epoch = this->_table_switch_epoch.load(std::memory_order::acquire); curr_epoch > table_switch_epoch)
        {
            table_switch_epoch = curr_epoch;
            local_memtable_ref = this->_table.load(std::memory_order::relaxed);
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

    async::task<bool> memtable::_flush(rw_sync_table_ptr_t expected_table)
    {
        bool expected = false;

        // Only one thread can enter here
        if(this->_flush_mutex.compare_exchange_strong(expected, true))
        {
            // Guard: if the table was already swapped by a concurrent flush, bail out
            if(this->_table.load(std::memory_order::relaxed) != expected_table)
            {
                this->_flush_mutex.store(false);
                co_return false;
            }

            size_t curr_flush_epoch = this->_flush_epoch->fetch_add(1, std::memory_order::relaxed);
            rw_sync_table_ptr_t memtable_to_flush{};

            expected_table->freeze_writes();

            {
                std::unique_lock lk(this->_pending_flushes_mutex);

                co_await this->_pending_flushes_cv.wait(lk, [this]()
                                                        { 
                                                            // if(this->_pending_flushes.size() >= MAX_PENDING_FLUSHES)
                                                                // std::cout << "Stalling pending flushes: " << this->_pending_flushes.size() << std::endl;

                                                            return this->_pending_flushes.size() < MAX_PENDING_FLUSHES; });

                rw_sync_table_ptr_t next_in_pipeline = this->_pipelined_table.exchange(nullptr);

                memtable_to_flush = this->_table.exchange(next_in_pipeline, std::memory_order::relaxed);
                this->_pending_flushes.insert({curr_flush_epoch, memtable_to_flush});
            }

            co_await this->_table.notify_all();

            // Publish new memtable to readers
            this->_table_switch_epoch.fetch_add(1, std::memory_order::release);

            this->_pipelined_table.store(this->make_memtable(), std::memory_order_relaxed);

            // Launch flush job
            this->_flusher.submit(
                [this, curr_flush_epoch, memtable_to_flush]()
                {
                    // this->_logger.log("Flushing mem index, epoch: ", curr_flush_epoch, " size: ", memtable_to_flush->ptr()->size());
                    auto t0 = std::chrono::high_resolution_clock::now();

                    while(memtable_to_flush->any_active_writer()) // Wait until every writer is done with the object
                        std::this_thread::yield();

                    // auto partitioned_sorted_indices = index_ops::flush_mem_index2(
                    //     this->_indices_path,
                    //     memtable_to_flush.get()->ptr(),
                    //     this->_num_partition_exponent,
                    //     curr_flush_epoch,
                    //     this->_cache,
                    //     this->_cfg.use_odirect,
                    //     this->_flush_worker_pool,
                    //     this->_cfg.fdatasync_flushed_sst);

                    auto partitioned_sorted_indices = index_ops::flush_mem_index2_parallel(
                        this->_indices_path,
                        memtable_to_flush.get()->ptr(),
                        this->_num_partition_exponent,
                        curr_flush_epoch,
                        this->_cache,
                        this->_cfg.use_odirect,
                        this->_flush_executor_pool,
                        this->_cfg.fdatasync_flushed_sst);

                    if(!partitioned_sorted_indices.has_value())
                    {
                        this->_logger.log("could not flush memtable: ", partitioned_sorted_indices.error().to_string());
                        return;
                    }

                    // Push the new indices to the DB, to make them visible as Sorted indices
                    bool should_notify_flush = false;

                    std::vector<fs::file> wals;

                    {
                        std::unique_lock lk(this->_pending_flushes_mutex);
                        this->_push_new_indices(std::move(partitioned_sorted_indices.value()));
                        auto it = this->_pending_flushes.find(curr_flush_epoch);
                        assert(it->second == memtable_to_flush);
                        wals = std::move(it->second->ptr()->per_thread_wals);
                        this->_pending_flushes.erase(it);
                        should_notify_flush = this->_pending_flushes.size() <= MAX_PENDING_FLUSHES / 2;
                    }

                    if(should_notify_flush)
                        this->_pending_flushes_cv.notify_all(this->_flusher.ring());

                    bool under_pressure = this->_compaction_backpressure && this->_compaction_backpressure->load(std::memory_order::relaxed);
                    if(under_pressure)
                    {
                        this->_logger.log("Flush completed for epoch ", curr_flush_epoch, " but compaction backpressure is active, waiting...");
                        this->_compaction_backpressure->wait(true, std::memory_order::relaxed);
                    }

                    if(this->_cfg.auto_compaction)
                        this->_trigger_compaction_callback();

                    std::ranges::for_each(wals, [](fs::file& wal)
                                          { std::filesystem::remove(wal.path()); });
                    wals.clear();

                    auto t1 = std::chrono::high_resolution_clock::now();
                    [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
                    [[maybe_unused]] double throughput = (double)memtable_to_flush->ptr()->size() / (duration.count() / 1'000'000.0);
                    // this->_logger.log("Mem index flushed ", memtable_to_flush->ptr()->size(), " keys in ", (double)duration.count() / 1000.0, " ms", " throughput: ", throughput, " keys/s");
                });

            // Release mutex
            this->_flush_mutex.store(false);
            co_return true;
        }

        co_return false;
    }

    hedge::status memtable::replay_wal()
    {
        if(!std::filesystem::exists(this->_indices_path))
            return hedge::ok();

        // Collect non-empty WAL files with their epochs
        struct wal_file_info
        {
            std::filesystem::path path;
            size_t epoch;
        };

        std::vector<wal_file_info> wal_files;

        for(const auto& entry : std::filesystem::directory_iterator(this->_indices_path))
        {
            if(!entry.is_regular_file())
                continue;

            auto fname = entry.path().filename().string();
            if(!fname.starts_with("wal."))
                continue;

            // Skip empty files (belong to freshly created memtable)
            if(entry.file_size() == 0)
                continue;

            auto last_dot = fname.rfind('.');
            if(last_dot == std::string::npos)
                continue;

            size_t epoch = std::stoull(fname.substr(last_dot + 1));
            wal_files.push_back({entry.path(), epoch});
        }

        if(wal_files.empty())
            return hedge::ok();

        // Collect all entries from all WAL files
        std::vector<wal_entry> all_entries;

        for(auto& wf : wal_files)
        {
            auto gen = read_wal_file(wf.path, wf.epoch);
            for(auto& entry : gen)
            {
                all_entries.push_back(std::move(entry));
            }
        }

        if(all_entries.empty())
        {
            // Delete WAL files even if they had no valid entries
            for(auto& wf : wal_files)
            {
                auto f = fs::file::from_path(wf.path, fs::file::open_mode::read_only, false);
                if(f)
                    std::filesystem::remove(f.value().path());
            }
            return hedge::ok();
        }

        // Sort by (epoch, seq_nr) for chronological order
        std::sort(all_entries.begin(), all_entries.end(),
                  [](const wal_entry& a, const wal_entry& b)
                  {
                      if(a.epoch != b.epoch)
                          return a.epoch < b.epoch;
                      return a.seq_nr < b.seq_nr;
                  });

        // Insert into current memtable
        auto table_ptr = this->_table.load(std::memory_order::relaxed);
        auto* memtable = table_ptr->ptr();

        constexpr size_t alignment = std::atomic_ref<std::span<const uint8_t>>::required_alignment;

        size_t replayed = 0;

        for(auto& entry : all_entries)
        {
            auto* value_ptr = memtable->value_arenas[0]->allocate_many(entry.value.size(), alignment);
            if(value_ptr == nullptr)
            {
                this->_logger.log("WAL replay: arena exhausted after ", replayed, " entries");
                break;
            }

            std::memcpy(value_ptr, entry.value.data(), entry.value.size());
            std::span<const uint8_t> value_span(value_ptr, entry.value.size());

            memtable->insert(entry.key, value_span);

            ++replayed;
        }

        this->_logger.log("WAL replay: replayed ", replayed, " entries from ", wal_files.size(), " WAL files");

        // Delete replayed WAL files
        for(auto& wf : wal_files)
        {
            auto f = fs::file::from_path(wf.path, fs::file::open_mode::read_only, false);
            if(f)
                std::filesystem::remove(f.value().path());
        }

        return hedge::ok();
    }

} // namespace hedge::db
