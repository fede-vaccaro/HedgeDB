#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <map>
#include <random>
#include <ranges>
#include <sstream>
#include <vector>

#include <gtest/gtest.h>

#include "db/index_ops.h"
#include "db/memtable.h" // For memtable_impl3_t
#include "db/scan_iterator.h"
#include "db/sst.h"
#include "io/io_executor.h"
#include "single_buffer_arena_allocator.h"
#include "tmc/sync.hpp"
#include "types.h"
#include "utils.h"

std::string to_hex_string(const hedge::key_t& key)
{
    auto key_span = static_cast<std::span<const std::byte>>(key);
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for(std::byte c : key_span)
    {
        ss << std::setw(2) << static_cast<int>(c);
    }
    return ss.str();
}

// Shared value-generation and checking helpers for all scan test fixtures.
struct scan_test_helpers
{
    static constexpr size_t TEST_VALUE_SIZE = 100;

    hedge::key_t generate_key(size_t length)
    {
        auto k = hedge::key_t::make_with_length(length);
        auto span = static_cast<std::span<std::byte>>(k);
        for(size_t i = 0; i < length; ++i)
            span[i] = dist(generator);
        return k;
    }

    static void generate_inline_value(std::span<std::byte> buffer, size_t seed)
    {
        assert(buffer.size() == TEST_VALUE_SIZE);
        seed = seed * 31 + 17;
        auto byte_seed = static_cast<std::byte>(seed & 0xFF);
        std::ranges::for_each(buffer,
                              [&](std::byte& byte)
                              {
                                  byte = byte_seed;
                                  byte_seed = byte_seed * 31 + 17;
                              });
    }

    static void generate_value_ptr(std::span<std::byte> buffer, size_t seed)
    {
        size_t fake_offset = (seed * 1234567) % (1ULL << 32);
        size_t fake_size = (seed % 1024) + 1;
        hedge::value_ptr_t vp(fake_offset, static_cast<uint32_t>(fake_size), 0);
        std::memcpy(buffer.data(), &vp, sizeof(vp));
    }

    [[nodiscard]] bool check_returned_payload(const hedge::value_t& value, size_t seed) const
    {
        return std::visit(
            hedge::overloaded{
                [seed](const hedge::value_ptr_t& v) -> bool
                {
                    hedge::value_ptr_t groundtruth{};
                    generate_value_ptr(
                        std::span<std::byte>{reinterpret_cast<std::byte*>(&groundtruth), sizeof(groundtruth)}, seed);
                    return groundtruth == v;
                },
                [seed](const std::vector<std::byte>& v) -> bool
                {
                    std::vector<std::byte> groundtruth(TEST_VALUE_SIZE);
                    generate_inline_value(groundtruth, seed);
                    return std::equal(groundtruth.begin(), groundtruth.end(), v.begin());
                },
                [](const hedge::tombstone_t&) -> bool
                { return true; },
            },
            value);
    }

    std::span<std::byte> generate_value(hedge::single_buffer_arena_allocator& arena, size_t seed, hedge::value_type type)
    {
        std::span<std::byte> buffer;
        switch(type)
        {
            case hedge::value_type::VALUE_PTR:
                buffer = arena.allocate(sizeof(hedge::value_ptr_t) + 1);
                buffer[0] = static_cast<std::byte>(hedge::value_type::VALUE_PTR);
                generate_value_ptr(buffer.subspan(1), seed);
                break;
            case hedge::value_type::IN_PLACE_VALUE:
                buffer = arena.allocate(TEST_VALUE_SIZE + 1);
                buffer[0] = static_cast<std::byte>(hedge::value_type::IN_PLACE_VALUE);
                generate_inline_value(buffer.subspan(1), seed);
                break;
            case hedge::value_type::TOMBSTONE:
                buffer = arena.allocate(1);
                buffer[0] = static_cast<std::byte>(hedge::value_type::TOMBSTONE);
                break;
            default:
                throw std::invalid_argument("Invalid value type");
        }
        return buffer;
    }

    size_t seed{107279581};
    std::mt19937 generator{seed};
    std::uniform_int_distribution<uint8_t> dist{0, 255};
    std::uniform_int_distribution<size_t> len_dist{8, 64};
};

struct range_scan_test : public ::testing::TestWithParam<std::tuple<size_t, size_t, hedge::value_type>>,
                         public scan_test_helpers
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->NUM_PARTITION_EXPONENT = std::get<1>(GetParam());
        this->VALUE_TYPE = std::get<2>(GetParam());

        this->_keys.reserve(this->N_KEYS);

        if(!std::filesystem::exists(this->_base_path))
            std::filesystem::create_directories(this->_base_path);
        else
        {
            std::filesystem::remove_all(this->_base_path);
            std::filesystem::create_directories(this->_base_path);
        }

        this->_executor = std::make_unique<hedge::io::io_executor>(8, 64);
    }

    void TearDown() override
    {
        this->_executor.reset();
    }

    static auto lookup_task_factory(size_t j,
                                    const hedge::key_t& key,
                                    const hedge::db::sst& sst,
                                    range_scan_test& test_instance,
                                    std::atomic<size_t>& found_count) -> tmc::task<void>
    {
        auto lookup = co_await sst.lookup_async(key, nullptr);

        if(!lookup.has_value())
        {
            std::cerr << "Error during lookup for key: " << to_hex_string(key) << " error: " << lookup.error().to_string() << '\n';
        }
        else
        {
            auto val = std::move(lookup.value());

            if(test_instance.check_returned_payload(val, j))
            {
                found_count.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                std::cerr << "Value mismatch for key: " << to_hex_string(key) << '\n';
            }
        }
    };

    size_t N_KEYS = 2000000;
    size_t NUM_PARTITION_EXPONENT = 0;
    hedge::value_type VALUE_TYPE = hedge::value_type::UNDEFINED;
    std::vector<hedge::key_t> _keys;
    std::string _base_path = "/tmp/hh/test_index2";
    std::unique_ptr<hedge::io::io_executor> _executor{};
    hedge::single_buffer_arena_allocator arena{1024 * 1024 * 128};
};

TEST_P(range_scan_test, test_flush_and_range_scan_all_16b_keys)
{
    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    std::cout << "Generating " << this->N_KEYS << " keys..." << std::endl;
    for(size_t j = 0; j < this->N_KEYS; ++j)
    {
        this->_keys.emplace_back(generate_key(16));
        const auto& key = this->_keys.back();

        auto buffer = generate_value(arena, j, this->VALUE_TYPE);
        memtable.insert(key, buffer);
    }

    std::cout << "Keys generated." << std::endl;

    std::filesystem::path base_path = this->_base_path;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto begin = memtable.accessor().cbegin();
    auto end = memtable.accessor().cend();

    auto result_future = tmc::post_waitable(
        *_executor,
        hedge::db::index_ops::flush_memtable(
            base_path,
            begin,
            end,
            this->NUM_PARTITION_EXPONENT,
            0, // flush_iteration
            nullptr,
            false, // use_odirect
            this->_executor->ex(),
            false // fdatasync_ssts
            ));

    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    auto result = result_future.get();

    if(!result.has_value())
    {
        std::cerr << "Failed to flush indices: " << result.error().to_string() << '\n';
        FAIL();
    }
    std::cout << "Flush completed." << std::endl;
    std::cout << "Flush took " << duration / 1000.0 << " ms" << std::endl;

    std::vector<hedge::db::sst> ssts = std::move(result.value());
    ASSERT_FALSE(ssts.empty());

    // Prepare lookup
    auto make_partition_from_sst = [](hedge::db::sst_ptr_t sst)
    {
        return hedge::db::partition_t{hedge::db::level_t{std::move(sst)}};
    };

    std::map<uint16_t, hedge::db::partition_t> sst_map;
    for(auto& sst : ssts)
        sst_map[sst.upper_bound()] = make_partition_from_sst(std::make_shared<hedge::db::sst>(std::move(sst)));

    // Prepare ground truth
    struct key_with_seed
    {
        hedge::key_t k;
        uint64_t seed;
    };

    using groundtruth_t = std::map<uint16_t, std::vector<key_with_seed>>;
    groundtruth_t keys_gt;
    for(const auto& [i, key] : this->_keys | std::views::enumerate)
    {
        size_t partition_key_prefix_range = (1 << 16) / (1 << this->NUM_PARTITION_EXPONENT);
        size_t partition_id = hedge::find_partition_prefix_for_key(key, partition_key_prefix_range);
        keys_gt[partition_id].emplace_back(key, i);
    }

    for(auto& [prefix, gt_vec] : keys_gt)
    {
        std::ranges::sort(gt_vec, [](const auto& lhs, const auto& rhs)
                          { return lhs.k < rhs.k; });
    }

    // Test range scan

    struct test_result_t
    {
        size_t count;
        size_t expected;
        int64_t duration_us;
    };

    t0 = std::chrono::high_resolution_clock::now();
    std::vector<std::future<hedge::expected<test_result_t>>> futures;
    futures.reserve(sst_map.size());
    for(const auto& [prefix, partition] : sst_map)
    {
        auto f = tmc::post_waitable(
            *this->_executor,
            [](const hedge::db::partition_t& partition, uint16_t prefix, groundtruth_t& keys_gt, auto* test_fixture) -> tmc::task<hedge::expected<test_result_t>>
            {
                auto t0 = std::chrono::high_resolution_clock::now();
                auto maybe_it = hedge::db::scan_iterator::from_partition(
                    nullptr,
                    &partition,
                    std::nullopt,
                    std::nullopt,
                    64 * 1024);

                if(!maybe_it)
                    co_return maybe_it.error();

                auto& it = maybe_it.value();

                size_t c = 0;
                while(true)
                {
                    auto maybe_kv = co_await it.next();
                    if(!maybe_kv.has_value() && maybe_kv.error().code() == hedge::errc::END_OF_SCAN)
                        break;

                    if(!maybe_kv.has_value())
                        co_return maybe_kv.error();

                    auto& [k, v] = maybe_kv.value();

                    // check key
                    if(k != keys_gt.at(prefix).at(c).k)
                    {
                        co_return hedge::error(
                            "Key mismatch at partition " + std::to_string(prefix) +
                            " index " + std::to_string(c) +
                            ": " + to_hex_string(k) +
                            " vs " + to_hex_string(keys_gt.at(prefix).at(c).k));
                    }

                    // check value
                    if(!test_fixture->check_returned_payload(v, keys_gt.at(prefix).at(c).seed))
                    {
                        co_return hedge::error(
                            "Value mismatch for key " + to_hex_string(k) +
                            " at partition " + std::to_string(prefix) +
                            " index " + std::to_string(c));
                    }

                    c++;
                }
                auto t1 = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                co_return test_result_t{.count = c, .expected = keys_gt.at(prefix).size(), .duration_us = duration};
            }(partition, prefix, keys_gt, this));
        futures.emplace_back(std::move(f));
    }

    for(auto& f : futures)
    {
        auto s = f.get();
        if(!s)
        {
            std::cerr << "Range scan failed: " << s.error().to_string() << '\n';
            FAIL();
        }

        EXPECT_EQ(s.value().count, s.value().expected);
    }
    t1 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    std::cout << "Range scans completed. Total duration: " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Throughput: " << static_cast<size_t>(this->N_KEYS / (duration / 1'000'000.0)) << " keys/s" << std::endl;
    std::cout << "Average latency per partition: " << (duration / futures.size()) / 1000.0 << " ms" << std::endl;
}

struct memtable_merged_scan_test : public ::testing::TestWithParam<std::tuple<size_t, hedge::value_type>>,
                                   public scan_test_helpers
{
    void SetUp() override
    {
        this->N_KEYS = std::get<0>(GetParam());
        this->VALUE_TYPE = std::get<1>(GetParam());

        if(std::filesystem::exists(this->_base_path))
            std::filesystem::remove_all(this->_base_path);
        std::filesystem::create_directories(this->_base_path);

        this->_executor = std::make_unique<hedge::io::io_executor>(4, 32);
    }

    void TearDown() override
    {
        this->_executor.reset();
    }

    [[nodiscard]] hedge::db::memtable::rw_sync_table_ptr_t make_table(size_t budget = 16 * 1024 * 1024) const
    {
        return std::make_shared<hedge::db::memtable::rw_sync_table_t>(
            1, this->_base_path, false, 0, budget, 1, budget);
    }

    [[nodiscard]] hedge::expected<std::vector<hedge::db::sst>> do_flush(hedge::db::skiplist_wrapper& mem, size_t flush_iter) const
    {
        auto begin = mem.accessor().cbegin();
        auto end = mem.accessor().cend();

        return tmc::post_waitable(
                   *this->_executor,
                   hedge::db::index_ops::flush_memtable(
                       std::filesystem::path{this->_base_path},
                       begin,
                       end,
                       0,
                       flush_iter,
                       nullptr,
                       false,
                       this->_executor->ex(),
                       false))
            .get();
    }

    hedge::db::scan_iterator build_iterator(
        const hedge::db::partition_t& partition,
        hedge::db::memtable::snapshot snapshot)
    {
        constexpr size_t read_ahead_size = hedge::PAGE_SIZE_IN_BYTES * 4;

        std::vector<hedge::db::sst_ptr_t> sst_ptrs;
        std::vector<hedge::db::page_range> ranges;
        for(const auto& level : partition)
        {
            for(const auto& sp : level)
            {
                auto range = sp->find_range(std::nullopt, std::nullopt);
                if(!range)
                    continue;
                sst_ptrs.push_back(sp);
                ranges.push_back(*range);
            }
        }

        auto rbufs = std::make_unique<hedge::db::sst_stream_set>(sst_ptrs.size());
        for(size_t i = 0; i < sst_ptrs.size(); ++i)
        {
            const auto& sp = sst_ptrs[i];
            const auto& r = ranges[i];
            size_t start = sp->footer().index_offset + (r.first_page_id * hedge::PAGE_SIZE_IN_BYTES);
            size_t end = sp->footer().index_offset + ((r.last_page_id + 1) * hedge::PAGE_SIZE_IN_BYTES);
            rbufs->emplace_back(*sp,
                                hedge::fs::file_reader2_config{
                                    .start_offset = start,
                                    .end_offset = end,
                                    .read_ahead_size = read_ahead_size},
                                read_ahead_size);
        }

        return hedge::db::scan_iterator{
            std::move(snapshot),
            std::move(sst_ptrs),
            std::move(rbufs),
            std::nullopt,
            std::nullopt};
    }

    struct scan_result_t
    {
        std::vector<std::pair<hedge::key_t, hedge::value_t>> entries;
        std::string error;
        int64_t duration_us{0};
    };

    [[nodiscard]] scan_result_t run_scan(hedge::db::scan_iterator it) const
    {
        return tmc::post_waitable(
                   *this->_executor,
                   [it = std::move(it)]() mutable -> tmc::task<scan_result_t>
                   {
                       scan_result_t r;
                       auto t0 = std::chrono::high_resolution_clock::now();
                       while(true)
                       {
                           auto maybe_kv = co_await it.next();
                           if(!maybe_kv && maybe_kv.error().code() == hedge::errc::END_OF_SCAN)
                               break;
                           if(!maybe_kv)
                           {
                               r.error = maybe_kv.error().to_string();
                               co_return r;
                           }
                           r.entries.push_back(std::move(maybe_kv.value()));
                       }
                       r.duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::high_resolution_clock::now() - t0)
                                           .count();
                       co_return r;
                   }())
            .get();
    }

    size_t N_KEYS{1000};
    hedge::value_type VALUE_TYPE{hedge::value_type::VALUE_PTR};
    std::string _base_path = "/tmp/hh/test_memtable_merged_p";
    std::unique_ptr<hedge::io::io_executor> _executor{};
    hedge::single_buffer_arena_allocator _arena{64 * 1024 * 1024};
};

// Verify that a memtable-only scan (no SSTs) returns all keys sorted.
TEST_P(memtable_merged_scan_test, memtable_only)
{
    const size_t N = this->N_KEYS;

    std::vector<hedge::key_t> all_keys(N);
    for(auto& k : all_keys)
        k = generate_key(16);

    auto table = make_table();
    for(size_t i = 0; i < N; ++i)
        table->ptr()->insert(all_keys[i], generate_value(this->_arena, i, this->VALUE_TYPE));

    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.curr = table;

    hedge::db::partition_t empty_partition{};
    auto result = run_scan(build_iterator(empty_partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    std::cout << "Scanned " << result.entries.size() << " keys in "
              << result.duration_us / 1000.0 << " ms, throughput: "
              << static_cast<size_t>(result.entries.size() / (result.duration_us / 1e6)) << " keys/s\n";

    ASSERT_EQ(result.entries.size(), N);

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N; ++i)
        expected_seed[all_keys[i]] = i;

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

// Verify that live memtable keys are merged with SST keys during a scan,
// and that the memtable version wins for keys present in both.
TEST_P(memtable_merged_scan_test, single_sst_single_memtable)
{
    const size_t N = this->N_KEYS / 3; // keys per group
    const size_t OVERLAP = std::min(N, size_t{10});

    // Generate three disjoint key groups.
    // sst_only and overlap are flushed to SST; mem_only and overlap go into the live memtable.
    // For overlap keys, the memtable version (seed = 3*N+i) must beat the SST version (seed = N+i).
    std::vector<hedge::key_t> sst_only(N);
    std::vector<hedge::key_t> overlap(OVERLAP);
    std::vector<hedge::key_t> mem_only(N);
    for(auto& k : sst_only)
        k = generate_key(16);
    for(auto& k : overlap)
        k = generate_key(16);
    for(auto& k : mem_only)
        k = generate_key(16);

    std::atomic_size_t seq_nr{};
    auto memtable = hedge::db::skiplist_wrapper{&seq_nr, 1024 * 1024 * 128};

    for(size_t i = 0; i < N; ++i)
        memtable.insert(sst_only[i], generate_value(this->_arena, i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        memtable.insert(overlap[i], generate_value(this->_arena, N + i, this->VALUE_TYPE));

    auto flush_result = do_flush(memtable, 0);
    ASSERT_TRUE(flush_result) << flush_result.error().to_string();

    hedge::db::partition_t partition{hedge::db::level_t{}};
    for(auto& sst : flush_result.value())
        partition[0].push_back(std::make_shared<hedge::db::sst>(std::move(sst)));

    auto live = make_table();
    for(size_t i = 0; i < N; ++i)
        live->ptr()->insert(mem_only[i], generate_value(this->_arena, (2 * N) + i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        live->ptr()->insert(overlap[i], generate_value(this->_arena, (3 * N) + i, this->VALUE_TYPE)); // wins

    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.curr = live;

    auto result = run_scan(build_iterator(partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    std::cout << "Scanned " << result.entries.size() << " keys in "
              << result.duration_us / 1000.0 << " ms, throughput: "
              << static_cast<size_t>(result.entries.size() / (result.duration_us / 1e6)) << " keys/s\n";

    ASSERT_EQ(result.entries.size(), (2 * N) + OVERLAP);

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N; ++i)
        expected_seed[sst_only[i]] = i;
    for(size_t i = 0; i < OVERLAP; ++i)
        expected_seed[overlap[i]] = (3 * N) + i; // memtable wins
    for(size_t i = 0; i < N; ++i)
        expected_seed[mem_only[i]] = (2 * N) + i;

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

// Verify that keys from 2 SSTs and 2 live memtables are all merged,
// and that memtable versions beat SST versions for overlapping keys.
TEST_P(memtable_merged_scan_test, multiple_ssts_multiple_memtables)
{
    const size_t N = this->N_KEYS / 4; // keys per source
    const size_t OVERLAP = std::min(N, size_t{10});

    // Four independent key sets, one per source.
    // The first OVERLAP keys of sst1_keys are also written into mem2 with a different value;
    // the mem2 version must win because memtable epoch > SST epoch.
    std::vector<hedge::key_t> sst1_keys(N);
    std::vector<hedge::key_t> sst2_keys(N);
    std::vector<hedge::key_t> mem1_keys(N);
    std::vector<hedge::key_t> mem2_keys(N);
    for(auto& k : sst1_keys)
        k = generate_key(16);
    for(auto& k : sst2_keys)
        k = generate_key(16);
    for(auto& k : mem1_keys)
        k = generate_key(16);
    for(auto& k : mem2_keys)
        k = generate_key(16);

    // SST1 (seeds 0..N-1)
    std::atomic_size_t seq_nr{};
    hedge::db::skiplist_wrapper sst1_mem(&seq_nr, 16 * 1024 * 1024);
    for(size_t i = 0; i < N; ++i)
        sst1_mem.insert(sst1_keys[i], generate_value(this->_arena, i, this->VALUE_TYPE));
    auto r1 = do_flush(sst1_mem, 0);
    ASSERT_TRUE(r1) << r1.error().to_string();

    // SST2 (seeds N..2N-1)
    std::atomic_size_t seq_nr2{};
    hedge::db::skiplist_wrapper sst2_mem(&seq_nr2, 16 * 1024 * 1024);
    for(size_t i = 0; i < N; ++i)
        sst2_mem.insert(sst2_keys[i], generate_value(this->_arena, N + i, this->VALUE_TYPE));
    auto r2 = do_flush(sst2_mem, 1);
    ASSERT_TRUE(r2) << r2.error().to_string();

    hedge::db::partition_t partition{hedge::db::level_t{}};
    for(auto& sst : r1.value())
        partition[0].push_back(std::make_shared<hedge::db::sst>(std::move(sst)));
    for(auto& sst : r2.value())
        partition[0].push_back(std::make_shared<hedge::db::sst>(std::move(sst)));

    // Mem1 (seeds 2N..3N-1)
    auto mem1 = make_table();
    for(size_t i = 0; i < N; ++i)
        mem1->ptr()->insert(mem1_keys[i], generate_value(this->_arena, (2 * N) + i, this->VALUE_TYPE));

    // Mem2 (seeds 3N..4N-1) + overlap with SST1 (seeds 4N..4N+OVERLAP-1, wins)
    auto mem2 = make_table();
    for(size_t i = 0; i < N; ++i)
        mem2->ptr()->insert(mem2_keys[i], generate_value(this->_arena, (3 * N) + i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        mem2->ptr()->insert(sst1_keys[i], generate_value(this->_arena, (4 * N) + i, this->VALUE_TYPE));

    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.curr = mem1;
    snap.pending_flushes[999] = mem2;

    auto result = run_scan(build_iterator(partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    std::cout << "Scanned " << result.entries.size() << " keys in "
              << result.duration_us / 1000.0 << " ms, throughput: "
              << static_cast<size_t>(result.entries.size() / (result.duration_us / 1e6)) << " keys/s\n";

    ASSERT_EQ(result.entries.size(), 4 * N); // OVERLAP keys deduplicated, not added

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N; ++i)
        expected_seed[sst1_keys[i]] = i;
    for(size_t i = 0; i < N; ++i)
        expected_seed[sst2_keys[i]] = N + i;
    for(size_t i = 0; i < N; ++i)
        expected_seed[mem1_keys[i]] = 2 * N + i;
    for(size_t i = 0; i < N; ++i)
        expected_seed[mem2_keys[i]] = 3 * N + i;
    for(size_t i = 0; i < OVERLAP; ++i)
        expected_seed[sst1_keys[i]] = 4 * N + i; // mem2 wins

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

// The skiplist stores every insert as a separate entry keyed by (key, seq_nr DESC).
// A single memtable can therefore hold multiple versions of the same key.
// The memtable_cursor must deduplicate these internally: for each key it should
// yield only the entry with the highest sequence number (the most recent write)
// and skip all older versions.
TEST_P(memtable_merged_scan_test, intra_memtable_key_dedup)
{
    const size_t N_UNIQUE = this->N_KEYS;
    constexpr size_t N_OVERWRITES = 5;
    const size_t OVERWRITE_COUNT = std::min(N_UNIQUE, size_t{50});

    // Insert N_UNIQUE distinct keys with initial values (seeds 0..N-1).
    std::vector<hedge::key_t> unique_keys(N_UNIQUE);
    for(auto& k : unique_keys)
        k = generate_key(16);

    auto table = make_table();

    for(size_t i = 0; i < N_UNIQUE; ++i)
        table->ptr()->insert(unique_keys[i], generate_value(this->_arena, i, this->VALUE_TYPE));

    // Overwrite the first OVERWRITE_COUNT keys N_OVERWRITES more times.
    // Each round produces a new skiplist entry with a higher seq_nr for
    // the same key. Only the value from the last round should survive.
    for(size_t round = 1; round <= N_OVERWRITES; ++round)
        for(size_t i = 0; i < OVERWRITE_COUNT; ++i)
            table->ptr()->insert(unique_keys[i], generate_value(this->_arena, (N_UNIQUE * round) + i, this->VALUE_TYPE));

    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.curr = table;

    hedge::db::partition_t empty_partition{};
    auto result = run_scan(build_iterator(empty_partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    // Each key must appear exactly once — no duplicates from older versions.
    ASSERT_EQ(result.entries.size(), N_UNIQUE);

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    // Overwritten keys must carry the value from the final overwrite round.
    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N_UNIQUE; ++i)
        expected_seed[unique_keys[i]] = i;
    for(size_t i = 0; i < OVERWRITE_COUNT; ++i)
        expected_seed[unique_keys[i]] = N_UNIQUE * N_OVERWRITES + i;

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

// When multiple memtables are queued for flush, each one gets a distinct epoch
// (the map key in snapshot.pending_flushes). For keys that appear in more than
// one pending flush, the scan must keep the version from the flush with the
// highest epoch — that flush was frozen later and therefore carries the most
// recent write.
//
// Before the fix, all pending flush cursors reported epoch = MAX_UINT64, so the
// heap could not distinguish them and the wrong version could survive dedup.
TEST_P(memtable_merged_scan_test, pending_flush_epoch_ordering)
{
    const size_t N = this->N_KEYS / 3;
    const size_t OVERLAP = std::min(N, size_t{50});

    // Three disjoint key sets: unique to pf1, unique to pf2, shared (overlap).
    std::vector<hedge::key_t> pf1_keys(N);
    std::vector<hedge::key_t> pf2_keys(N);
    std::vector<hedge::key_t> overlap_keys(OVERLAP);
    for(auto& k : pf1_keys)
        k = generate_key(16);
    for(auto& k : pf2_keys)
        k = generate_key(16);
    for(auto& k : overlap_keys)
        k = generate_key(16);

    // Pending flush 1 — older (epoch = 10).
    // Overlap keys here use seeds N..N+OVERLAP-1 and must lose.
    auto pf1 = make_table();
    for(size_t i = 0; i < N; ++i)
        pf1->ptr()->insert(pf1_keys[i], generate_value(this->_arena, i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        pf1->ptr()->insert(overlap_keys[i], generate_value(this->_arena, N + i, this->VALUE_TYPE));

    // Pending flush 2 — newer (epoch = 20).
    // Overlap keys here use seeds 3N..3N+OVERLAP-1 and must win.
    auto pf2 = make_table();
    for(size_t i = 0; i < N; ++i)
        pf2->ptr()->insert(pf2_keys[i], generate_value(this->_arena, (2 * N) + i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        pf2->ptr()->insert(overlap_keys[i], generate_value(this->_arena, (3 * N) + i, this->VALUE_TYPE));

    // No current memtable — only the two pending flushes with distinct epochs.
    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.pending_flushes[10] = pf1;
    snap.pending_flushes[20] = pf2;

    hedge::db::partition_t empty_partition{};
    auto result = run_scan(build_iterator(empty_partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    // Overlap keys are deduplicated: 2*N unique + OVERLAP shared = 2*N + OVERLAP total.
    ASSERT_EQ(result.entries.size(), (2 * N) + OVERLAP);

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N; ++i)
        expected_seed[pf1_keys[i]] = i;
    for(size_t i = 0; i < N; ++i)
        expected_seed[pf2_keys[i]] = 2 * N + i;
    for(size_t i = 0; i < OVERLAP; ++i)
        expected_seed[overlap_keys[i]] = 3 * N + i; // epoch 20 wins

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

// The current (live) memtable is assigned epoch = MAX_UINT64, which is strictly
// greater than any pending flush epoch. This guarantees that for any key present
// in both the live memtable and a pending flush, the live memtable's version is
// always the one that survives deduplication — regardless of the pending flush's
// epoch value.
TEST_P(memtable_merged_scan_test, current_memtable_beats_pending_flush)
{
    const size_t N = this->N_KEYS / 3;
    const size_t OVERLAP = std::min(N, size_t{50});

    // Three disjoint key sets: unique to pending flush, unique to current, shared.
    std::vector<hedge::key_t> pf_keys(N);
    std::vector<hedge::key_t> curr_keys(N);
    std::vector<hedge::key_t> overlap_keys(OVERLAP);
    for(auto& k : pf_keys)
        k = generate_key(16);
    for(auto& k : curr_keys)
        k = generate_key(16);
    for(auto& k : overlap_keys)
        k = generate_key(16);

    // Pending flush (epoch = 999).
    // Overlap keys here use seeds N..N+OVERLAP-1 and must lose.
    auto pf = make_table();
    for(size_t i = 0; i < N; ++i)
        pf->ptr()->insert(pf_keys[i], generate_value(this->_arena, i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        pf->ptr()->insert(overlap_keys[i], generate_value(this->_arena, N + i, this->VALUE_TYPE));

    // Current (live) memtable (epoch = MAX_UINT64).
    // Overlap keys here use seeds 3N..3N+OVERLAP-1 and must win.
    auto curr = make_table();
    for(size_t i = 0; i < N; ++i)
        curr->ptr()->insert(curr_keys[i], generate_value(this->_arena, (2 * N) + i, this->VALUE_TYPE));
    for(size_t i = 0; i < OVERLAP; ++i)
        curr->ptr()->insert(overlap_keys[i], generate_value(this->_arena, (3 * N) + i, this->VALUE_TYPE));

    hedge::db::memtable::snapshot snap{.seq_nr = std::numeric_limits<uint64_t>::max(), .curr = nullptr, .pending_flushes = {}};
    snap.curr = curr;
    snap.pending_flushes[999] = pf;

    hedge::db::partition_t empty_partition{};
    auto result = run_scan(build_iterator(empty_partition, std::move(snap)));
    ASSERT_TRUE(result.error.empty()) << result.error;

    // Overlap keys are deduplicated: 2*N unique + OVERLAP shared = 2*N + OVERLAP total.
    ASSERT_EQ(result.entries.size(), (2 * N) + OVERLAP);

    std::vector<hedge::key_t> keys;
    keys.reserve(result.entries.size());
    for(auto& [k, v] : result.entries)
        keys.push_back(k);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    std::map<hedge::key_t, size_t> expected_seed;
    for(size_t i = 0; i < N; ++i)
        expected_seed[pf_keys[i]] = i;
    for(size_t i = 0; i < N; ++i)
        expected_seed[curr_keys[i]] = 2 * N + i;
    for(size_t i = 0; i < OVERLAP; ++i)
        expected_seed[overlap_keys[i]] = 3 * N + i; // live memtable wins

    for(auto& [k, v] : result.entries)
        EXPECT_TRUE(check_returned_payload(v, expected_seed.at(k))) << "value mismatch";
}

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    memtable_merged_scan_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)),
    [](const testing::TestParamInfo<memtable_merged_scan_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto value_type = std::get<1>(info.param);
        return "N_" + std::to_string(num_keys) +
               (value_type == hedge::value_type::VALUE_PTR ? "_VALUE_PTR" : "_IN_PLACE_VALUE");
    });

INSTANTIATE_TEST_SUITE_P(
    test_suite,
    range_scan_test,
    testing::Combine(
        testing::Values(1'000, 10'000, 200'000, 500'000),
        testing::Values(0, 1, 4, 10),
        testing::Values(hedge::value_type::VALUE_PTR, hedge::value_type::IN_PLACE_VALUE)),

    [](const testing::TestParamInfo<range_scan_test::ParamType>& info)
    {
        auto num_keys = std::get<0>(info.param);
        auto num_partitions = 1 << std::get<1>(info.param);
        auto value_type = std::get<2>(info.param);
        std::string name = "N_" + std::to_string(num_keys) + "_P_" + std::to_string(num_partitions) + (value_type == hedge::value_type::VALUE_PTR ? "_VALUE_PTR" : "_IN_PLACE_VALUE");
        return name;
    });
