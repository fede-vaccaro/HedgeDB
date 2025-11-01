#pragma once

#include <chrono>
#include <cstddef>

namespace hedge
{
    struct config
    {
        size_t keys_in_mem_before_flush = 2'000'000;          // default number of keys to push
        size_t num_partition_exponent = 10;                   // default partition exponent
        double compaction_size_ratio = 0.2;                   // if during two way merge, rhs/lhs > compaction_size_ratio, a compaction job is triggered
        size_t compaction_read_ahead_size_bytes = 16384;      // it will read from each table 16 KB at a time
        std::chrono::milliseconds compaction_timeout{120000}; // stop waiting if this timeout is due
        bool auto_compaction = true;                          // compaction is automatically triggered when the memtable reaches its limit
        size_t max_pending_compactions = 16;                  // maximum number of pending compactions before the database stops accepting new writes: TODO: implement this
        bool use_odirect_for_indices = false;                 // if true, use O_DIRECT flag for sorted_index file I/O to bypass
    };
} // namespace hedge