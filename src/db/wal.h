#pragma once

#include <async/generator.h>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <vector>

#include "error.hpp"
#include "fs/fs.hpp"
#include "logger.h"
#include "types.h"
#include "tmc/task.hpp"

namespace hedge::db
{
    // wal represents a single pre-allocated WAL file that can be written to by a single thread at a time.
    class wal
    {
    public:
        static constexpr const char* WAL_FILE_PREFIX = "log";

        struct config
        {
            std::filesystem::path base_path;
            size_t slot_idx;
            size_t n_threads;
            size_t file_size_hint;
            size_t fsync_interval_bytes = 1;
        };

        explicit wal(const config& cfg);

        hedge::status append(size_t thread_idx, uint64_t seq_nr,
                             const key_t& key, std::span<const std::byte> value);

        tmc::task<hedge::status> sync();

        void reset();

        // Reads all non-empty WAL files under `path`, sorted by seq_nr.
        // Calls on_entry for each; stops early if it returns false.
        static hedge::status replay(
            const std::filesystem::path& path,
            const std::function<bool(const key_t&, std::span<const std::byte>, uint64_t)>& on_entry,
            logger& log);

        static std::vector<std::filesystem::path> collect_wal_filenames(const std::filesystem::path& path);

    private:
        struct alignas(64) aligned_counter
        {
            size_t value = 0;
        };

        std::vector<fs::file> _files;
        size_t _file_size_hint{};
        size_t _fsync_interval{};
        std::vector<aligned_counter> _bytes_written;

        static tmc::task<void> _fsync_one(wal* self, size_t idx, std::vector<int>& results);

        struct wal_entry
        {
            uint64_t seq_nr;
            hedge::key_t key;
            std::vector<std::byte> value;
            size_t wal_entry_bytes;
        };

        static std::vector<wal::wal_entry> read_all_entries(const std::filesystem::path& path, logger& log);
        static hedge::async::generator<hedge::expected<wal_entry>> read_wal_file_generator(const std::filesystem::path& path);
        static hedge::status write_entry(fs::file& file, uint64_t seq_nr,
                                         const key_t& key, std::span<const std::byte> value);

        static std::vector<std::byte> zero_bytes;
    };

} // namespace hedge::db
