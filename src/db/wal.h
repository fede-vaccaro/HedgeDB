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
        };

        explicit wal(const config& cfg);

        hedge::status append(size_t thread_idx, uint64_t seq_nr,
                             const key_t& key, std::span<const std::byte> value);

        tmc::task<hedge::status> reset();

        // Reads all non-empty WAL files under `path`, sorted by seq_nr.
        // Calls on_entry for each; stops early if it returns false.
        static hedge::status replay(
            const std::filesystem::path& path,
            const std::function<bool(const key_t&, std::span<const std::byte>, uint64_t)>& on_entry,
            logger& log);

        static std::vector<std::filesystem::path> collect_wal_filenames(const std::filesystem::path& path);

    private:
        struct wal_file
        {
            fs::file file;
            size_t offset;
        };

        std::vector<wal_file> _files;
        size_t _file_size_hint{};

        struct wal_entry
        {
            uint64_t seq_nr;
            hedge::key_t key;
            std::vector<std::byte> value;
            size_t wal_entry_bytes;
        };

        static std::vector<wal::wal_entry> read_all_entries(const std::filesystem::path& file, logger& log);
        static hedge::async::generator<hedge::expected<wal_entry>> read_wal_file_generator(const std::filesystem::path& wal_file);
        static hedge::status write_entry(wal_file& file_and_offset, uint64_t seq_nr,
                                         const key_t& key, std::span<const std::byte> value);

        static tmc::task<status> zero_wal_until_size(wal_file& file, size_t size);
        static std::vector<std::byte> zero_bytes;
    };

} // namespace hedge::db
