#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
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

        // Reads all non-empty WAL files under `path`, sorted by seq_nr.
        // Calls on_entry for each; stops early if it returns false.
        static hedge::status replay(
            const std::filesystem::path& path,
            const std::function<bool(const key_t&, std::span<const std::byte>, uint64_t)>& on_entry,
            logger& log);

        // Truncates all WAL files to zero and re-hints the pre-allocation,
        // making this slot ready for reuse.
        void reset();

    private:
        std::vector<fs::file> _files;
        size_t _file_size_hint{};

        static hedge::status _write_entry(int32_t fd, uint64_t seq_nr,
                                          const key_t& key, std::span<const std::byte> value);
    };

} // namespace hedge::db
