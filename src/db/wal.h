#pragma once

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
    class wal
    {
    public:
        struct config
        {
            std::filesystem::path base_path;
            size_t epoch;
            size_t n_threads;
            size_t file_size_hint;
        };

        explicit wal(const config& cfg);

        hedge::status append(size_t thread_idx, uint64_t seq_nr,
                             const key_t& key, std::span<const uint8_t> value);

        // Reads all WAL files under `path`, sorted by (epoch, seq_nr).
        // Calls on_entry for each; stops early if it returns false.
        // Deletes the WAL files after processing.
        static hedge::status replay(
            const std::filesystem::path& path,
            const std::function<bool(const key_t&, std::span<const uint8_t>, uint64_t)>& on_entry,
            logger& log);

        void remove();

    private:
        std::vector<fs::file> _files;

        static hedge::status _write_entry(int32_t fd, uint64_t seq_nr,
                                          const key_t& key, std::span<const uint8_t> value);
    };

} // namespace hedge::db
