#include <algorithm>
#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <numeric>
#include <stdexcept>
#include <unistd.h>
#include <vector>

#include "error.hpp"
#include "fs/fs.hpp"
#include "generator.h"
#include "hasher.h"
#include "key.h"
#include "logger.h"
#include "types.h"
#include "wal.h"

namespace
{
    struct wal_entry
    {
        size_t epoch;
        uint64_t seq_nr;
        hedge::key_t key;
        std::vector<uint8_t> value;
    };

    struct wal_file_info
    {
        std::filesystem::path path;
        size_t epoch;
    };

    hedge::async::generator<hedge::expected<wal_entry>> read_wal_file_generator(std::filesystem::path path, size_t epoch)
    {
        auto maybe_file = hedge::fs::file::from_path(path, hedge::fs::file::open_mode::read_only, false);
        if(!maybe_file)
            throw std::runtime_error(std::format("could not open wal file {}: {}", path.string(), maybe_file.error().to_string()));

        auto& file = maybe_file.value();
        size_t file_size = file.file_size();
        if(file_size == 0)
            throw std::runtime_error("wal file " + path.string() + " is empty");

        std::vector<uint8_t> buffer(file_size);
        ssize_t bytes_read = ::pread(file.fd(), buffer.data(), file_size, 0);
        if(bytes_read <= 0)
            throw std::runtime_error("failed to read wal file " + path.string());

        auto* pos = buffer.data();
        auto* end = buffer.data() + bytes_read;
        hedge::third_party::hasher64 hasher;

        auto read_from_buf = [&pos, end](auto* dst, size_t n) -> bool
        {
            if(pos + n > end)
                return false;

            std::memcpy(static_cast<void*>(dst), pos, n);
            pos += n;

            return true;
        };

        auto offset = [&buffer, &pos]()
        {
            return pos - buffer.data();
        };

        /*
            WAL format is:
            [seq_nr (8 bytes)]
            [encoded_key_size (1 byte)]
            [key (encoded_key_size bytes)]
            [value_size (2 bytes)]
            [value (value_size bytes)]
            [checksum (4 bytes)]
        */
        while(pos < end)
        {
            uint64_t seq_nr;
            if(!read_from_buf(&seq_nr, sizeof(uint64_t)))
            {
                co_yield hedge::error(std::format("incomplete WAL entry sequence number in file {} at offset {}", path.string(), offset()));
                break;
            }

            uint8_t encoded_key_size;
            if(!read_from_buf(&encoded_key_size, sizeof(uint8_t)))
            {
                co_yield hedge::error(std::format("incomplete WAL entry key size in file {} at offset {}", path.string(), offset()));
                break;
            }

            size_t key_size = hedge::decode_key_size(encoded_key_size);
            if(key_size > hedge::MAX_KEY_LEN)
            {
                co_yield hedge::error(std::format("invalid encoded key size {} in WAL entry in file {} at offset {}", encoded_key_size, path.string(), offset()));
                break;
            }

            hedge::key_t key = hedge::key_t::make_with_length(key_size);
            if(!read_from_buf(key.data(), key_size))
            {
                co_yield hedge::error(std::format("incomplete WAL entry key in file {} at offset {}", path.string(), offset()));
                break;
            }

            uint16_t value_size;
            if(!read_from_buf(&value_size, sizeof(uint16_t)))
            {
                co_yield hedge::error(std::format("incomplete WAL entry value size in file {} at offset {}", path.string(), offset()));
                break;
            }

            std::vector<uint8_t> value(value_size);
            if(!read_from_buf(value.data(), value_size))
            {
                co_yield hedge::error(std::format("incomplete WAL entry value in file {} at offset {}", path.string(), offset()));
                break;
            }

            uint32_t checksum;
            if(!read_from_buf(&checksum, sizeof(uint32_t)))
            {
                co_yield hedge::error(std::format("incomplete WAL entry checksum in file {} at offset {}", path.string(), offset()));
                break;
            }

            // Recompute checksum and compare
            hasher.update(&seq_nr, sizeof(uint64_t));
            hasher.update(&encoded_key_size, sizeof(uint8_t));
            hasher.update(key.data(), key.size());
            hasher.update(&value_size, sizeof(uint16_t));
            hasher.update(value.data(), value_size);

            constexpr uint64_t MASK_32_BIT = (1ULL << 32) - 1;
            auto computed_checksum = static_cast<uint32_t>(hasher.sum() & MASK_32_BIT);
            if(checksum != computed_checksum)
            {
                co_yield hedge::error(
                    "checksum mismatch for WAL entry with seq_nr " + std::to_string(seq_nr) +
                    " in file " + path.string() +
                    ": expected " + std::to_string(checksum) +
                    ", computed " + std::to_string(computed_checksum));
                break;
            }

            co_yield wal_entry{
                .epoch = epoch,
                .seq_nr = seq_nr,
                .key = std::move(key),
                .value = std::move(value)};
        }
    }

    std::vector<wal_file_info> collect_wal_files(const std::filesystem::path& path)
    {
        std::vector<wal_file_info> files;
        if(!std::filesystem::exists(path))
            return files;

        for(const auto& entry : std::filesystem::directory_iterator(path))
        {
            if(!entry.is_regular_file() || entry.file_size() == 0)
                continue;

            auto fname = entry.path().filename().string();
            if(!fname.starts_with("wal."))
                continue;

            auto last_dot = fname.rfind('.');
            if(last_dot == std::string::npos)
                continue;

            size_t epoch = std::stoull(fname.substr(last_dot + 1));
            files.push_back({entry.path(), epoch});
        }

        return files;
    }

    std::vector<wal_entry> read_all_entries(const std::vector<wal_file_info>& files, logger& log)
    {
        bool any_errors = false;
        std::vector<wal_entry> entries;

        for(const auto& wf : files)
        {
            for(auto& entry : read_wal_file_generator(wf.path, wf.epoch))
            {
                if(!entry) [[unlikely]]
                {
                    any_errors = true;
                    log.log("Error reading WAL file ", wf.path, ": ", entry.error().to_string());
                    continue;
                }
                entries.push_back(std::move(entry.value()));
            }
        }

        if(any_errors)
            throw std::runtime_error("Errors occurred while reading WAL files; see log for details");

        std::ranges::sort(
            entries,
            [](const wal_entry& a, const wal_entry& b)
            {
                if(a.epoch != b.epoch)
                    return a.epoch < b.epoch;
                return a.seq_nr < b.seq_nr;
            });

        return entries;
    }

} // anonymous namespace

namespace hedge::db
{

    wal::wal(const config& cfg)
    {
        this->_files.reserve(cfg.n_threads);
        for(size_t i = 0; i < cfg.n_threads; ++i)
        {
            const auto wal_path = std::format("wal.t{}.{}", i, cfg.epoch);
            auto maybe_file = fs::file::from_path(
                cfg.base_path / wal_path,
                fs::file::open_mode::write_append_new,
                false,
                std::nullopt);

            if(!maybe_file.has_value())
                throw std::runtime_error("could not open wal " + (cfg.base_path / wal_path).string() +
                                         " : " + maybe_file.error().to_string());

            ::fallocate(maybe_file.value().fd(), FALLOC_FL_KEEP_SIZE, 0, static_cast<off_t>(cfg.file_size_hint));
            this->_files.emplace_back(std::move(maybe_file.value()));
        }

        if(cfg.dir_fd)
            ::fdatasync(*cfg.dir_fd);
    }

    hedge::status wal::_write_entry(int32_t fd, uint64_t seq_nr,
                                    const key_t& key, std::span<const uint8_t> value)
    {
        uint8_t encoded_key_size = hedge::encode_key_size(key.size());
        auto value_size = static_cast<uint16_t>(value.size());

        uint32_t checksum;

        std::array<iovec, 6> entry{
            iovec{.iov_base = &seq_nr, .iov_len = sizeof(uint64_t)},
            iovec{.iov_base = &encoded_key_size, .iov_len = sizeof(uint8_t)},
            iovec{.iov_base = const_cast<uint8_t*>(key.data()), .iov_len = key.size()},
            iovec{.iov_base = &value_size, .iov_len = sizeof(uint16_t)},
            iovec{.iov_base = const_cast<uint8_t*>(value.data()), .iov_len = value.size()},
            iovec{.iov_base = &checksum, .iov_len = sizeof(uint32_t)}};

        // Update hasher with all entry components except the checksum itself
        thread_local hedge::third_party::hasher64 hasher;
        for(size_t i = 0; i < entry.size() - 1; ++i)
            hasher.update(entry[i].iov_base, entry[i].iov_len);

        constexpr uint64_t MASK_32_BIT = (1ULL << 32) - 1;
        checksum = static_cast<uint32_t>(hasher.sum() & MASK_32_BIT);

        auto expected_bytes = std::accumulate(entry.begin(), entry.end(), size_t(0),
                                              [](size_t sum, const iovec& v)
                                              { return sum + v.iov_len; });

        int32_t res = pwritev2(fd, entry.data(), static_cast<int>(entry.size()), -1, 0);

        if(res < 0)
            return hedge::error("could not write into wal: " + std::string(strerror(errno)));

        if(size_t(res) != expected_bytes)
            return hedge::error("partial write into wal: " + std::to_string(res) + " != " + std::to_string(expected_bytes));

        return hedge::ok();
    }

    hedge::status wal::append(size_t thread_idx, uint64_t seq_nr,
                              const key_t& key, std::span<const uint8_t> value)
    {
        return _write_entry(_files[thread_idx].fd(), seq_nr, key, value);
    }

    hedge::status wal::replay(const std::filesystem::path& path,
                              const std::function<bool(const key_t&, std::span<const uint8_t>, uint64_t)>& on_entry,
                              logger& log)
    {
        auto files = collect_wal_files(path);
        if(files.empty())
            return hedge::ok();

        auto entries = read_all_entries(files, log);

        size_t replayed = 0;
        for(const auto& entry : entries)
        {
            if(!on_entry(entry.key, entry.value, entry.seq_nr))
                break;
            ++replayed;
        }

        log.log("WAL replay: replayed ", replayed, " entries from ", files.size(), " WAL files");

        for(const auto& wf : files)
            std::filesystem::remove(wf.path);

        return hedge::ok();
    }

    void wal::remove()
    {
        for(const auto& f : _files)
            std::filesystem::remove(f.path());
        _files.clear();
    }

} // namespace hedge::db
