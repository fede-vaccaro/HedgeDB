#include <filesystem>
#include <unordered_set>

#include "sst_manager.h"

namespace hedge::db
{

    size_t flush_iteration_from_filename(const std::filesystem::path& p)
    {
        // Filename is <partition_prefix>.00012
        // -> extension is ".00012"
        // -> we skip the dot, parse "00012" and get '12' as size_t
        return std::stoull(p.extension().string().substr(1));
    };

    class sst_manager_helpers
    {
    public:
        static std::vector<std::string_view> split_str_by(std::string_view s, char delim)
        {
            std::vector<std::string_view> parts;
            size_t pos = 0;
            while(pos <= s.size())
            {
                size_t next = s.find(delim, pos);
                if(next == std::string_view::npos)
                {
                    parts.emplace_back(s.substr(pos));
                    break;
                }
                parts.emplace_back(s.substr(pos, next - pos));
                pos = next + 1;
            }
            return parts;
        }

        static hedge::expected<partition_t> _parse_manifest_and_load_ssts(std::string_view manifest_content,
                                                                          const std::filesystem::path& dir_path,
                                                                          size_t max_num_levels,
                                                                          bool use_odirect)
        {
            partition_t result(max_num_levels);
            size_t level_idx = 0;
            size_t pos = 0;

            while(level_idx < max_num_levels && pos < manifest_content.size())
            {
                size_t nl = manifest_content.find('\n', pos);
                if(nl == std::string_view::npos)
                    break;

                std::string_view line = manifest_content.substr(pos, nl - pos);
                pos = nl + 1;

                if(!line.empty())
                {
                    auto& level = result[level_idx];
                    for(auto fname : split_str_by(line, ';'))
                    {
                        if(fname.empty())
                            continue;
                        auto sst_path = dir_path / std::string(fname);
                        auto maybe_sst = sst::load(sst_path, use_odirect);
                        if(!maybe_sst)
                            return maybe_sst.error();
                        level.emplace_back(std::make_shared<sst>(std::move(maybe_sst.value())));
                    }
                }

                ++level_idx;
            }

            return result;
        }

        static hedge::status _load_partition_manifest(sst_manager::_partition_state& state, const std::filesystem::path& dir_path)
        {
            auto partition_manifest_path = dir_path / sst_manager::MANIFEST_FILENAME;
            if(!std::filesystem::exists(partition_manifest_path))
                return hedge::error(std::format("Manifest does not exist: {}", partition_manifest_path.string()));

            auto maybe_file = fs::file::from_path(partition_manifest_path, fs::file::open_mode::read_write, false, PAGE_SIZE_IN_BYTES);
            if(maybe_file)
                state.state_file = std::move(maybe_file.value());

            int fd = ::open(dir_path.c_str(), O_RDONLY | O_DIRECTORY);
            if(fd < 0)
                return hedge::error(std::format("Failed to open directory for partition {}: {}", dir_path.string(), std::strerror(errno)));

            state.dir_fd = fd;
            return hedge::ok();
        }

        // _check_orphans check whether there is any untracked file (orphans) other than the tracked SSTs described in the  manifest
        // returns hedge::error if any untracked file is detected, hedge::ok otherwise
        static hedge::status _check_orphans(sst_manager::_partition_state& state, const std::filesystem::path& indices_path)
        {
            // Collect tracked filenames to detect orphans on disk
            std::unordered_set<std::string> tracked_filenames;
            for(const auto& level : state.levels)
            {
                for(const auto& sst_ptr : level)
                    tracked_filenames.insert(sst_ptr->path().filename().string());
            }

            std::vector<std::string> orphans;
            for(const auto& entry : std::filesystem::directory_iterator(indices_path))
            {
                if(!entry.is_regular_file() || entry.path().stem() == sst_manager::MANIFEST_FILENAME)
                    continue;

                auto filename = entry.path().filename().string();
                if(!tracked_filenames.contains(filename))
                {
                    // max_flush_iteration = std::max(max_flush_iteration,
                    //    flush_iteration_from_filename(entry.path()));
                    orphans.push_back(filename);
                }
            }

            if(!orphans.empty())
            {
                std::string orphan_list;
                for(const auto& o : orphans)
                    orphan_list += " " + o;
                return hedge::error("Orphan SST files detected in partition " + indices_path.string() + ":" + orphan_list);
            }

            return hedge::ok();
        }

        static hedge::status _load_partition(const sst_manager::config& cfg, const std::unique_ptr<sst_manager::_partition_state>& state_ptr, size_t partition_id, size_t& max_flush_iteration)
        {
            auto& state = *state_ptr;
            auto [dir_prefix, file_prefix] = format_prefix(partition_id);
            auto dir_path = cfg.indices_path / dir_prefix;

            if(!std::filesystem::exists(dir_path))
                return hedge::error(std::format("Missing directory for partition: {}", dir_path.string()));

            auto ok = sst_manager_helpers::_load_partition_manifest(state, dir_path);
            if(!ok)
                return hedge::error(std::format("Failed to load manifest for partition {}: {}", partition_id, ok.error().to_string()));

            std::string buf(PAGE_SIZE_IN_BYTES, '\0');
            ssize_t n = ::pread(state.state_file.fd(), buf.data(), PAGE_SIZE_IN_BYTES, 0);

            if(n > 0)
            {
                auto maybe_levels = _parse_manifest_and_load_ssts(buf, dir_path, cfg.max_num_levels, cfg.use_odirect_for_indices);
                if(maybe_levels)
                {
                    state.levels = std::move(maybe_levels.value());

                    state.created.resize(cfg.max_num_levels, 0);
                    for(auto i = 0UL; i < state.levels.size(); ++i)
                        state.created[i] = state.levels[i].size() > 0 ? 1 : 0;

                    for(const auto& level : state.levels)
                    {
                        for(const auto& sst_ptr : level)
                        {
                            max_flush_iteration = std::max(max_flush_iteration, flush_iteration_from_filename(sst_ptr->path()));
                        }
                    }
                }
            }

            ok = sst_manager_helpers::_check_orphans(state, dir_path);
            if(!ok)
                return hedge::error(std::format("Error while checking orphans: {}", ok.error().to_string()));

            return hedge::ok();
        }
    };

    hedge::expected<std::unique_ptr<sst_manager>> sst_manager::load(const config& cfg,
                                                                    std::shared_ptr<io::io_executor> compaction_pool,
                                                                    std::shared_ptr<sharded_page_cache> page_cache)
    {
        auto mgr = std::make_unique<sst_manager>(cfg, std::move(compaction_pool), std::move(page_cache));

        if(!std::filesystem::exists(cfg.indices_path))
            return mgr;

        size_t max_flush_iteration = 0;

        for(auto& [partition_id, partition_state_ptr] : mgr->_sorted_indices)
        {
            auto status = sst_manager_helpers::_load_partition(cfg, partition_state_ptr, partition_id, max_flush_iteration);
            if(!status)
                return hedge::error(std::format("Error while loading prefix {}: {}", partition_id, status.error().to_string()));
        }

        mgr->_flush_iteration.store(max_flush_iteration + 1, std::memory_order::relaxed);

        return mgr;
    }

} // namespace hedge::db