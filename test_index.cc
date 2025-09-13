#include <random>
#include <filesystem>

#include <error.h>
#include <uuid.h>

#include "index.h"

int main(int argc, char* argv[])
{
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> dist(0, 15);
    uuids::uuid_random_generator gen{generator};

    auto N_UUIDS = std::stoul(argv[1]);
    auto BASE_PATH = std::filesystem::path(argv[2]);

    std::cout << "N_UUIDS: " << N_UUIDS << '\n';
    std::cout << "BASE_PATH: " << BASE_PATH << '\n';

    auto constexpr FLUSH_INDICES_LIMIT = 5; // Flush every 5 indices (memory intensive!)
    auto constexpr INDEX_MAX_SIZE = 2 * 1000 * 1000; // 2 million entries

    std::vector<hedgehog::db::mem_index> indices;

    hedgehog::db::mem_index current_index;

    auto elapsed_total = 0;

    for (size_t i = 0; i < N_UUIDS; ++i)
    {
        current_index.add(gen(), {static_cast<uint64_t>(i), static_cast<uint32_t>(i % 100), 0});
        
        if (current_index.size() >= INDEX_MAX_SIZE)
        {
            indices.emplace_back();

            std::swap(indices.back(), current_index);
        }

        if (indices.size() >= FLUSH_INDICES_LIMIT)
        {
            std::cout << "Flushing " << (FLUSH_INDICES_LIMIT*INDEX_MAX_SIZE) << " indices to disk... UUID is: " << i << "\n";

            auto t0 = std::chrono::high_resolution_clock::now();
            auto status = hedgehog::db::index_ops::flush_and_compact(BASE_PATH, std::move(indices), true);
            auto t1 = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            
            elapsed_total += elapsed;

            if (!status)
            {
                std::cerr << "Failed to flush indices: " << status.error().to_string() << '\n';
                return 1;
            }

            std::cout << "Flushed " << (FLUSH_INDICES_LIMIT*INDEX_MAX_SIZE) << " entries in " << elapsed << " ms\n";
            std::cout << "Flush rate: " << (double)(FLUSH_INDICES_LIMIT*INDEX_MAX_SIZE)/elapsed*1000.0 << " key/s\n";

            indices.clear();
        }
    }

    if(current_index.size() > 0)
        indices.emplace_back(std::move(current_index));

    if (!indices.empty())
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        auto status = hedgehog::db::index_ops::flush_and_compact(BASE_PATH, std::move(indices));
        auto t1 = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        elapsed_total += elapsed;

        if (!status)
        {
            std::cerr << "Failed to flush remaining indices: " << status.error().to_string() << '\n';
            return 1;
        }

        std::cout << "Flushed remaining entries in " << elapsed << " ms\n";

        std::cout << "Total time for flushing: " << (double)elapsed_total/1000 << " s\n";
    }

}