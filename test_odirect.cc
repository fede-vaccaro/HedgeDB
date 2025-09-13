#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <iostream>
#include <fcntl.h>
#include <fstream>
#include <format>
#include <filesystem>
#include <random>
#include <unistd.h>


constexpr size_t PAGE_SIZE = 4 * 1024; // 4KB alignment

int main(int argc, char *argv[])
{

    std::string file_path = argv[1];
    size_t N_READS = std::stoul(argv[2]);

    std::cout << "File path: " << file_path << std::endl;
    std::cout << "Number of reads: " << N_READS << std::endl;

    auto file_size = std::filesystem::file_size(file_path);
    std::cout << "File size: " << file_size << " bytes" << std::endl;

    std::random_device rd{};
    std::mt19937 gen{rd()};

    assert(file_size % PAGE_SIZE == 0 && "File size must be a multiple of alignment (4KB)");

    std::uniform_int_distribution<size_t> dist(0, file_size/(PAGE_SIZE*2));

    // open fd with O_DIRECT
    int fd = open(file_path.c_str(), O_RDONLY | O_DIRECT);

    if (fd < 0) {
        std::cerr << "Error opening file with O_DIRECT: " << strerror(errno) << std::endl;
        return 1;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N_READS; i++)
    {

        uint8_t* aligned_raw_buffer = nullptr;
        if (posix_memalign(reinterpret_cast<void**>(&aligned_raw_buffer), PAGE_SIZE, PAGE_SIZE * 2) != 0) {
            std::string error_msg = std::format("Failed to allocate aligned memory for O_DIRECT: {}", strerror(errno));
            std::cerr << error_msg << std::endl;
            return 1;
        }

        auto aligned_offset = dist(gen);

        off_t seek_result = lseek(fd, aligned_offset * PAGE_SIZE, SEEK_SET);    

        if (seek_result < 0) {
            std::cerr << "Error seeking in file: " << strerror(errno) << std::endl;
            free(aligned_raw_buffer);
            return 1;
        }

        ssize_t bytes_read = read(fd, aligned_raw_buffer, PAGE_SIZE*2);

        if (bytes_read != static_cast<ssize_t>(PAGE_SIZE*2)) {
            std::cerr << "Error reading from file: " << strerror(errno) << " read only: " << bytes_read << std::endl;
            free(aligned_raw_buffer);
            return 1;
        }

        free(aligned_raw_buffer); // Free the aligned memory

    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    std::cout << "Time taken for " << N_READS << " reads: " << duration / 1000.0 << " ms" << std::endl;
    std::cout << "Average time per read: " << static_cast<double>(duration / 1000.0) / N_READS << " ms" << std::endl;
    std::cout << "4K-page reads per second: " << (N_READS * 1000.0 * 1000.0 / duration) << std::endl;

    close(fd);
    return 0;
}
