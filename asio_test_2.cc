#include "asio/io_context.hpp"
#include "asio/random_access_file.hpp"
#include "asio/buffer.hpp"
#include "asio/error.hpp"
#include <chrono>
#include <vector>
#include <random>
#include <iostream>
#include <thread>
#include <atomic>
#include <memory>

int main()
{
    asio::io_context io_context;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard(asio::make_work_guard(io_context));

    asio::random_access_file file(io_context, "/tmp/testfile", asio::file_base::read_only);

    if (!file.is_open()) {
        std::cerr << "Failed to open file\n";
        return 1;
    }    

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, file.size()/4096 - 20);

    int N_READS = 10000;

    std::vector<uint64_t> offsets;
    offsets.reserve(N_READS);
    for(int i = 0; i < N_READS; ++i)
        offsets.push_back(dist(gen)*4096);

    std::atomic<int> completed_reads = 0;

    for(int i = 0; i < N_READS; ++i)
    {
        uint64_t current_offset = offsets[i];
        auto buffer = std::make_shared<std::vector<uint8_t>>(1024*4);

        file.async_read_some_at(current_offset, asio::buffer(*buffer),
            [&io_context, &completed_reads, N_READS, buffer](asio::error_code ec, size_t n) mutable {
                if (ec && ec != asio::error::eof)
                {
                    std::cerr << "Error reading file: " << ec.message() << std::endl;
                }

                if (++completed_reads == N_READS) {
                    io_context.stop();
                }
            });
    }

    std::cout << "Spawning read operations..." << std::endl;
    auto t0 = std::chrono::steady_clock::now();

    int num_threads = 1;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&io_context] { io_context.run(); });
    }

    work_guard.reset();

    for (auto& t : threads) {
        t.join();
    }   

    io_context.stop();

    auto t1 = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000.0;

    std::cout << "All read operations completed in " << elapsed << " ms." << std::endl;
    std::cout << "Read/s: " << (N_READS / (elapsed / 1000.0)) << std::endl;

    file.close();
    std::cout << "Test completed successfully." << std::endl;

    return 0;
}