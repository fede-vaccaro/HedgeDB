#include "asio/any_io_executor.hpp"
#include "asio/io_context.hpp"
#include <asio/as_tuple.hpp>
#include <asio/use_awaitable.hpp>
#include <asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/random_access_file.hpp>
#include <asio/buffer.hpp>
#include <chrono>
#include <vector>
#include <random>
#include <iostream>
#include <thread>

asio::awaitable<std::vector<uint8_t>> read_some(size_t size, uint64_t offset, asio::random_access_file& file)
{
    std::vector<uint8_t> buffer(size);
    auto [ec, n] = co_await file.async_read_some_at(offset, asio::buffer(buffer), asio::as_tuple(asio::use_awaitable));
    
    if (ec && ec != asio::error::eof)
    {
        std::cerr << "Error reading file: " << ec.message() << std::endl;
        co_return std::vector<uint8_t>{};
    }

    co_return buffer;
}


int main()
{
    asio::io_context io_context;
    auto work_guard = asio::make_work_guard(io_context);

    asio::random_access_file file(io_context, "/tmp/testfile", asio::file_base::read_only);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, file.size() / 4096 - 20);

    auto N_READS = 10000;

    std::vector<uint64_t> offsets;
    offsets.reserve(N_READS);
    for(int i = 0; i < N_READS; ++i)
        offsets.push_back(dist(gen) * 4096);

    for(int i = 0; i < N_READS; ++i)
    {
        asio::co_spawn(io_context, [&file, offs = offsets[i]]() -> asio::awaitable<void> {
            auto data = co_await read_some(1024*4, offs, file);
        }, asio::as_tuple(asio::detached));    
    }

    std::cout << "Spawning read operations..." << std::endl;
    auto t0 = std::chrono::steady_clock::now();

    auto num_threads = 1;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&io_context] { io_context.run(); });
    }

    work_guard.reset(); // Release the work guard

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }   

    io_context.stop(); // Force stop after work guard is released, if still running

    auto t1 = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count() / 1000.0;

    std::cout << "All read operations completed in " << elapsed << " ms." << std::endl;
    std::cout << "Read/s: " << (N_READS / (elapsed / 1000.0)) << std::endl;

    file.close();
    std::cout << "Test completed successfully." << std::endl;



    return 0;
}