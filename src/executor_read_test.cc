#include <atomic>
#include <bits/types/timer_t.h>

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <liburing.h>
#include <random>
#include <stdexcept>
#include <utility>

#include <logger.h>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"
#include "async/task.h"
#include "perf_counter.h"

using namespace std::string_literals;

std::pair<int, size_t> open_fd(const std::string& path, bool direct = true)
{
    auto flag = O_RDWR;
    if(direct)
        flag |= O_DIRECT;

    int fd = open(path.c_str(), flag);
    if(fd < 0)
    {
        throw std::runtime_error("couldnt open"s + path);
    }

    auto size = std::filesystem::file_size(path);

    std::cout << ""
                 "Opened file "
              << path << " of size " << size << " bytes\n";
    return {fd, size};
}

const std::string INDEX_PATH = "/tmp/testfile";

size_t PAGE_SIZE = 4096;

class working_group
{
    std::atomic_uint64_t _counter{0};
    std::atomic_bool _done{false};

    std::condition_variable _cv;
    std::mutex _mutex;

public:
    void incr()
    {
        this->_counter++;
    }

    void set(size_t count)
    {
        this->_counter = count;
    }

    void decr()
    {
        this->_counter--;

        this->_done = this->_counter == 0;

        if(this->_done)
            this->_cv.notify_all();
    }

    void wait()
    {
        std::unique_lock lk(this->_mutex);
        this->_cv.wait(lk, [this]()
                       { return this->_done.load(); });
    }
};

std::atomic_uint64_t atomic_counter = 0;

std::vector<int> createAlignedFiles()
{
    const size_t TOTAL_GIB = 16;
    const size_t FILE_SIZE_MIB = 32;
    const size_t BYTES_PER_MIB = 1024 * 1024;
    const size_t FILE_SIZE_BYTES = FILE_SIZE_MIB * BYTES_PER_MIB;
    const int NUM_FILES = (TOTAL_GIB * 1024) / FILE_SIZE_MIB;

    std::vector<int> fds;
    fds.reserve(NUM_FILES);

    // Allocate a page-aligned buffer for writing
    void* buffer = nullptr;
    if(posix_memalign(&buffer, PAGE_SIZE, FILE_SIZE_BYTES) != 0)
    {
        throw std::runtime_error("Failed to allocate page-aligned memory.");
    }

    // Fill the buffer with the pattern (i % 256)
    unsigned char* bytePtr = static_cast<unsigned char*>(buffer);
    for(size_t i = 0; i < FILE_SIZE_BYTES; ++i)
    {
        bytePtr[i] = static_cast<unsigned char>(i % 256);
    }

    std::cout << "Starting file creation..." << std::endl;

    for(int i = 0; i < NUM_FILES; ++i)
    {
        std::string filename = "/tmp/fio/data_file_" + std::to_string(i) + ".bin";

        // Open file: Read/Write, Create if doesn't exist, Truncate if it does
        int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR);

        if(fd == -1)
        {
            std::cerr << "\nError opening file " << filename << ". Check ulimit -n." << std::endl;
            break;
        }

        auto r = fallocate(fd, 0, 0, FILE_SIZE_BYTES);
        if(r != 0)
        {
            std::cerr << "\nError allocating space for file " << filename << std::endl;
            close(fd);
            break;
        }

        // Write the aligned buffer to the file
        ssize_t bytesWritten = write(fd, buffer, FILE_SIZE_BYTES);
        if(bytesWritten == -1)
        {
            std::cerr << "\nError writing to file " << filename << std::endl;
            close(fd);
            break;
        }

        fds.push_back(fd);

        // Progress Reporting
        float progress = (static_cast<float>(i + 1) / NUM_FILES) * 100.0f;
        std::cout << "\rProgress: " << std::fixed << std::setprecision(2)
                  << progress << "% (" << (i + 1) << "/" << NUM_FILES << " files)" << std::flush;
    }

    std::cout << "\nFinished. Total file descriptors held: " << fds.size() << std::endl;

    // Free the allocated buffer (files remain open via fds)
    free(buffer);

    return fds;
}

hedge::async::task<void> get_obj([[maybe_unused]] hedge::async::read_request r, [[maybe_unused]] hedge::async::read_request r2, [[maybe_unused]] hedge::async::executor_context& executor, working_group& wg)
{
    std::array<char, 1241> buffer{};
    hedge::prof::do_not_optimize(buffer);

    // hedge::prof::counter_guard guard(hedge::prof::get<"test_task">());
    auto t0 = std::chrono::high_resolution_clock::now();
    hedge::prof::do_not_optimize(t0);

    while(true)
    {
        auto t1 = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0);
        if(duration.count() > 100)
            break;
    }

    // [[maybe_unused]] auto data_ptr = static_cast<uint8_t*>(aligned_alloc(PAGE_SIZE, PAGE_SIZE));
    // r.data = data_ptr;

    // [[maybe_unused]] auto response_f = co_await executor.submit_request(r);

    // free(data_ptr);
    // auto response2 = co_await executor.submit_request(r2);

    // auto response3 = co_await executor.submit_request(r);

    // if(response.bytes_read != PAGE_SIZE)
    // {
    //     std::cerr << "Error: expected size " << PAGE_SIZE << ", got " << response.bytes_read << " at offset " << r.offset << "\n";
    //     throw std::runtime_error("Data size mismatch");
    // }

    // std::vector<uint8_t> data{};
    // data.assign(static_cast<uint8_t*>(response.data.get()),
    //             static_cast<uint8_t*>(response.data.get()) + response.bytes_read);

    // log("[get_obj] received data of size: ", data.size());

    // auto start_value = r.offset / sizeof(uint64_t);

    // auto* ptr = reinterpret_cast<uint64_t*>(data.data());
    // for(size_t i = 0; i < data.size() / sizeof(uint64_t); ++i)
    // {
    //     if(start_value + i != ptr[i])
    //     {
    //         std::cerr << "Error: expected value " << start_value << ", got " << ptr[i] << " at index " << i << " and offset " << r.offset << "\n";
    //         throw std::runtime_error("Data mismatch");
    //     }
    // }

    // log_always("[get_obj] task complete with size ", data.size(), "counting up to: ", atomic_counter++);

    wg.decr();

    co_return;
}

int main()
{
    working_group wg;

    const uint32_t QUEUE_DEPTH = 64;
    std::vector<std::shared_ptr<hedge::async::executor_context>> contexts;

    contexts.reserve(8);
    for(int i = 0; i < 8; ++i)
        contexts.emplace_back(hedge::async::executor_context::make_new(QUEUE_DEPTH));

    std::vector<int> fds;
    [[maybe_unused]] size_t file_size = 0;
    for(int i = 0; i < 1; ++i)
    {
        auto [fd, size] = open_fd(INDEX_PATH, true);
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        fds.push_back(fd);
        file_size = size;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    // auto fds = createAlignedFiles();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto duration_setup = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Created " << fds.size() << " files in " << duration_setup.count() / 1000.0 << " ms." << std::endl;
    std::cout << "Throughput: " << (static_cast<double>(fds.size() * (32 * 1024 * 1024)) / (1024.0 * 1024.0)) / (duration_setup.count() / 1000000.0) << " MiB/sec." << std::endl;

    auto rng = std::mt19937(std::random_device{}());
    auto dist = std::uniform_int_distribution<size_t>(0, (1024 * 1024 * 32UL / PAGE_SIZE) - 1);

    auto N_REQUESTS = 20'000'000UL;
    wg.set(N_REQUESTS);

    std::vector<size_t> offsets;
    offsets.reserve(N_REQUESTS);
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        offsets.push_back(dist(rng) * PAGE_SIZE);
    }

    std::vector<size_t> offsets2;
    offsets2.reserve(N_REQUESTS);
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        offsets2.push_back(dist(rng) * PAGE_SIZE);
    }

    t0 = std::chrono::high_resolution_clock::now();
    hedge::prof::do_not_optimize(t0);
    size_t j = 0;
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        auto* context = contexts[j++ % contexts.size()].get();
        auto task = get_obj(
            hedge::async::read_request{.fd = fds[i % fds.size()], .offset = offsets[i], .size = PAGE_SIZE},
            hedge::async::read_request{.fd = fds[i % fds.size()], .offset = offsets2[i], .size = PAGE_SIZE},
            *context, wg);
        // hedge::prof::do_not_optimize(task);
        while(!context->try_submit_io_task(task))
        {
            context = contexts[j++ % contexts.size()].get();
        };
        // context->submit_io_task(std::move(task));
    }
    std::cout << "Submitted " << N_REQUESTS << " jobs\n";
    wg.wait();
    t1 = std::chrono::high_resolution_clock::now();
    hedge::prof::do_not_optimize(t1);

    std::cout << "All tasks completed, shutting down executors..." << std::endl;

    for(auto& context : contexts)
        context->shutdown();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Processed " << N_REQUESTS << " requests in " << duration.count() / 1000.0 << " ms." << std::endl;
    std::cout << "Average time per request: " << static_cast<double>(duration.count()) / N_REQUESTS << " us." << std::endl;
    std::cout << "Average throughput: " << static_cast<size_t>(static_cast<double>(N_REQUESTS) / (duration.count() / 1000000.0)) << " requests/sec." << std::endl;

    hedge::prof::print_internal_perf_stats();
}