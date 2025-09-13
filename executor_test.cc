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

#include "io_executor.h"
#include "logger.h"
#include "task.h"
#include "mailbox_impl.h"

using namespace std::string_literals;

std::pair<int, size_t> open_fd(const std::string& path, bool direct = true)
{
    auto flag = O_RDONLY;
    if(direct)
        flag |= O_DIRECT;

    int fd = open(path.c_str(), flag);
    if(fd < 0)
    {
        throw std::runtime_error("couldnt open"s + path);
    }

    auto size = std::filesystem::file_size(path);
    return {fd, size};
}

const std::string INDEX_PATH = "/tmp/iota.bin";

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

task<void> get_obj(read_request r, executor_context& executor, working_group& wg)
{
    auto response = co_await executor.submit_request(r);

    std::vector<uint8_t> data{};
    data.assign(static_cast<uint8_t*>(response.data.get()),
                static_cast<uint8_t*>(response.data.get()) + response.data_size);

    // log("[get_obj] received data of size: ", data.size());

    auto start_value = r.offset / sizeof(uint64_t);

    auto* ptr = reinterpret_cast<uint64_t*>(data.data());
    for(size_t i = 0; i < data.size() / sizeof(uint64_t); ++i)
    {
        if(start_value + i != ptr[i])
        {
            std::cerr << "Error: expected value " << start_value << ", got " << ptr[i] << " at index " << i << " and offset " << r.offset << "\n";
            throw std::runtime_error("Data mismatch");
        }
    }

    // log_always("[get_obj] task complete with size ", data.size(), "counting up to: ", atomic_counter++);

    wg.decr();
}

int main()
{
    working_group wg;
    
    const uint32_t QUEUE_DEPTH = 64;
    executor_context context(QUEUE_DEPTH);

    auto [fd, file_size] = open_fd(INDEX_PATH);

    auto rng = std::mt19937(std::random_device{}());
    auto dist = std::uniform_int_distribution<size_t>(0, file_size / PAGE_SIZE - 10);

    auto N_REQUESTS = 1000000;
    wg.set(N_REQUESTS);

    auto t0 = std::chrono::steady_clock::now();
    for(size_t i = 0; i < N_REQUESTS; i++)
    {
        auto task = get_obj(read_request{.fd = fd, .offset = dist(rng) * PAGE_SIZE, .size = PAGE_SIZE}, context, wg);
        context.submit_io_task(std::move(task));
    }
    std::cout << "Submitted " <<  N_REQUESTS << " jobs\n";
    wg.wait();
    auto t1 = std::chrono::steady_clock::now();

    context.shutdown();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);
    std::cout << "Processed " << N_REQUESTS << " requests in " << duration.count() / 1000.0 << " ms." << std::endl;
    std::cout << "Average time per request: " << static_cast<double>(duration.count()) / N_REQUESTS << " us." << std::endl;
    std::cout << "Average throughput: " << static_cast<size_t>(static_cast<double>(N_REQUESTS) / (duration.count() / 1000000.0)) << " requests/sec." << std::endl;
}