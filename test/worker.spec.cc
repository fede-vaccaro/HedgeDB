#include <chrono>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include "worker.h"

TEST(worker_test, basic_test)
{
    hedge::async::worker worker{};

    std::cout << "main thread sleeping for 2 secs\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "main thread slept for 2 secs\n";

    std::promise<size_t> promise;
    auto future = promise.get_future();

    worker.submit([&promise]()
                  { std::this_thread::sleep_for(std::chrono::seconds(2));
                std::cout << "slept for 2 secs\n";
            promise.set_value(42); });

    size_t val = future.get();
    std::cout << "[main thread] value is: " << val << "\n";

    worker.submit([]()
                  {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    std::cout << "slept for 1 sec\n"; });

    worker.submit([]()
                  { std::cout << "nothing special going on the worker thread\n"; });

    worker.shutdown();
}