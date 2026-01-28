#include <atomic>
#include <gtest/gtest.h>
#include <optional>
#include <thread>
#include <vector>

#include "async/mpsc.h" // Assuming mpsc_queue is defined in this header
#include "async/spinlock.h"

// --- Functional Tests ---

TEST(MpscQueueTest, BasicPushPop)
{
    hedge::async::mpsc_queue<int, 4UL> queue;

    EXPECT_TRUE(queue.push_back(100));
    EXPECT_TRUE(queue.push_back(200));

    auto val1 = queue.pop_front();
    ASSERT_TRUE(val1.has_value());
    EXPECT_EQ(*val1, 100);

    auto val2 = queue.pop_front();
    ASSERT_TRUE(val2.has_value());
    EXPECT_EQ(*val2, 200);

    EXPECT_FALSE(queue.pop_front().has_value());
}

TEST(MpscQueueTest, FullQueueReturnsFalse)
{
    hedge::async::mpsc_queue<int, 2> queue;

    EXPECT_TRUE(queue.push_back(1));
    EXPECT_TRUE(queue.push_back(2));

    // This should fail as the capacity is 2
    EXPECT_FALSE(queue.push_back(3));
}

TEST(MpscQueueTest, WrapAroundBehavior)
{
    hedge::async::mpsc_queue<int, 2> queue;

    // Fill and empty once
    queue.push_back(1);
    queue.push_back(2);
    queue.pop_front();
    queue.pop_front();

    // Push again to test index masking (wrap around)
    EXPECT_TRUE(queue.push_back(3));
    auto val = queue.pop_front();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 3);
}

// --- Stress Test ---

class MpscStressTest : public ::testing::Test
{
protected:
    static constexpr size_t QUEUE_SIZE = 2048;
    static constexpr int PRODUCERS = 4;
    static constexpr int OPS_PER_PRODUCER = 40'000'000;
    static constexpr size_t TOTAL_OPS = PRODUCERS * OPS_PER_PRODUCER;
};

TEST_F(MpscStressTest, MultiProducersSingleConsumer)
{
    hedge::async::mpsc_queue<size_t, QUEUE_SIZE> queue;
    std::atomic<bool> start_signal{false};

    // Consumer logic: sum up all values received
    // We use size_t to avoid overflow during the summation
    size_t consumed_count = 0;
    size_t checksum = 0;

    auto consumer_func = [&]()
    {
        int s{0};
        while(consumed_count < TOTAL_OPS)
        {
            auto val = queue.pop_front();
            if(val)
            {
                checksum += *val;
                consumed_count++;
            }
            else
            {
                // Keep the CPU from melting, but stay responsive
                hedge::async::nanosleep(s);
            }
        }
    };

    // Producer logic: push '1' into the queue repeatedly
    auto producer_func = [&]()
    {
        // Busy-wait for start signal for better simultaneous start
        while(!start_signal.load())
        {
            std::this_thread::yield();
        }

        int s{0};
        for(int i = 0; i < OPS_PER_PRODUCER; ++i)
        {
            while(!queue.push_back(1))
            {
                hedge::async::nanosleep(s);
            }
        }
    };

    // Launch threads
    std::thread consumer_thread(consumer_func);
    std::vector<std::thread> producer_threads;
    producer_threads.reserve(PRODUCERS);

    for(int i = 0; i < PRODUCERS; ++i)
    {
        producer_threads.emplace_back(producer_func);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    start_signal.store(true);

    // Join all
    for(auto& t : producer_threads)
        t.join();
    consumer_thread.join();
    auto end_time = std::chrono::high_resolution_clock::now();

    // Validation
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    EXPECT_EQ(consumed_count, TOTAL_OPS);
    EXPECT_EQ(checksum, TOTAL_OPS);

    std::cout << "[          ] Throughput: "
              << (TOTAL_OPS / (duration.count() / 1000.0)) / 1e6
              << " million ops/sec" << std::endl;
}