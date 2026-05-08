// Smoke-test: exercises the core API patterns from the getting-started guide.

#include <cassert>
#include <filesystem>
#include <iostream>
#include <variant>
#include <vector>

#include "db/database.h"
#include "io/static_pool.h"
#include "tmc/sync.hpp"
#include "tmc/task.hpp"

tmc::task<void> run(std::shared_ptr<hedge::db::database> db)
{
    // Single put + get
    hedge::key_t k{"test_key"};
    std::string v{"Hello, world!"};

    if(auto s = co_await db->put_async(k, std::as_bytes(std::span{v})); !s)
    {
        std::cerr << "put failed: " << s.error().to_string() << "\n";
        co_return;
    }

    auto maybe_value = co_await db->get_async(k);
    if(!maybe_value)
    {
        std::cerr << "get failed: " << maybe_value.error().to_string() << "\n";
        co_return;
    }

    const auto& bytes = maybe_value.value();
    std::string_view readback(
        reinterpret_cast<const char*>(bytes.data()), bytes.size());
    std::cout << "read back value: " << readback << "\n";

    // 10 writes sharing the same key prefix
    constexpr int32_t NUM_ENTRIES = 10;
    for(int i = 0; i < NUM_ENTRIES; ++i)
    {
        hedge::key_t ki{"prefix_" + std::to_string(i)};
        std::string v{"Hello, I am: " + std::to_string(i)};
        if(auto s = co_await db->put_async(ki, std::as_bytes(std::span{v})); !s)
        {
            std::cerr << "put failed: " << s.error().to_string() << "\n";
            co_return;
        }
    }

    // Range scan over that partition, starting from "prefix_"
    hedge::key_t range_key_start{"prefix_0"};
    hedge::key_t range_key_end{"prefix_9"};
    auto maybe_it = db->scan(range_key_start, range_key_end);
    if(!maybe_it)
    {
        std::cerr << "failed to create iterator: " << maybe_it.error().to_string() << "\n";
        co_return;
    }

    auto it = std::move(maybe_it.value());
    while(auto entry = co_await it.next())
    {
        auto& [key, val] = entry.value();
        std::string_view key_str(reinterpret_cast<const char*>(key.data()), key.size());
        assert(std::holds_alternative<std::vector<std::byte>>(val));

        const auto& bytes = std::get<std::vector<std::byte>>(val);
        std::string_view val_str(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        std::cout << key_str << " -> " << val_str << "\n";
    }
}

int main()
{
    const std::filesystem::path db_path = "/tmp/examples_db";
    if(std::filesystem::exists(db_path))
        std::filesystem::remove_all(db_path);

    hedge::io::static_pool::instance()->init(hedge::io::executor_config{
        .name = "examples-pool",
        .queue_depth = 16,
        .n_threads = 4,
    });

    auto maybe_db = hedge::db::database::make_new(db_path, hedge::db::db_config{});
    if(!maybe_db)
    {
        std::cerr << "failed to create database: " << maybe_db.error().to_string() << "\n";
        return 1;
    }

    std::shared_ptr<hedge::db::database> db = std::move(maybe_db.value());

    tmc::post_waitable(*hedge::io::static_pool::instance(), run(db)).wait();

    db.reset();
    hedge::io::static_pool::instance()->shutdown();
}
