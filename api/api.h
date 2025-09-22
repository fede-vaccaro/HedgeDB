#pragma once

#include <functional>
#include <future>

#include <error.hpp>
#include <uuid.h>

#include "config.h"

#include "async/task.h"

namespace hedge
{

    class db_impl;

    class api
    {
        std::unique_ptr<db_impl> _db_impl;

    public:
        using key_t = uuids::uuid;

        static expected<std::shared_ptr<api>> open_database(const std::string& path);
        static expected<std::shared_ptr<api>> create_database(const std::string& path, const config& config);

        // async callback api
        using get_callback_t = std::function<void(expected<std::vector<uint8_t>>)>;
        using put_callback_t = std::function<void(hedge::status)>;
        using remove_callback_t = std::function<void(hedge::status)>;

        void get(key_t key, get_callback_t callback);
        void remove(key_t key, remove_callback_t callback);
        void put(key_t key, std::vector<uint8_t> value, put_callback_t callback);

        // async coro api
        using transaction_callback_t = std::function<void(hedge::status)>;
        async::task<expected<std::vector<uint8_t>>> get(key_t key);
        async::task<hedge::status> put(key_t key, std::vector<uint8_t> value);
        async::task<hedge::status> remove(key_t key);

        void transaction(async::task<hedge::status> task, transaction_callback_t callback = {});

        std::future<hedge::status> compact_sorted_indices(bool ignore_ratio);

        double load_factor();
        hedge::status flush();

        api() = default;
        ~api() = default;

        api(const api&) = delete;
        api(api&&) noexcept = default;

        api& operator=(const api&) = delete;
        api& operator=(api&&) noexcept = default;
    };

} // namespace hedge