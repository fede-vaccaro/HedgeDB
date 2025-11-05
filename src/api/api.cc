#include <cstdint>
#include <cstring>
#include <memory>

#include "async/io_executor.h"
#include "async/task.h"
#include "../db/database.h"

#include "api.h"

namespace hedge
{
    class db_impl
    {
        std::shared_ptr<db::database> _db;
        std::shared_ptr<async::executor_context> _executor;

    public:
        db_impl(std::shared_ptr<db::database> db, std::shared_ptr<async::executor_context> executor)
            : _db(std::move(db)), _executor(std::move(executor))
        {
        }

        auto& db()
        {
            return this->_db;
        }

        auto& executor()
        {
            return this->_executor;
        }
    };

    db::db_config convert_config(const config& src)
    {
        static_assert(sizeof(db::db_config) == sizeof(config));

        db::db_config cfg{};

        std::memcpy(reinterpret_cast<uint8_t*>(&cfg), reinterpret_cast<const uint8_t*>(&src), sizeof(db::db_config));

        return cfg;
    };

    expected<std::shared_ptr<api>> api::open_database(const std::string& path)
    {
        auto maybe_db = db::database::load(path);

        if(!maybe_db)
            return maybe_db.error();

        auto api_ptr = std::make_shared<api>();

        api_ptr->_db_impl = std::make_unique<db_impl>(
            std::move(maybe_db.value()),
            std::make_shared<async::executor_context>(128));

        return api_ptr;
    }

    expected<std::shared_ptr<api>> api::create_database(const std::string& path, const config& config)
    {
        auto maybe_db = db::database::make_new(path, convert_config(config));

        if(!maybe_db)
            return maybe_db.error();

        auto api_ptr = std::make_shared<api>();

        api_ptr->_db_impl = std::make_unique<db_impl>(
            std::move(maybe_db.value()),
            std::make_shared<async::executor_context>(128));

        return api_ptr;
    }

    void api::get(key_t key, get_callback_t callback)
    {
        auto task = this->_db_impl->db()->get_async(key, this->_db_impl->executor());

        auto task_instantiator = [](const std::shared_ptr<db::database>& db,
                                    const std::shared_ptr<async::executor_context>& executor,
                                    key_t key,
                                    get_callback_t callback) -> async::task<void>
        {
            auto result = co_await db->get_async(key, executor);
            callback(std::move(result));
        };

        this->_db_impl->executor()->submit_io_task(
            task_instantiator(
                this->_db_impl->db(),
                this->_db_impl->executor(),
                key,
                std::move(callback)));
    }

    void api::put(key_t key, std::vector<uint8_t> value, put_callback_t callback)
    {
        auto task = this->_db_impl->db()->put_async(
            key,
            std::move(value),
            this->_db_impl->executor());

        auto task_instantiator = [](const std::shared_ptr<db::database>& db,
                                    const std::shared_ptr<async::executor_context>& executor,
                                    key_t key,
                                    std::vector<uint8_t> value,
                                    put_callback_t callback) -> async::task<void>
        {
            auto status = co_await db->put_async(key, std::move(value), executor);
            callback(std::move(status));
        };

        this->_db_impl->executor()->submit_io_task(
            task_instantiator(this->_db_impl->db(),
                              this->_db_impl->executor(),
                              key, std::move(value),
                              std::move(callback)));
    }

    void api::remove(key_t key, remove_callback_t callback)
    {
        auto task = this->_db_impl->db()->remove_async(key, this->_db_impl->executor());

        auto task_instantiator = [](const std::shared_ptr<db::database>& db,
                                    const std::shared_ptr<async::executor_context>& executor,
                                    key_t key,
                                    remove_callback_t callback) -> async::task<void>
        {
            auto status = co_await db->remove_async(key, executor);
            callback(std::move(status));
        };

        this->_db_impl->executor()->submit_io_task(
            task_instantiator(
                this->_db_impl->db(),
                key, std::move(callback)));
    }

    async::task<expected<std::vector<uint8_t>>> api::get(key_t key)
    {
        co_return co_await this->_db_impl->db()->get_async(key, this->_db_impl->executor());
    }

    async::task<hedge::status> api::put(key_t key, std::vector<uint8_t> value)
    {
        co_return co_await this->_db_impl->db()->put_async(key, std::move(value), this->_db_impl->executor());
    }

    async::task<hedge::status> api::remove(key_t key)
    {
        co_return co_await this->_db_impl->db()->remove_async(key, this->_db_impl->executor());
    }

    void api::transaction(async::task<hedge::status> task, transaction_callback_t callback)
    {
        auto task_instantiator = [](async::task<hedge::status> task,
                                    transaction_callback_t callback) -> async::task<void>
        {
            auto status = co_await task;

            if(callback)
                callback(status);
        };

        this->_db_impl->executor()->submit_io_task(
            task_instantiator(
                std::move(task),
                std::move(callback)));
    }

    std::future<hedge::status> api::compact_sorted_indices(bool ignore_ratio)
    {
        return this->_db_impl->db()->compact_sorted_indices(ignore_ratio);
    }

    double api::read_amplification_factor()
    {
        return this->_db_impl->db()->read_amplification_factor();
    }

    hedge::status api::flush()
    {
        return this->_db_impl->db()->flush();
    }

} // namespace hedge