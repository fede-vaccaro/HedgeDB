#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "../io_executor.h"
#include "../value_table.h"

namespace hedgehog::db
{

    struct value_table_test : public ::testing::Test
    {
        void SetUp() override
        {
            if(std::filesystem::exists(this->_base_path))
                std::filesystem::remove_all(this->_base_path);

            std::filesystem::create_directories(this->_base_path);

            this->_executor = std::make_shared<hedgehog::async::executor_context>(128);
        }

        void TearDown() override
        {
            if(this->_executor)
            {
                this->_executor->shutdown();
                this->_executor.reset();
            }
        }

        std::filesystem::path _base_path = "/tmp/hh/values";

        uuids::uuid generate_uuid()
        {
            return this->gen();
        }

        std::vector<uint8_t> make_random_vec(size_t size)
        {
            std::vector<uint8_t> vec(size);

            static std::uniform_int_distribution<uint8_t> dist(0, 255);

            std::ranges::generate(vec, [this]()
                                  { return dist(this->_generator); });
            return vec;
        }

        static void check_read_result(const output_file& output_file, const uuids::uuid& key, const std::vector<uint8_t>& value)
        {
            ASSERT_EQ(output_file.header.key, key);
            ASSERT_EQ(output_file.header.file_size, value.size());
            ASSERT_EQ(output_file.binaries, value);
        }

        std::shared_ptr<hedgehog::async::executor_context> _executor;

        size_t seed{107279581};
        std::mt19937 _generator{seed};
        std::uniform_int_distribution<int> dist{0, 15};
        uuids::basic_uuid_random_generator<std::mt19937> gen{_generator};
    };

    TEST_F(value_table_test, test_create_write_read)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 22);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 22);

        auto value = make_random_vec(1024);
        auto reservation = table.get_write_reservation(value.size());

        ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

        auto key = this->generate_uuid();

        auto maybe_write_result = this->_executor->sync_submit(
            table.write_async(key,
                              value,
                              reservation.value(),
                              this->_executor));

        ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

        auto write_result = maybe_write_result.value();

        // check infos
        auto expected_info = value_table_info{
            .current_offset = write_result.offset + write_result.size,
            .items_count = 1,
            .occupied_space = value.size() + sizeof(file_header),
            .deleted_count = 0,
            .freed_space = 0};
        auto info = table.info();
        EXPECT_EQ(info, expected_info);

        auto read_result = this->_executor->sync_submit(
            table.read_async(write_result.offset, write_result.size, this->_executor));

        ASSERT_TRUE(read_result.has_value()) << "An error occurred while reading from the table: " << read_result.error().to_string();
        check_read_result(read_result.value(), key, value);
    }

    TEST_F(value_table_test, test_multiple_reservations_with_read_back)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 23);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 23);

        size_t payload_size = value_table::MAX_FILE_SIZE;

        std::vector<std::vector<uint8_t>> values;
        std::vector<value_table::write_reservation> reservations;
        std::vector<key_t> keys;
        std::vector<hedgehog::value_ptr_t> write_results;

        for(auto i = 0UL; i < value_table::TABLE_MAX_SIZE_BYTES / payload_size; ++i)
        {
            auto value = this->make_random_vec(payload_size);
            auto reservation = table.get_write_reservation(value.size());

            ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

            auto key = this->generate_uuid();

            auto maybe_write_result = this->_executor->sync_submit(
                table.write_async(key,
                                  value,
                                  reservation.value(),
                                  this->_executor));

            ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

            values.push_back(std::move(value));
            reservations.push_back(reservation.value());
            keys.push_back(key);
            write_results.push_back(maybe_write_result.value());

            // check infos
            auto expected_info = value_table_info{
                .current_offset = write_results.back().offset + write_results.back().size,
                .items_count = i + 1,
                .occupied_space = (i + 1) * (payload_size + sizeof(file_header)),
                .deleted_count = 0,
                .freed_space = 0};
            auto info = table.info();
            EXPECT_EQ(info, expected_info);
        }

        // now asking for a reservation will result in an error
        auto reservation = table.get_write_reservation(payload_size);
        ASSERT_FALSE(reservation.has_value()) << "Expected an error when trying to reserve space in a full table, but got: " << reservation.error().to_string();
        ASSERT_EQ(reservation.error().code(), errc::VALUE_TABLE_NOT_ENOUGH_SPACE);

        // test readback
        for(size_t i = 0; i < values.size(); ++i)
        {
            auto read_result = this->_executor->sync_submit(
                table.read_async(write_results[i].offset, write_results[i].size, this->_executor));

            ASSERT_TRUE(read_result.has_value()) << "An error occurred while reading from the table: " << read_result.error().to_string();

            check_read_result(read_result.value(), keys[i], values[i]);
        }
    }

    TEST_F(value_table_test, test_read_from_closed_table)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 24);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 24);

        auto value = make_random_vec(1024);
        auto reservation = table.get_write_reservation(value.size());

        ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

        auto key = this->generate_uuid();

        auto maybe_write_result = this->_executor->sync_submit(
            table.write_async(key,
                              value,
                              reservation.value(),
                              this->_executor));

        ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

        auto write_result = maybe_write_result.value();

        // check infos
        auto expected_info = value_table_info{
            .current_offset = write_result.offset + write_result.size,
            .items_count = 1,
            .occupied_space = value.size() + sizeof(file_header),
            .deleted_count = 0,
            .freed_space = 0};
        auto info = table.info();
        EXPECT_EQ(info, expected_info);

        // try to read from the closed table
        auto read_result = this->_executor->sync_submit(
            table.read_async(write_result.offset, write_result.size, this->_executor));

        ASSERT_TRUE(read_result.has_value()) << "An error occurred while reading from the closed table: " << read_result.error().to_string();
        auto& output_file = read_result.value();

        check_read_result(output_file, key, value);

        ASSERT_EQ(table.fd().path(), (this->_base_path / std::format("{}{}", table.id(), value_table::TABLE_FILE_EXTENSION)).string());
        ASSERT_FALSE(std::filesystem::exists(with_extension(table.fd().path(), value_table::TABLE_FILE_EXTENSION)));
    }

    TEST_F(value_table_test, test_reopen_not_closed_table_and_write)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 25);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 25);

        auto value = make_random_vec(1024);
        auto reservation = table.get_write_reservation(value.size());

        ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

        auto key = this->generate_uuid();

        auto maybe_write_result = this->_executor->sync_submit(
            table.write_async(key,
                              value,
                              reservation.value(),
                              this->_executor));

        ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

        // try to reopen the table without closing it
        auto reopen_maybe_table = value_table::load(table.fd().path(), fs::file_descriptor::open_mode::read_write);
        ASSERT_TRUE(reopen_maybe_table.has_value()) << "An error occurred while reopening the table: " << reopen_maybe_table.error().to_string();

        // write again to the reopened table
        auto new_value = make_random_vec(512);
        auto new_reservation = reopen_maybe_table.value().get_write_reservation(new_value.size());

        ASSERT_TRUE(new_reservation.has_value()) << "An error occurred while reserving space for writing in reopened table: " << new_reservation.error().to_string();

        auto new_key = this->generate_uuid();

        auto maybe_new_write_result = this->_executor->sync_submit(
            reopen_maybe_table.value().write_async(new_key,
                                                   new_value,
                                                   new_reservation.value(),
                                                   this->_executor));

        ASSERT_TRUE(maybe_new_write_result.has_value()) << "An error occurred while writing to the reopened table: " << maybe_new_write_result.error().to_string();

        // test readback both values from the reopened table
        auto read_result = this->_executor->sync_submit(
            reopen_maybe_table.value().read_async(maybe_write_result.value().offset, maybe_write_result.value().size, this->_executor));
        ASSERT_TRUE(read_result.has_value()) << "An error occurred while reading from the reopened table: " << read_result.error().to_string();
        check_read_result(read_result.value(), key, value);

        auto new_read_result = this->_executor->sync_submit(
            reopen_maybe_table.value().read_async(maybe_new_write_result.value().offset, maybe_new_write_result.value().size, this->_executor));

        ASSERT_TRUE(new_read_result.has_value()) << "An error occurred while reading from the reopened table: " << new_read_result.error().to_string();
        check_read_result(new_read_result.value(), new_key, new_value);
    }

    TEST_F(value_table_test, test_delete)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 23);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 23);

        size_t payload_size = 1024;

        std::vector<std::vector<uint8_t>> values;
        std::vector<value_table::write_reservation> reservations;
        std::vector<key_t> keys;
        std::vector<hedgehog::value_ptr_t> write_results;

        auto constexpr N_ITEMS = 10;

        for(auto i = 0UL; i < N_ITEMS; ++i)
        {
            auto value = this->make_random_vec(payload_size);
            auto reservation = table.get_write_reservation(value.size());

            ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

            auto key = this->generate_uuid();

            auto maybe_write_result = this->_executor->sync_submit(
                table.write_async(key,
                                  value,
                                  reservation.value(),
                                  this->_executor));

            ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

            values.push_back(std::move(value));
            reservations.push_back(reservation.value());
            keys.push_back(key);
            write_results.push_back(maybe_write_result.value());

            // check infos
            auto expected_info = value_table_info{
                .current_offset = write_results.back().offset + write_results.back().size,
                .items_count = i + 1,
                .occupied_space = (i + 1) * (payload_size + sizeof(file_header)),
                .deleted_count = 0,
                .freed_space = 0};
            auto info = table.info();
            EXPECT_EQ(info, expected_info);
        }

        // delete entry 3 and 7
        auto status = this->_executor->sync_submit(table.delete_async(keys[3], write_results[3].offset, this->_executor));
        ASSERT_TRUE(status) << "An error occurred on deletion: " << status.error().to_string();

        status = this->_executor->sync_submit(table.delete_async(keys[7], write_results[7].offset, this->_executor));
        ASSERT_TRUE(status) << "An error occurred on deletion: " << status.error().to_string();

        // test readback
        for(size_t i = 0; i < values.size(); ++i)
        {
            auto read_result = this->_executor->sync_submit(
                table.read_async(write_results[i].offset, write_results[i].size, this->_executor));

            if(i == 3 || i == 7)
            {
                ASSERT_FALSE(read_result);
                ASSERT_EQ(read_result.error().code(), errc::DELETED);
                continue;
            }

            ASSERT_TRUE(read_result.has_value()) << "An error occurred while reading from the table: " << read_result.error().to_string();

            check_read_result(read_result.value(), keys[i], values[i]);
        }
    }

    TEST_F(value_table_test, test_iterate_after_delete)
    {
        auto maybe_table = value_table::make_new(this->_base_path, 23);

        ASSERT_TRUE(maybe_table.has_value()) << "An error occurred while creating the table: " << maybe_table.error().to_string();

        auto table = std::move(maybe_table.value());

        ASSERT_EQ(table.id(), 23);

        size_t payload_size = 1024;

        std::vector<std::vector<uint8_t>> values;
        std::vector<value_table::write_reservation> reservations;
        std::vector<key_t> keys;
        std::vector<hedgehog::value_ptr_t> write_results;

        auto constexpr N_ITEMS = 10;

        for(auto i = 0UL; i < N_ITEMS; ++i)
        {
            auto value = this->make_random_vec(payload_size);
            auto reservation = table.get_write_reservation(value.size());

            ASSERT_TRUE(reservation.has_value()) << "An error occurred while reserving space for writing: " << reservation.error().to_string();

            auto key = this->generate_uuid();

            auto maybe_write_result = this->_executor->sync_submit(
                table.write_async(key,
                                  value,
                                  reservation.value(),
                                  this->_executor));

            ASSERT_TRUE(maybe_write_result.has_value()) << "An error occurred while writing to the table: " << maybe_write_result.error().to_string();

            values.push_back(std::move(value));
            reservations.push_back(reservation.value());
            keys.push_back(key);
            write_results.push_back(maybe_write_result.value());

            // check infos
            auto expected_info = value_table_info{
                .current_offset = write_results.back().offset + write_results.back().size,
                .items_count = i + 1,
                .occupied_space = (i + 1) * (payload_size + sizeof(file_header)),
                .deleted_count = 0,
                .freed_space = 0};
            auto info = table.info();
            EXPECT_EQ(info, expected_info);
        }

        // delete entry 3 and 7
        auto status = this->_executor->sync_submit(table.delete_async(keys[3], write_results[3].offset, this->_executor));
        ASSERT_TRUE(status) << "An error occurred on deletion: " << status.error().to_string();

        // check infos after first deletion
        auto info = table.info();
        EXPECT_EQ(info.deleted_count, 1);
        EXPECT_EQ(info.freed_space, write_results[3].size);

        status = this->_executor->sync_submit(table.delete_async(keys[7], write_results[7].offset, this->_executor));
        ASSERT_TRUE(status) << "An error occurred on deletion: " << status.error().to_string();

        // check infos after second deletion
        info = table.info();
        EXPECT_EQ(info.deleted_count, 2);
        EXPECT_EQ(info.freed_space, write_results[3].size + write_results[7].size);

        auto count = 0;

        size_t offset = write_results.front().offset;
        size_t size = write_results.front().size;

        std::vector<std::vector<uint8_t>> readback_values;

        // test readback
        while(true)
        {
            auto maybe_read_result = this->_executor->sync_submit(
                table.read_file_and_next_header_async(offset, size, this->_executor));

            ASSERT_TRUE(maybe_read_result.has_value()) << "An error occurred while reading from the table: " << maybe_read_result.error().to_string() << " at iteration " << count;

            auto& read_result = maybe_read_result.value();

            readback_values.push_back(std::move(read_result.first.binaries));

            std::tie(offset, size) = read_result.second;

            count++;

            if(offset == std::numeric_limits<size_t>::max() && size == 0)
                break;
        }

        EXPECT_EQ(count, N_ITEMS - 2) << "Expected to read " << (N_ITEMS - 2) << " items, but got " << count;

        // check that deleted entries are not in the read values
        values.erase(values.begin() + 7);
        values.erase(values.begin() + 3);

        EXPECT_EQ(values, readback_values) << "Read values do not match expected values after deletion";
    }

} // namespace hedgehog::db