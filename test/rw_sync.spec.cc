#include <memory>

#include <gtest/gtest.h>
#include <rw_sync.h>

namespace hedge::async
{

    TEST(FastRefCountTest, SimpleTest)
    {
        using obj_t = rw_sync<size_t>;

        std::shared_ptr<obj_t> shared_obj = std::make_shared<obj_t>(1, 42);

        auto writer = shared_obj->acquire_writer(0);

        ASSERT_TRUE(writer);

        *writer = 42;

        writer.~acquired_writer(); // call dtor

        ASSERT_FALSE(writer);

        // After sealing, writers can no longer acquire
        shared_obj->freeze_writes();

        bool any_writer = shared_obj->any_active_writer();

        ASSERT_FALSE(any_writer);

        auto new_writer = shared_obj->acquire_writer(0);
        ASSERT_FALSE(new_writer);
    }

    TEST(FastRefCountTest, FluentInterface)
    {
        struct foo
        {
            size_t v;

            // NOLINTNEXTLINE
            int bar() { return v; }
        };

        using obj_t = rw_sync<foo>;

        std::shared_ptr<obj_t> shared_obj = std::make_shared<obj_t>(1, 42);

        {
            auto writer = shared_obj->acquire_writer(1);
            writer->bar();
        }

        ASSERT_FALSE(shared_obj->any_active_writer());
    }

    TEST(FastRefCountTest, MultiOwners)
    {
        struct foo
        {
            size_t v;

            // NOLINTNEXTLINE
            int bar() { return v; }
        };

        using obj_t = rw_sync<foo>;

        std::shared_ptr<obj_t> shared_obj = std::make_shared<obj_t>(2, 42);

        auto w1 = shared_obj->acquire_writer(0);
        auto w2 = shared_obj->acquire_writer(1);

        w1.release();

        ASSERT_TRUE(shared_obj->any_active_writer());

        w2.release();

        ASSERT_FALSE(shared_obj->any_active_writer());
    }

} // namespace hedge::async
