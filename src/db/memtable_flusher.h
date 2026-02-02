#include <mutex>
#include <vector>

#include "skiplist.h"
#include "spinlock.h"
#include "types.h"

namespace hedge::db
{
    struct memtable_flush_group
    {
        memtable_flush_group() = default;
        async::rw_spinlock flush_lock;
        std::vector<skiplist<key_t, value_ptr_t>*> memtables;
        std::atomic_size_t size{0};
        std::once_flag once;
    };
} // namespace hedge::db