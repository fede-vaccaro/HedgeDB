#include <algorithm>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <set>
#include <stdexcept>
#include <vector>

#include "generator.h"

#ifdef __linux__
#include <fstream>
#include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#endif

/**
 * Utility to get current Resident Set Size (RSS) in bytes.
 */
size_t get_rss_bytes()
{
#ifdef __linux__
    long rss = 0L;
    std::ifstream stat_stream("/proc/self/statm", std::ios_base::in);
    if(stat_stream.is_open())
    {
        stat_stream >> rss; // first is size, second is resident
        stat_stream >> rss;
    }
    return (size_t)rss * (size_t)sysconf(_SC_PAGESIZE);
#elif defined(__APPLE__)
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if(task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) != KERN_SUCCESS)
        return 0UL;
    return (size_t)info.resident_size;
#else
    return 0UL; // Unsupported platform
#endif
}

struct node
{
    constexpr static size_t EMPTY_IDX = std::numeric_limits<uint32_t>::max();

    int data;
    uint32_t next[1]; // trick for C++ variable length arrays

    explicit node(size_t height)
    {
        for(auto* it = next; it < next + height + 1; ++it)
            *it = EMPTY_IDX;
    }

    std::string to_string(size_t k)
    {
        std::string s = "node(data=" + std::to_string(this->data) + ", succ=[";
        for(size_t i = 0; i < k + 1; ++i)
        {
            s += std::to_string(this->next[i]);
            s += ", ";
        }
        s += "])";
        return s;
    }
};

struct const_node_view
{
    constexpr static size_t EMPTY_IDX = std::numeric_limits<uint32_t>::max();

    const uint32_t* view{nullptr};
    uint32_t idx{0};

    // int data{};

    const_node_view() = default;

    explicit const_node_view(const uint32_t* v, uint32_t idx) : view(v), idx(idx) {}

    [[nodiscard]] const int& data() const
    {
        return *reinterpret_cast<const int*>(view);
    }

    [[nodiscard]] const uint32_t* successors() const
    {
        return view + 1;
    }

    [[nodiscard]] std::string to_string(size_t k) const
    {
        std::string s = "node(idx=" + std::to_string(this->idx) + ", data=" + std::to_string(this->data()) + ", succ=[";
        for(size_t i = 0; i < k + 1; ++i)
        {
            s += std::to_string(this->successors()[i]);
            if(i < k - 1)
                s += ", ";
        }
        s += "])";
        return s;
    }

};

size_t sizeof_node(size_t target_level)
{
    // e.g. max_levels = 12
    // target_level == 0 -> allocate 1 * 4 bytes per level
    // target_level == 1 -> allocate 2 * 4 bytes per level
    // target_level == 11 -> allocate 12 * 4 bytes per level
    // target_levle == 12 OUT OF BOUNDS!

    size_t v = ((target_level + 1) * sizeof(uint32_t)) + sizeof(uint32_t);

    // std::cout << "allocating memory for levels " << levels << ": " << v << std::endl;

    return v;
}

class arena
{
    size_t _head{0};
    size_t _capacity{0};
    std::unique_ptr<uint8_t[], decltype(&std::free)> _data = std::unique_ptr<uint8_t[], decltype(&std::free)>(nullptr, nullptr);

public:
    ~arena()
    {
        std::cout << "Arena allocated bytes: " << this->_capacity / 1024 << " KB\n";
        std::cout << "Arena used bytes: " << this->_head / 1024 << " KB\n";
    }

    explicit arena(size_t size_bytes) : _capacity(size_bytes)
    {
        this->_data = std::unique_ptr<uint8_t[], decltype(&std::free)>((uint8_t*)std::aligned_alloc(8, size_bytes), &std::free);

        std::fill_n(this->_data.get(), this->_capacity, 0);
    }
    [[nodiscard]] void* at(size_t idx)
    {
        return this->_data.get() + idx;
    }

    [[nodiscard]] const void* at(size_t idx) const
    {
        return this->_data.get() + idx;
    }

    size_t allocate(size_t bytes)
    {
        size_t cur = this->_head;

        if(this->_head + bytes > _capacity)
            throw std::runtime_error("arena out of mem");

        this->_head += bytes;

        return cur;
    }
};

struct skiplist
{
public:
    constexpr static size_t LIST_CAPACITY = 200'000;

private:
    constexpr static double P = 0.33;
    constexpr static size_t EXPECTED_POINTERS = LIST_CAPACITY * (1.0 + P) * 1.5; // add some margin for the pointers
    const static size_t HARD_LEVEL_CAP = static_cast<size_t>(std::log2(LIST_CAPACITY) + 1);

    // arena _data_ptr_pool = arena(LIST_CAPACITY * sizeof(int));
    arena _node_ptr_pool = arena((EXPECTED_POINTERS + LIST_CAPACITY + 1 + HARD_LEVEL_CAP) * sizeof(uint32_t));

    node* _head;
    uint32_t _max_levels;
    size_t _size{0};

    std::vector<uint8_t> _precomputed_random_levels;

    node* _node_ptr(uint32_t node_idx)
    {
        return static_cast<node*>(this->_node_ptr_pool.at(node_idx));
    }

    [[nodiscard]] const node* _node_ptr(uint32_t node_idx) const
    {
        return static_cast<const node*>(this->_node_ptr_pool.at(node_idx));
    }

    // int* _data_ptr(uint32_t node_idx)
    // {
    //     return reinterpret_cast<int*>(this->_data_ptr_pool.read_at(this->_node_ptr(node_idx)->data));
    // }

public:
    explicit skiplist()
    {
        this->_max_levels = 12;

        auto head_idx = this->_node_ptr_pool.allocate(sizeof_node(this->_max_levels - 1));

        void* mem = this->_node_ptr_pool.at(head_idx);

        ::new(mem) node(this->_max_levels);

        this->_head = (node*)mem;

        this->_precomputed_random_levels.resize(LIST_CAPACITY);

        thread_local std::random_device rd{};
        thread_local std::mt19937 gen{rd()};
        thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);

        for(size_t i = 0; i < LIST_CAPACITY; ++i)
        {
            this->_precomputed_random_levels[i] = 0;

            uint8_t count{0};

            while(dist(gen) < P)
                ++count;

            this->_precomputed_random_levels[i] = std::min(count, (uint8_t)(this->_max_levels - 1));
        }
    }

    void insert(int data)
    {
        // std::cout << "inserting " << data << "\n";

        size_t dest_level = skiplist::_random_level();

        // if(dest_level >= 1)
        // dest_level--;

        // build the new node
        auto new_node_idx = this->_node_ptr_pool.allocate(sizeof_node(dest_level));

        auto* new_node = static_cast<node*>(this->_node_ptr_pool.at(new_node_idx));

        ::new(new_node) node(dest_level);

        new_node->data = data;

        // bind data to new node & copy data to the pool

        // auto* head_successors = this->_head.successors();

        // set-up the heads
        // for(; k > -1; --k)
        // {
        // if(*(head_successors + k) == node_view::EMPTY_IDX)
        // *(head_successors + k) = new_node_idx;
        // else
        // break;
        // }

        // std::cout << "Inserting data=" << x.to_string() << " at level=" << dest_level << "\n";

        int k = this->_max_levels - 1;

        node* x{this->_head}; // X is the current node
        node* z{};            // Z is the next

        // Starts from max level
        // Position search and insertion happen at the same time
        thread_local std::vector<std::pair<node*, uint32_t>> updates;
        updates.clear();
        updates.resize(dest_level + 1);

        for(; k > -1; --k)
        {
            uint32_t z_idx = x->next[k];
            z = this->_node_ptr(z_idx);

            // Iterate and find the position at the current level
            while(z_idx != node::EMPTY_IDX && data >= z->data)
            {
                // std::cout << "Traversing at level " << k << ": " << z.to_string(k) << "\n";
                x = z;
                z_idx = z->next[k];

                [[maybe_unused]] auto check_z_idx_is_valued = [&]()
                {
                    // std::cout << "next_z_idx: " << z_idx << " z: " << z->to_string(dest_level) << std::endl;
                    // std::cout << "x: " << x->to_string(dest_level) << std::endl;

                    return z_idx != 0;
                };

                // assert(check_z_idx_is_valued());
                z = this->_node_ptr(z_idx);
            }

            // When the position is found
            if(k <= static_cast<int>(dest_level))
                updates[k] = std::pair<node*, uint32_t>{x, z_idx};
        }

        for(int k = 0; k < (int)updates.size(); ++k)
        {
            auto [x, z_idx] = updates[k];
            new_node->next[k] = z_idx;
            x->next[k] = new_node_idx;
        }

        ++this->_size;
    }

    [[nodiscard]] std::optional<int> find(int data) const
    {

        const node* x{this->_head}; // X is the current node
        const node* z{};            // Z is the next

        for(int k = this->_max_levels - 1; k > -1; --k)
        {
            uint32_t z_idx = x->next[k];
            z = this->_node_ptr(z_idx);

            while(z_idx != node::EMPTY_IDX && !(data < z->data))
            {
                if(z->data == data)
                    return data;

                x = z;
                z_idx = z->next[k];
                z = this->_node_ptr(z_idx);

                // assert(z_idx != 0);
            }
            // std::cout << "Level " << k << " hops: " << level_hops << "\n";
        }

        return std::nullopt;
    }

    [[nodiscard]] size_t levels() const
    {
        return this->_max_levels;
    }

    hedge::async::generator<int> reader(size_t level = 0)
    {
        uint32_t curr_node_idx = this->_head->next[level];
        const node* curr = this->_node_ptr(curr_node_idx);

        while(curr_node_idx != node::EMPTY_IDX)
        {
            int data = curr->data;
            co_yield data;

            curr_node_idx = curr->next[level];
            curr = this->_node_ptr(curr_node_idx);
        }

        co_return;
    }

private:
    size_t _random_level()
    {
        return this->_precomputed_random_levels[this->_size];
    }
};

int main()
{
    skiplist skl{};

    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<int> dist(0, 10000000);

    std::vector<int> values;
    values.reserve(skiplist::LIST_CAPACITY);

    for(size_t i = 0; i < skiplist::LIST_CAPACITY; ++i)
    {
        int val = dist(rd);
        values.push_back(val);
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < skiplist::LIST_CAPACITY; ++i)
        skl.insert(values[i]);
    auto t1 = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::micro> insert_duration = t1 - t0;
    std::cout << "Inserted " << skiplist::LIST_CAPACITY << " items in "
              << insert_duration.count() << " us\n";
    std::cout << "Average insertion time: "
              << (insert_duration.count() / skiplist::LIST_CAPACITY) << " us\n";
    std::cout << "Average insertion throughput: "
              << (skiplist::LIST_CAPACITY / insert_duration.count()) << " million ops/s\n";

    // print level0
    // for(auto i : skl.reader(0))
    // {
    //     std::cout << i << " ";
    // }
    // std::cout << "\n";

    std::vector<int> readback;
    readback.reserve(skiplist::LIST_CAPACITY);

    t0 = std::chrono::high_resolution_clock::now();
    {
        for(auto v : values)
        {
            auto found = skl.find(v);
            if(found)
                readback.push_back(found.value());
        }
    }
    t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> find_duration = t1 - t0;
    std::cout << "Found " << skiplist::LIST_CAPACITY << " items in "
              << find_duration.count() << " us\n";
    std::cout << "Average find time: "
              << (find_duration.count() / skiplist::LIST_CAPACITY) << " us\n";
    std::cout << "Average find throughput: "
              << (skiplist::LIST_CAPACITY / find_duration.count()) << " million ops/s\n";

    std::ranges::sort(readback);
    std::ranges::sort(values);

    if(values != readback)
    {
        std::cout << "values and readback does not match\n";
        return 0;
    }

    std::cout << "values and readback matches!\n";

    std::cout << "number of levels: " << skl.levels() << "\n";

    auto count_at_level = [&skl](size_t l) -> size_t
    {
        size_t count{0};

        for([[maybe_unused]] auto _ : skl.reader(l))
            count++;

        // std::cout << "items at level " << l << ": " << count << "\n";
        return count;
    };

    size_t total_count = 0;
    for(int k = skl.levels() - 1; k > -1; --k)
    {
        auto c = count_at_level(k);
        total_count += c;
        std::cout << "items at level " << k << ": " << c << "\n";
    }
    std::cout << "total pointers: " << total_count << "\n";

    // benchmark set
    std::set<int> benchmark_set;
    // std::mutex m;
    t0 = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < skiplist::LIST_CAPACITY; ++i)
    {
        // std::lock_guard<std::mutex> lock(m);
        benchmark_set.insert(values[i]);
    }

    t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> set_insert_duration = t1 - t0;
    std::cout << "std::set inserted " << skiplist::LIST_CAPACITY << " items in "
              << set_insert_duration.count() << " us\n";
    std::cout << "Average std::set insertion time: "
              << (set_insert_duration.count() / skiplist::LIST_CAPACITY) << " us\n";

    std::vector<int> set_readback;
    set_readback.reserve(skiplist::LIST_CAPACITY);

    t0 = std::chrono::high_resolution_clock::now();
    {
        for(auto v : values)
        {
            auto found = benchmark_set.find(v);
            if(found != benchmark_set.end())
                //;
                set_readback.push_back(*found);
        }
    }
    t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> set_find_duration = t1 - t0;
    std::cout << "std::set found " << skiplist::LIST_CAPACITY << " items in "
              << set_find_duration.count() << " us\n";
    std::cout << "Average std::set find time: "
              << (set_find_duration.count() / skiplist::LIST_CAPACITY) << " us\n";

    size_t rss = get_rss_bytes();
    std::cout << "\n"
              << std::left << std::setw(25) << "Resident Set Size (RSS):"
              << (double)rss / (1024 * 1024) << " MB" << std::endl;

    return 0;
}