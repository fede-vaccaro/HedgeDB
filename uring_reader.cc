#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <iostream>
#include <ratio>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <liburing.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <chrono>
#include <string>
#include <random>
#include <algorithm>
#include <ctime>
#include <functional>
#include <thread>
#include <uuid.h>

const size_t PAGE_SIZE = 4096;
const int QUEUE_DEPTH = 64;
const std::string FILE_PATH = "/tmp/hh/index.bin";
const unsigned long long NUM_RANDOM_READS = 1000000;

using namespace std::string_literals;


struct request;
using request_buffer_t = std::deque<std::pair<size_t, request>>;
struct request {
    int fd;
    size_t offset;
    size_t size;
    
    std::function<void(uint8_t* ptr_start, uint8_t* ptr_end)> callback;    
};


std::ostream& operator<<(std::ostream& ofs, const request& req) 
{
    ofs << "fd: " << req.fd << ", offset: " << req.offset 
        << ", size: " << req.size << ", callback: " 
        << (req.callback ? "set" : "not set") << std::endl;
    return ofs;
}

std::vector<uuids::uuid> read_uuids_from_file(const std::string& file_name)
{
    auto ifs = std::ifstream(file_name, std::ios::binary);
    auto file_size = std::filesystem::file_size(file_name);
    if(file_size % sizeof(uuids::uuid) != 0)
        throw std::runtime_error("invalid file size for input uuids");

    std::vector<uuids::uuid> uuids(file_size / sizeof(uuids::uuid));
    ifs.read(reinterpret_cast<char*>(uuids.data()), sizeof(uuids::uuid)*uuids.size());

    return uuids;
}

struct value_ptr_t
{
    uint8_t dummy[16];
};

struct index_key_t
{
    uuids::uuid key;
    value_ptr_t value_ptr;

    bool operator<(const index_key_t& other) const
    {
        return key < other.key;
    }
};

std::optional<value_ptr_t> _find_in_page(const uuids::uuid& key, const index_key_t* start, const index_key_t* end)
{
    // std::cout << "finding key: " << key << std::endl;

    // for(auto it = start; it != end; it++)
    // {
        // std::cout << "elements " << it->key << std::endl;
    // }

    auto it = std::lower_bound(start, end, index_key_t{key, {}});

    if (it != end && it->key == key)
        return it->value_ptr;

    return std::nullopt;
}



class wg
{
    std::atomic_uint64_t _counter{0}; 
    std::atomic_bool _done {false};

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
        this->_cv.wait(lk, [this](){ return this->_done.load(); });
    }
};

class uring_reader
{
    std::atomic_bool _running{true};

    size_t _current_request_id{0};

    std::thread _worker;
    struct io_uring _ring;

    static constexpr size_t MAX_BUFFERED_REQUESTS = 4096;
    std::condition_variable _cv;
    std::mutex _pending_requests_mutex;
    std::deque<std::pair<size_t, request>> _pending_requests_buffer{}; 

    std::vector<void*> _mem_aligned_buffers = std::vector<void*>(QUEUE_DEPTH);

    static void _process_queue(std::deque<std::pair<size_t, request>>&& requests, io_uring& ring, std::vector<void*>& buffers)
    {
        std::vector<std::pair<size_t, request>> in_flight_requests;
        in_flight_requests.reserve(QUEUE_DEPTH);

        while(!requests.empty())
        {
            in_flight_requests.clear();

            while (!requests.empty()) 
            {
                auto& [req_id, req] = requests.front();
                
                io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    
                if (!sqe)
                    break;
    
                size_t ref_buffer = in_flight_requests.size();
    
                // std::cout << "ref_buffer: " << ref_buffer << "\n";
    
                io_uring_prep_read(sqe, req.fd, buffers[ref_buffer], PAGE_SIZE, req.offset);
    
                sqe->user_data = ref_buffer;
    
                in_flight_requests.emplace_back(req_id, std::move(req));
    
                requests.pop_front();
            }
    
            if (auto ready = io_uring_sq_ready(&ring); ready > 0) 
            {
                // std::cout << "Submitting " << ready << " requests." << std::endl;
    
                auto submit = io_uring_submit(&ring);
    
                if (submit < 0) 
                    throw std::runtime_error("io_uring_submit: "s + strerror(-submit));
    
                if (ready != in_flight_requests.size() || static_cast<size_t>(submit) != in_flight_requests.size()) // todo might remove
                {
                    std::cerr << "Warning: io_uring_submit submitted != requests than expected. Expected: " 
                                << in_flight_requests.size() << ", ready: " << ready << "submit: " << submit << std::endl;
                }
            }
    
            // std::cout << "Submitted " << in_flight_requests.size() << " requests." << std::endl;
    
            struct io_uring_cqe *cqe;
            int wait_nr = static_cast<int>(in_flight_requests.size());

            auto ret = io_uring_wait_cqes(&ring, &cqe, wait_nr, NULL, NULL);
    
            if (ret < 0)
                throw std::runtime_error("io_uring_wait_cqe: "s + strerror(-ret));
    
            unsigned head;
            unsigned cqe_count = 0;
    
            io_uring_for_each_cqe(&ring, head, cqe) 
            {
                if (cqe->res < 0) {
                    std::cout << "current request: " << in_flight_requests[cqe->user_data].second << std::endl;
                    throw std::runtime_error("Read error: "s + strerror(-cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
                } else if (cqe->res != PAGE_SIZE) {
                    throw std::runtime_error("Short read: expected " + std::to_string(PAGE_SIZE) + ", got " + std::to_string(cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
                }
    
                if(cqe->user_data >= in_flight_requests.size())
                    throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", exceeds in_flight_requests size: " + std::to_string(in_flight_requests.size()));
    
                auto& req = in_flight_requests[cqe->user_data];
    
                // std::vector<uint8_t> buffer(PAGE_SIZE);
    
                auto* src = static_cast<uint8_t*>(buffers[cqe->user_data]);
                auto* src_end = src + PAGE_SIZE;
                // std::copy(src, src_end, buffer.begin());
                
                req.second.callback(src, src_end); // todo defer to executor
    
                cqe_count++;
            }
    
            io_uring_cq_advance(&ring, cqe_count);
        }
    }   

    void _run()
    {
        std::cout << "Starting uring_reader worker thread." << std::endl;

        while(true)
        {
            std::unique_lock lk(this->_pending_requests_mutex); // should have priority over submit

            this->_cv.wait(lk, [this](){
                return !this->_pending_requests_buffer.empty() || !this->_running;
            });

            if(!this->_running.load(std::memory_order_relaxed) && this->_pending_requests_buffer.empty())
                break;

            // std::cout << "Pending requests: " << this->_pending_requests_buffer.size() << std::endl;
            
            auto requests = decltype(this->_pending_requests_buffer){};            
            requests.swap(this->_pending_requests_buffer);

            lk.unlock();
            this->_cv.notify_all();

            uring_reader::_process_queue(std::move(requests), this->_ring, this->_mem_aligned_buffers);    
        }
    }

public:
    uring_reader()
    {
        int ret = io_uring_queue_init(QUEUE_DEPTH, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);
        
        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s +  strerror(-ret));

        this->_mem_aligned_buffers.resize(QUEUE_DEPTH);

        for (int i = 0; i < QUEUE_DEPTH; ++i) 
        {
            if (posix_memalign(&this->_mem_aligned_buffers[i], PAGE_SIZE, PAGE_SIZE) != 0) 
            {
                perror("posix_memalign failed");
                for (int j = 0; j < i; ++j) 
                    free(this->_mem_aligned_buffers[j]);
                throw std::runtime_error("Failed to allocate aligned memory for buffers");
            }
        }
        

        this->_worker = std::thread(&uring_reader::_run, this);
    }

    ~uring_reader()
    {
        this->close();

        io_uring_queue_exit(&this->_ring);

        for (auto& ptr: this->_mem_aligned_buffers)
            free(ptr);
    }

    void submit_request(request req)
    {
        if(!this->_running.load(std::memory_order_relaxed))
            return;

        {
            // back pressure here
            std::unique_lock lk(this->_pending_requests_mutex);

            this->_cv.wait(lk, [this]()
            {
                return this->_pending_requests_buffer.size() < MAX_BUFFERED_REQUESTS;
            });

            this->_pending_requests_buffer.emplace_back(this->_current_request_id, std::move(req));
            this->_current_request_id++;
        }

        this->_cv.notify_all();
    };

    void close()
    {
        std::cout << "Closing uring_reader." << std::endl;

        this->_running.store(false, std::memory_order_relaxed);

        this->_cv.notify_all();

        if (_worker.joinable())
            _worker.join();
    }

};

int main() {
    // The file creation logic has been removed as per your request.
    // Ensure that /tmp/huge_file.bin exists and is accessible.

    int fd = open(FILE_PATH.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        perror("Failed to open file");
        return 1;
    }

    struct stat file_stat;
    if (fstat(fd, &file_stat) < 0) {
        perror("Failed to get file stats");
        close(fd);
        return 1;
    }
    off_t file_size = file_stat.st_size;
    std::cout << "File size: " << file_size << " bytes" << std::endl;

    unsigned long long total_num_pages = file_size / PAGE_SIZE;
    
    if (total_num_pages == 0) {
        std::cerr << "File has no pages to read." << std::endl;
        close(fd);
        return 1;
    }

    auto uuids = read_uuids_from_file("uuids_sequential.bin");

    std::mt19937 gen(static_cast<unsigned int>(time(0)));
    std::uniform_int_distribution<unsigned long long> distribution(0, total_num_pages - 15);
    std::uniform_int_distribution<uint32_t> uuids_distribution(0, uuids.size() - 1);

    uring_reader reader;

    wg wg;
    wg.set(NUM_RANDOM_READS);


    auto start_time = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < NUM_RANDOM_READS; ++i) {
        unsigned long long random_page_index = distribution(gen);
        size_t random_offset = random_page_index * PAGE_SIZE;

        auto rand_uuid = uuids[uuids_distribution(gen)];

        auto req = request{
            .fd = fd, 
            .offset = random_offset,
            .size = PAGE_SIZE,
            .callback = [&wg, i, uuid = rand_uuid](uint8_t* ptr_start, uint8_t* ptr_end) {
                auto* ptr_start_ = reinterpret_cast<index_key_t*>(ptr_start);
                auto* ptr_end_ = reinterpret_cast<index_key_t*>(ptr_end);
                // std::cout << "Read " << buffer.size() << " bytes from offset. Request id: " << i << std::endl;
                auto result = _find_in_page(uuid, ptr_start_, ptr_end_);

                if(!result)
                    throw std::runtime_error("result should be always found");

                wg.decr();
            }
        };   

        reader.submit_request(std::move(req));
    }

    wg.wait();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\nTotal requests submitted: " << NUM_RANDOM_READS << std::endl;
    std::cout << "Average requests per second: " 
              << (NUM_RANDOM_READS * 1000.0 / duration.count()) << std::endl;
    std::cout << "Time taken: " << duration.count() << " ms" << std::endl;

    close(fd);

    // Removed the dummy file removal logic as well.

    return 0;
}
