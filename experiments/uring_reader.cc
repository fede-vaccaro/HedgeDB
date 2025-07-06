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
#include <sys/types.h>
#include <variant>
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
const size_t FILE_SIZE = 4096; // 4KB
const int QUEUE_DEPTH = 256;
const std::string INDEX_PATH = "/tmp/hh/index.bin";
const std::string VALUE_PATH = "/tmp/input_file";
const unsigned long long NUM_RANDOM_READS = 1000000;

using namespace std::string_literals;

std::pair<int, size_t> open_fd(const std::string& path, bool direct = true)
{
    auto flag = O_RDONLY;
    if(direct)
        flag |= O_DIRECT;

    int fd = open(path.c_str(), flag);
    if (fd < 0) {
        throw std::runtime_error("couldnt open"s + path);
    }

    auto size = std::filesystem::file_size(path);
    return {fd, size};
}

struct index_page_request {
    int fd;
    size_t offset;
    uuids::uuid uuid;
    static inline constexpr size_t SIZE = 4096;
    std::function<void(std::vector<uint8_t>&&)> callback;    
};

struct file_reader_request {
    int fd;
    size_t offset;
    size_t size;
    std::function<void(std::vector<uint8_t>&&)> callback;
};

using _request_t = std::variant<index_page_request, file_reader_request>;

using request_buffer_t = std::deque<std::pair<size_t, _request_t>>;

struct request_t
{
    uuids::uuid uuid;
    std::function<void(std::vector<uint8_t>&&)> callback;
};

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

static std::mt19937 gen(std::random_device{}());

class uring_reader
{
    std::atomic_bool _running{true};


    std::thread _worker;
    struct io_uring _ring;

    static constexpr size_t MAX_BUFFERED_REQUESTS = 4096;
    std::condition_variable _cv;
    std::mutex _pending_requests_mutex;
    request_buffer_t _pending_requests_buffer{}; 

    std::pair<int, size_t> _index_fd = open_fd(INDEX_PATH);
    std::pair<int, size_t> _value_fd = open_fd(VALUE_PATH, false);

    size_t _current_request_id{0};
    std::vector<void*> _index_page_buffers = std::vector<void*>(QUEUE_DEPTH);

    size_t _current_value_buffer_request_id{0};
    std::vector<void*> _value_buffers = std::vector<void*>(QUEUE_DEPTH);

    static void _process_queue(request_buffer_t&& requests_, io_uring& ring, std::vector<void*>& index_buffers, std::vector<void*>& value_buffers)
    {
        auto requests = std::move(requests_);

        std::vector<std::pair<size_t, _request_t>> in_flight_requests;
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
    
                // todo: just allocate memory instead of using the buffer

                size_t ref_buffer = in_flight_requests.size();
    
                // std::cout << "ref_buffer: " << ref_buffer << "\n";

                if(std::holds_alternative<index_page_request>(req))
                {
                    auto& index_req = std::get<index_page_request>(req);
                    io_uring_prep_read(sqe, index_req.fd, index_buffers[ref_buffer], index_page_request::SIZE, index_req.offset);
                }
                else 
                {
                    auto& index_req = std::get<file_reader_request>(req);
                    io_uring_prep_read(sqe, index_req.fd, value_buffers[ref_buffer], index_req.size, index_req.offset);
                }
    
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
                    // std::cout << "current request: " << in_flight_requests[cqe->user_data].second << std::endl;
                    throw std::runtime_error("Read error: "s + strerror(-cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
                } else if (cqe->res != PAGE_SIZE && cqe->res != FILE_SIZE) {
                    throw std::runtime_error("Wrong read: expected " + std::to_string(PAGE_SIZE) + ", got " + std::to_string(cqe->res) + " (user_data: " + std::to_string(cqe->user_data) + ")");
                }
    
                if(cqe->user_data >= in_flight_requests.size())
                    throw std::runtime_error("Invalid user_data: " + std::to_string(cqe->user_data) + ", exceeds in_flight_requests size: " + std::to_string(in_flight_requests.size()));
    
                auto& req = in_flight_requests[cqe->user_data];
                
                if(std::holds_alternative<index_page_request>(req.second))
                {
                    auto& index_req = std::get<index_page_request>(req.second);
                    _handle_index_request(index_req, requests, index_buffers[cqe->user_data]);
                }
                else 
                {
                    auto& file_req = std::get<file_reader_request>(req.second);
                    _handle_file_request(file_req, value_buffers[cqe->user_data]);
                }
    
                cqe_count++;
            }
    
            io_uring_cq_advance(&ring, cqe_count);
        }
    }   

    static void _handle_index_request(index_page_request& req, request_buffer_t& buffer, void* ptr)
    {
        auto* page_ptr = static_cast<index_key_t*>(ptr);
        auto page_end_ptr = page_ptr + (PAGE_SIZE / sizeof(index_key_t));

        auto value_ptr = _find_in_page(req.uuid, page_ptr, page_end_ptr);

        if(!value_ptr)
            throw std::runtime_error("UUID not found in index page: " + uuids::to_string(req.uuid));

        static constexpr size_t VALUE_SIZE_BYTES = 20971520000; // hardcoded
        static constexpr size_t VALUE_SIZE_PAGES = VALUE_SIZE_BYTES / PAGE_SIZE;

        thread_local std::uniform_int_distribution<uint64_t> dist(0, VALUE_SIZE_PAGES - 200);

        file_reader_request file_req{
            .fd = req.fd,
            .offset = dist(gen) * PAGE_SIZE,
            .size = FILE_SIZE, // 16KB
            .callback = std::move(req.callback)
        };

        buffer.push_front({0, std::move(file_req)}); // push to front to ensure it is processed next
    }

    static void _handle_file_request(file_reader_request& req, void* ptr)
    {
        auto* value_ptr = static_cast<uint8_t*>(ptr);

        if(!req.callback)
            return;

        std::vector<uint8_t> value_data(value_ptr, value_ptr + req.size);
        req.callback(std::move(value_data));        
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

            uring_reader::_process_queue(std::move(requests), this->_ring, this->_index_page_buffers, this->_value_buffers);    
        }
    }

public:
    uring_reader()
    {
        int ret = io_uring_queue_init(QUEUE_DEPTH, &this->_ring, IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER);
        
        if(ret < 0)
            throw std::runtime_error("error with io_uring_queue_init: "s +  strerror(-ret));

        this->_index_page_buffers.resize(QUEUE_DEPTH);

        for (int i = 0; i < QUEUE_DEPTH; ++i) 
        {
            if (posix_memalign(&this->_index_page_buffers[i], PAGE_SIZE, PAGE_SIZE) != 0) 
            {
                perror("posix_memalign failed");
                for (int j = 0; j < i; ++j) 
                    free(this->_index_page_buffers[j]);
                throw std::runtime_error("Failed to allocate aligned memory for buffers");
            }
        }

        this->_value_buffers.resize(QUEUE_DEPTH);

        for (int i = 0; i < QUEUE_DEPTH; ++i) 
        {
            if (posix_memalign(&this->_value_buffers[i], PAGE_SIZE, PAGE_SIZE*32) != 0) 
            {
                perror("posix_memalign failed");
                for (int j = 0; j < i; ++j) 
                    free(this->_value_buffers[j]);
                throw std::runtime_error("Failed to allocate aligned memory for buffers");
            }
        }
        

        this->_worker = std::thread(&uring_reader::_run, this);
    }

    ~uring_reader()
    {
        this->close_reader();

        io_uring_queue_exit(&this->_ring);

        for (auto& ptr: this->_index_page_buffers)
            free(ptr);
    }

    void submit_request(request_t req)
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

            thread_local std::uniform_int_distribution<uint64_t> dist(0, this->_index_fd.second / PAGE_SIZE - 4);

            index_page_request request{
                .fd = this->_index_fd.first,
                .offset = dist(gen)*PAGE_SIZE,
                .uuid = req.uuid,
                .callback = std::move(req.callback)
            };

            this->_pending_requests_buffer.emplace_back(this->_current_request_id, std::move(request));
            this->_current_request_id++;
        }

        this->_cv.notify_all();
    };

    void close_reader()
    {
        std::cout << "Closing uring_reader." << std::endl;

        this->_running.store(false, std::memory_order_relaxed);

        this->_cv.notify_all();

        if (_worker.joinable())
            _worker.join();

        close(this->_index_fd.first);
        close(this->_value_fd.first);
    }

};


int main() {
    // The file creation logic has been removed as per your request.
    // Ensure that /tmp/huge_file.bin exists and is accessible.
    

    auto uuids = read_uuids_from_file("uuids_sequential.bin");

    std::uniform_int_distribution<uint32_t> uuids_distribution(0, uuids.size() - 1);

    auto n_threads = 4;
    std::vector<uring_reader> readers(n_threads);

    wg wg;
    wg.set(NUM_RANDOM_READS);


    auto start_time = std::chrono::high_resolution_clock::now();

    for(size_t i = 0; i < NUM_RANDOM_READS; ++i) {
        auto rand_uuid = uuids[uuids_distribution(gen)];

        auto req = request_t{
            .uuid = rand_uuid,
            .callback = [&wg, i](std::vector<uint8_t>&& vec) {
                // std::cout << "Request " << i << " received data of size: " << vec.size() << std::endl;
                wg.decr();
            }
        };   

        readers[i % readers.size()].submit_request(std::move(req));
    }

    wg.wait();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\nTotal requests submitted: " << NUM_RANDOM_READS << std::endl;
    std::cout << "Average requests per second: " 
              << (NUM_RANDOM_READS * 1000.0 / duration.count()) << std::endl;
    std::cout << "Average (in batch) request time: " << (duration.count() * 1000.0 / NUM_RANDOM_READS) << " us" << std::endl; 
    std::cout << "Read bandwidth: " 
              << (NUM_RANDOM_READS * FILE_SIZE / 1024.0 / 1024.0 / (duration.count() / 1000.0)) << " MB/s" << std::endl; // Assuming each read is 4KB
    std::cout << "Time taken: " << duration.count() << " ms" << std::endl;

    return 0;
}
