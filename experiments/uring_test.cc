#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <liburing.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <chrono>
#include <string>
#include <random>
#include <ctime>
#include <functional>
#include <thread>

const size_t PAGE_SIZE = 4096;
const int QUEUE_DEPTH = 32;
const std::string FILE_PATH = "/tmp/testfile";
const unsigned long long NUM_RANDOM_READS = 10000;


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

    std::mt19937 generator(static_cast<unsigned int>(time(0)));
    std::uniform_int_distribution<unsigned long long> distribution(0, total_num_pages - 15);

    struct io_uring ring;
    int ret = io_uring_queue_init(QUEUE_DEPTH, &ring, IORING_SETUP_SQPOLL);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init: %s\n", strerror(-ret));
        close(fd);
        return 1;
    }

    std::vector<void*> buffers(QUEUE_DEPTH);
    for (int i = 0; i < QUEUE_DEPTH; ++i) {
        if (posix_memalign(&buffers[i], PAGE_SIZE, PAGE_SIZE) != 0) {
            perror("posix_memalign failed");
            for (int j = 0; j < i; ++j) {
                free(buffers[j]);
            }
            io_uring_queue_exit(&ring);
            close(fd);
            return 1;
        }
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    unsigned long long total_requests = 0;
    unsigned long long completed_requests = 0;

    while (completed_requests < NUM_RANDOM_READS) {
        while (total_requests < NUM_RANDOM_READS &&
               total_requests - completed_requests < QUEUE_DEPTH)
        {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                break;
            }

            off_t random_page_index = distribution(generator);
            off_t random_offset = random_page_index * PAGE_SIZE;

            io_uring_prep_read(sqe, fd, buffers[total_requests % QUEUE_DEPTH], PAGE_SIZE, random_offset);
            sqe->user_data = total_requests % QUEUE_DEPTH;

            total_requests++;
        }

        if (io_uring_sq_ready(&ring) > 0) {
            ret = io_uring_submit(&ring);
            if (ret < 0) {
                fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
                break;
            }
        }

        if (total_requests > completed_requests) {
            struct io_uring_cqe *cqe;
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
                break;
            }

            unsigned head;
            unsigned cqe_count = 0;
            io_uring_for_each_cqe(&ring, head, cqe) {
                if (cqe->res < 0) {
                    fprintf(stderr, "Read error: %s (user_data: %llu)\n", strerror(-cqe->res), (unsigned long long)cqe->user_data);
                } else if (cqe->res != PAGE_SIZE) {
                    fprintf(stderr, "Short read: expected %zu, got %d (user_data: %llu)\n", PAGE_SIZE, cqe->res, (unsigned long long)cqe->user_data);
                }
                completed_requests++;
                cqe_count++;
            }
            io_uring_cq_advance(&ring, cqe_count);
        }
        if (ret < 0) break;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\nTotal requests submitted: " << total_requests << std::endl;
    std::cout << "Total requests completed: " << completed_requests << std::endl;
    std::cout << "Average requests per second: " 
              << (completed_requests * 1000.0 / duration.count()) << std::endl;
    std::cout << "Time taken: " << duration.count() << " ms" << std::endl;

    for (int i = 0; i < QUEUE_DEPTH; ++i) {
        free(buffers[i]);
    }
    io_uring_queue_exit(&ring);
    close(fd);

    // Removed the dummy file removal logic as well.

    return 0;
}
