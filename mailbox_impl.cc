#include <cstdint>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stdlib.h>

#include <logger.h>

#include "mailbox_impl.h"

namespace hedgehog::async
{

    constexpr size_t PAGE_SIZE = 4096;

    std::unique_ptr<uint8_t> aligned_alloc(size_t size)
    {
        uint8_t* ptr = nullptr;
        if(posix_memalign((void**)&ptr, PAGE_SIZE, size) != 0) // todo: preallocate some memory for 4 KB pages
        {
            perror("posix_memalign failed");
            throw std::runtime_error("Failed to allocate aligned memory for buffers");
        }

        return std::unique_ptr<uint8_t>(ptr);
    }

    void read_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        this->response = {
            .data = aligned_alloc(request.size),
            .bytes_read = 0};

        io_uring_prep_read(sqe, this->request.fd, this->response.data.get(), this->request.size, this->request.offset);
    }

    bool read_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;

        return true;
    }

    void write_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        this->response = {};

        io_uring_prep_write(sqe, this->request.fd, this->request.data.data(), this->request.data.size(), this->request.offset);
    }

    bool write_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            response.error_code = cqe->res;

        this->response.bytes_written = cqe->res;

        if(cqe->res != this->request.data.size())
            log("Wrong read: expected ", this->request.data.size(), ", got ", cqe->res, " (user_data: ", cqe->user_data, ")");

        return true;
    }

    void multi_read_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto it = sqes.begin();

        for(const auto& req : this->request.requests)
        {
            io_uring_sqe* sqe = *it++;

            read_response response{
                .data = aligned_alloc(req.size),
                .bytes_read = req.size,
            };

            io_uring_prep_read(sqe, req.fd, response.data.get(), req.size, req.offset);

            this->response.responses.emplace_back(std::move(response));
        }
    }

    bool multi_read_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t sub_request_idx)
    {
        auto& response = this->response.responses[sub_request_idx];

        if(cqe->res < 0)
            response.error_code = cqe->res;
        else
            response.bytes_read = cqe->res;

        return ++this->_landed_response == this->request.requests.size();
    }

} // namespace hedgehog::async