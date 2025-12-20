#include <cstdint>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stdexcept>
#include <stdlib.h>

#include <logger.h>

#include "mailbox_impl.h"

namespace hedge::async
{
    void read_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        io_uring_prep_read(sqe, this->request.fd, this->request.data, this->request.size, this->request.offset);
    }

    bool read_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;

        return true;
    }

    void unaligned_read_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        this->response = {
            .data = std::vector<uint8_t>(request.size),
            .bytes_read = 0};

        io_uring_prep_read(sqe, this->request.fd, this->response.data.data(), this->request.size, this->request.offset);
    }

    bool unaligned_read_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;

        return true;
    }

    void unaligned_readv_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        this->response = {
            .bytes_read = 0,
            .error_code = 0};

        io_uring_prep_readv(sqe, this->request.fd, this->request.iovecs, this->request.iovecs_count, this->request.offset);
    }

    bool unaligned_readv_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
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

        io_uring_prep_write(sqe, this->request.fd, this->request.data, this->request.size, this->request.offset);
    }

    bool write_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            response.error_code = cqe->res;

        this->response.bytes_written = cqe->res;

        return true;
    }

    void writev_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        io_uring_sqe* sqe = sqes.front();

        this->response = {};

        io_uring_prep_writev(sqe, this->request.fd, request.iovecs, this->request.iovecs_count, this->request.offset);
    }

    bool writev_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            response.error_code = cqe->res;

        this->response.bytes_written = cqe->res;

        return true;
    }

    void multi_read_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto it = sqes.begin();

        for(const auto& req : this->request.requests)
        {
            io_uring_sqe* sqe = *it++;

            read_response response{
                .bytes_read = req.size,
            };

            io_uring_prep_read(sqe, req.fd, req.data, req.size, req.offset);

            this->response.responses.emplace_back(response);
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

    void open_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto* sqe = sqes.front();

        io_uring_prep_openat(sqe, AT_FDCWD, this->request.path.c_str(), this->request.flags, this->request.mode);
    }

    bool open_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res >= 0)
            this->response.file_descriptor = cqe->res;
        else
            this->response.error_code = cqe->res;

        return true;
    }

    void fallocate_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto* sqe = sqes.front();

        io_uring_prep_fallocate(sqe, this->request.fd, this->request.mode, this->request.offset, this->request.length);
    }

    bool fallocate_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;

        return true;
    }

    void close_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto* sqe = sqes.front();

        io_uring_prep_close(sqe, this->request.fd);
    }

    bool close_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;

        return true;
    }

    void file_info_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto* sqe = sqes.front();

        io_uring_prep_statx(sqe, AT_FDCWD, this->request.path.data(), 0, STATX_SIZE | STATX_BASIC_STATS, &this->_statx_buf);
    }

    bool file_info_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
        {
            this->response.error_code = cqe->res;
            return true;
        }

        this->response.exists = true;
        this->response.file_size = this->_statx_buf.stx_size;

        return true;
    }

    void fsync_mailbox::prepare_sqes(std::span<io_uring_sqe*> sqes)
    {
        auto* sqe = sqes.front();

        io_uring_prep_fsync(sqe, this->request.fd, 0);
    }

    bool fsync_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;

        return true;
    }

    void ftruncate_mailbox::prepare_sqes(std::span<io_uring_sqe*>)
    {
        throw std::runtime_error("ftruncate is not supported in this iouring version");
    }

    bool ftruncate_mailbox::handle_cqe(io_uring_cqe* cqe, uint8_t /* sub_request_idx */)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;

        return true;
    }

} // namespace hedge::async