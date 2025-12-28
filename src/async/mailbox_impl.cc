#include <cstdint>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <stdexcept>
#include <stdlib.h>

#include <logger.h>

#include "mailbox_impl.h"

namespace hedge::async
{
    void read_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_read(sqe, this->request.fd, this->request.data, this->request.size, this->request.offset);
    }

    void read_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;
    }

    void unaligned_read_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        this->response = {
            .data = std::vector<uint8_t>(request.size),
            .bytes_read = 0};

        io_uring_prep_read(sqe, this->request.fd, this->response.data.data(), this->request.size, this->request.offset);
    }

    void unaligned_read_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;
    }

    void unaligned_readv_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        this->response = {
            .bytes_read = 0,
            .error_code = 0};

        io_uring_prep_readv(sqe, this->request.fd, this->request.iovecs, this->request.iovecs_count, this->request.offset);
    }

    void unaligned_readv_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
        else
            this->response.bytes_read = cqe->res;
    }

    void write_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        this->response = {};

        io_uring_prep_write(sqe, this->request.fd, this->request.data, this->request.size, this->request.offset);
    }

    void write_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            response.error_code = cqe->res;

        this->response.bytes_written = cqe->res;
    }

    void writev_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        this->response = {};

        io_uring_prep_writev(sqe, this->request.fd, request.iovecs, this->request.iovecs_count, this->request.offset);
    }

    void writev_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            response.error_code = cqe->res;

        this->response.bytes_written = cqe->res;
    }

    void open_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_openat(sqe, AT_FDCWD, this->request.path.c_str(), this->request.flags, this->request.mode);
    }

    void open_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res >= 0)
            this->response.file_descriptor = cqe->res;
        else
            this->response.error_code = cqe->res;
    }

    void fallocate_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_fallocate(sqe, this->request.fd, this->request.mode, this->request.offset, this->request.length);
    }

    void fallocate_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
    }

    void close_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_close(sqe, this->request.fd);
    }

    void close_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
    }

    void file_info_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_statx(sqe, AT_FDCWD, this->request.path.data(), 0, STATX_SIZE | STATX_BASIC_STATS, &this->_statx_buf);
    }

    void file_info_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
        {
            this->response.error_code = cqe->res;
            return;
        }

        this->response.exists = true;
        this->response.file_size = this->_statx_buf.stx_size;
    }

    void fsync_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_fsync(sqe, this->request.fd, 0);
    }

    void fsync_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
    }

    void ftruncate_mailbox::prepare_sqe(io_uring_sqe*)
    {
        throw std::runtime_error("ftruncate is not supported in this iouring version");
    }

    void ftruncate_mailbox::handle_cqe(io_uring_cqe* cqe)
    {
        if(cqe->res < 0)
            this->response.error_code = cqe->res;
    }

    void yield_mailbox::prepare_sqe(io_uring_sqe* sqe)
    {
        io_uring_prep_nop(sqe);
    }

    void yield_mailbox::handle_cqe(io_uring_cqe* /* cqe */)
    {
    }

} // namespace hedge::async