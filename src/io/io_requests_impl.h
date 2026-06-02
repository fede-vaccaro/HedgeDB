#pragma once

#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstdint>
#include <linux/stat.h>
#include <string>

#include <liburing.h>

namespace hedge::io
{

    struct io_request
    {
        int32_t res{0};

        virtual void prepare_sqe(io_uring_sqe* sqe) = 0;
        virtual ~io_request() = default;
    };

    struct io_read_request final : io_request
    {
        int32_t fd;
        void* data;
        size_t len;
        size_t off;

        io_read_request(int32_t fd, void* data, size_t len, size_t off) : fd(fd), data(data), len(len), off(off) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_read(sqe, fd, data, len, off);
        }
    };

    struct io_read_fixed_request final : io_request
    {
        int32_t fd;
        void* data;
        size_t size;
        size_t offset;
        int32_t buf_index;

        io_read_fixed_request(int32_t fd, void* data, size_t size, size_t offset, int32_t buf_index)
            : fd(fd), data(data), size(size), offset(offset), buf_index(buf_index) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_read_fixed(sqe, fd, data, size, offset, buf_index);
        }
    };

    struct io_readv_request final : io_request
    {
        int32_t fd;
        iovec* iovecs;
        size_t iovecs_count;
        size_t offset;

        io_readv_request(int32_t fd, iovec* iovecs, size_t iovecs_count, size_t offset)
            : fd(fd), iovecs(iovecs), iovecs_count(iovecs_count), offset(offset) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_readv(sqe, fd, iovecs, iovecs_count, offset);
        }
    };

    struct io_write_request final : io_request
    {
        int32_t fd;
        const void* data;
        size_t size;
        size_t offset;

        io_write_request(int32_t fd, const void* data, size_t size, size_t offset)
            : fd(fd), data(data), size(size), offset(offset) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_write(sqe, fd, data, size, offset);
        }
    };

    struct io_writev_request final : io_request
    {
        int32_t fd;
        const iovec* iovecs;
        size_t iovecs_count;
        size_t offset;

        io_writev_request(int32_t fd, const iovec* iovecs, size_t iovecs_count, size_t offset)
            : fd(fd), iovecs(iovecs), iovecs_count(iovecs_count), offset(offset) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_writev(sqe, fd, iovecs, iovecs_count, offset);
        }
    };

    struct io_open_request final : io_request
    {
        std::string path;
        int32_t flags;
        mode_t mode;

        io_open_request(std::string path, int32_t flags, mode_t mode = 0777)
            : path(std::move(path)), flags(flags), mode(mode) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_openat(sqe, AT_FDCWD, path.c_str(), flags, mode);
        }
    };

    struct io_fallocate_request final : io_request
    {
        int32_t fd;
        int32_t mode;
        size_t offset;
        size_t length;

        io_fallocate_request(int32_t fd, int32_t mode, size_t offset, size_t length)
            : fd(fd), mode(mode), offset(offset), length(length) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_fallocate(sqe, fd, mode, offset, length);
        }
    };

    struct io_close_request final : io_request
    {
        int32_t fd;

        io_close_request(int32_t fd) : fd(fd) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_close(sqe, fd);
        }
    };

    struct io_statx_request final : io_request
    {
        std::string path;
        unsigned int mask;
        struct statx* statx_buf;

        io_statx_request(std::string path, struct statx* statx_buf, unsigned int mask = STATX_SIZE | STATX_BASIC_STATS)
            : path(std::move(path)), mask(mask), statx_buf(statx_buf) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_statx(sqe, AT_FDCWD, path.c_str(), 0, mask, statx_buf);
        }
    };

    struct io_fdatasync_request final : io_request
    {
        int32_t fd;

        io_fdatasync_request(int32_t fd) : fd(fd) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
        }
    };

    struct io_fsync_request final : io_request
    {
        int32_t fd;

        io_fsync_request(int32_t fd) : fd(fd) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_fsync(sqe, fd, 0);
        }
    };

    struct io_ftruncate_request final : io_request
    {
        int32_t fd;
        size_t length;

        io_ftruncate_request(int32_t fd, size_t length) : fd(fd), length(length) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_ftruncate(sqe, fd, length);
        }
    };

    struct io_unlink_request final : io_request
    {
        std::string path;
        int32_t flags;

        io_unlink_request(std::string path, int32_t flags = 0)
            : path(std::move(path)), flags(flags) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_unlinkat(sqe, AT_FDCWD, path.c_str(), flags);
        }
    };

    struct io_send_msg_request final : io_request
    {
        int32_t target_ring_fd;
        uint64_t data;
        int32_t len;

        io_send_msg_request(int32_t target_ring_fd, uint64_t data, int32_t len)
            : target_ring_fd(target_ring_fd), data(data), len(len) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_msg_ring(sqe, target_ring_fd, len, data, 0);
        }
    };

    struct io_timeout_request final : io_request
    {
        __kernel_timespec ts;
        unsigned count;
        unsigned flags;

        io_timeout_request(__kernel_timespec ts, unsigned count, unsigned flags)
            : ts(ts), count(count), flags(flags) {}

        void prepare_sqe(io_uring_sqe* sqe) override
        {
            io_uring_prep_timeout(sqe, &ts, count, flags);
        }
    };

} // namespace hedge::io
