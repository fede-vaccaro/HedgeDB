#pragma once

#include "io_ctx.h"

namespace hedge::io
{

    hedge::io::aw_io read(int32_t fd, void* buf, size_t count, size_t offset);
    hedge::io::aw_io read_fixed(int32_t fd, void* buf, size_t size, size_t offset, int32_t buf_index);
    hedge::io::aw_io readv(int32_t fd, iovec* iovecs, size_t count, size_t offset);
    hedge::io::aw_io write(int32_t fd, const void* buf, size_t count, size_t offset);
    hedge::io::aw_io writev(int32_t fd, const iovec* iovecs, size_t count, size_t offset);
    hedge::io::aw_io open(std::string path, int32_t flags, mode_t mode = 0777);
    hedge::io::aw_io fallocate(int32_t fd, int32_t mode, size_t offset, size_t length);
    hedge::io::aw_io close(int32_t fd);
    hedge::io::aw_io statx(std::string path, struct statx* buf, unsigned int mask = STATX_SIZE | STATX_BASIC_STATS);
    hedge::io::aw_io fdatasync(int32_t fd);
    hedge::io::aw_io ftruncate(int32_t fd, size_t length);
    hedge::io::aw_io unlink(std::string path, int32_t flags = 0);
    hedge::io::aw_io send_msg(int32_t target_ring_fd, uint64_t data, int32_t len);
    hedge::io::aw_io timeout(__kernel_timespec ts, unsigned count = 0, unsigned flags = 0);

#ifdef HEDGE_IO_IMPL
#undef HEDGE_IO_IMPL
    hedge::io::aw_io read(int32_t fd, void* buf, size_t count, size_t offset)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_read_request>(fd, buf, count, offset)};
    }

    hedge::io::aw_io read_fixed(int32_t fd, void* buf, size_t size, size_t offset, int32_t buf_index)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_read_fixed_request>(fd, buf, size, offset, buf_index)};
    }

    hedge::io::aw_io readv(int32_t fd, iovec* iovecs, size_t count, size_t offset)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_readv_request>(fd, iovecs, count, offset)};
    }

    hedge::io::aw_io write(int32_t fd, const void* buf, size_t count, size_t offset)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_write_request>(fd, buf, count, offset)};
    }

    hedge::io::aw_io writev(int32_t fd, const iovec* iovecs, size_t count, size_t offset)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_writev_request>(fd, iovecs, count, offset)};
    }

    hedge::io::aw_io open(std::string path, int32_t flags, mode_t mode)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_open_request>(std::move(path), flags, mode)};
    }

    hedge::io::aw_io fallocate(int32_t fd, int32_t mode, size_t offset, size_t length)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_fallocate_request>(fd, mode, offset, length)};
    }

    hedge::io::aw_io close(int32_t fd)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_close_request>(fd)};
    }

    hedge::io::aw_io statx(std::string path, struct statx* buf, unsigned int mask)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_statx_request>(std::move(path), buf, mask)};
    }

    hedge::io::aw_io fdatasync(int32_t fd)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_fdatasync_request>(fd)};
    }

    hedge::io::aw_io ftruncate(int32_t fd, size_t length)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_ftruncate_request>(fd, length)};
    }

    hedge::io::aw_io unlink(std::string path, int32_t flags)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_unlink_request>(std::move(path), flags)};
    }

    hedge::io::aw_io send_msg(int32_t target_ring_fd, uint64_t data, int32_t len)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_send_msg_request>(target_ring_fd, data, len)};
    }

    hedge::io::aw_io timeout(__kernel_timespec ts, unsigned count, unsigned flags)
    {
        return hedge::io::aw_io{std::make_unique<hedge::io::io_timeout_request>(ts, count, flags)};
    }

#endif // HEDGE_IO_IMPL

} // namespace hedge::io
