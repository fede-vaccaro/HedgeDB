#pragma once

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <optional>

#include <fcntl.h>
#include <sys/mman.h>

#include <error.hpp>
#include <utility>

#include "async/io_executor.h"
#include "async/mailbox_impl.h"
#include "async/task.h"

/*
    HedgeFS File Abstraction

    Provides a simple file abstraction over POSIX file descriptors with support for asynchronous operations via io_uring.
*/
namespace hedge::fs
{

    // todo: switch to a thread-safe version of strerror
    inline std::string get_error_message()
    {
        // constexpr size_t BUF_SIZE = 1024;
        // std::array<char, BUF_SIZE> buf{};

        // strerror_r(errno, buf.data(), buf.size());

        // return {buf.data()};
        return strerror(errno);
    }

    class file
    {
    public:
        enum class open_mode : int32_t // NOLINT(performance-enum-size) this will translate to int32_t anyway in the syscall
        {
            undefined = -1,
            read_only = O_RDONLY,
            write_new = O_WRONLY | O_CREAT | O_TRUNC,
            read_write = O_RDWR,
            read_write_new = O_RDWR | O_CREAT | O_TRUNC,
        };

    private:
        int _fd = -1;
        size_t _file_size{};
        std::filesystem::path _path;
        open_mode _mode{open_mode::undefined};
        bool _use_direct{false};
        std::atomic_bool _deletion_triggered{false};

    public:
        [[nodiscard]] int fd() const
        {
            return this->_fd;
        }

        [[nodiscard]] size_t file_size() const
        {
            return this->_file_size;
        }

        [[nodiscard]] const std::filesystem::path& path() const
        {
            return this->_path;
        }

        [[nodiscard]] open_mode mode() const
        {
            return this->_mode;
        }

        [[nodiscard]] bool has_direct_access() const
        {
            return this->_use_direct;
        }

        void set_delete_on_obj_destruction(bool val)
        {
            this->_deletion_triggered.store(val);
        }

        static hedge::expected<file> from_path(const std::filesystem::path& path, open_mode mode, bool use_direct = false, std::optional<size_t> expected_size = std::nullopt)
        {
            auto exists = std::filesystem::exists(path);

            if(!exists && mode == open_mode::read_only)
                return hedge::error("File does not exist: " + path.string());

            if(exists && (mode == open_mode::write_new || mode == open_mode::read_write_new))
                return hedge::error("File already exists: " + path.string());

            // Open the file;
            auto flag = static_cast<int>(mode);
            if(use_direct)
                flag |= O_DIRECT;

            int fd = open(path.c_str(), flag, 0777); // NOLINT

            if(fd == -1)
            {
                auto err = std::string(strerror(errno));
                return hedge::error("Failed to open file descriptor " + err);
            }

            size_t file_size = std::filesystem::file_size(path);

            if(expected_size.has_value())
            {
                switch(mode)
                {
                    case open_mode::undefined:
                        break;
                    case open_mode::read_write:
                    case open_mode::read_only:
                        if(file_size != expected_size.value())
                            return hedge::error("Invalid file size! " + std::to_string(file_size) + " != " + std::to_string(expected_size.value()));
                        break;
                    case open_mode::write_new:
                    case open_mode::read_write_new:
                        auto res = fallocate(fd, 0, 0, expected_size.value());
                        if(res == -1)
                        {
                            auto err = get_error_message();

                            std::filesystem::remove(path);
                            close(fd);
                            return hedge::error("Failed to allocate space for file: " + err);
                        }
                        file_size = expected_size.value();
                        break;
                };
            }

            file fd_wrapped{};

            fd_wrapped._fd = fd;
            fd_wrapped._file_size = file_size;
            fd_wrapped._path = path;
            fd_wrapped._mode = mode;
            fd_wrapped._use_direct = use_direct;

            return fd_wrapped;
        }

        static async::task<hedge::expected<file>> from_path_async(const std::filesystem::path& path, open_mode mode, std::shared_ptr<async::executor_context> executor, bool use_direct = false, std::optional<size_t> expected_size = std::nullopt)
        {
            // auto exists = std::filesystem::exists(path);
            auto stats = co_await executor->submit_request(async::file_info_request{
                .path = path.string()});

            if(!stats.exists && mode == open_mode::read_only)
                co_return hedge::error("File does not exist: " + path.string());

            if(stats.exists && mode == open_mode::write_new)
                co_return hedge::error("File already exists: " + path.string());

            // Open the file;
            auto flags = static_cast<int32_t>(mode);
            if(use_direct)
                flags |= O_DIRECT;

            auto open_retvalue = co_await executor->submit_request(async::open_request{
                .path = path.string(),
                .flags = flags,
                .mode = 0777});

            if(open_retvalue.error_code < 0)
                co_return hedge::error("Failed to open file descriptor: " + std::string(strerror(-open_retvalue.error_code)));

            int fd = open_retvalue.file_descriptor;

            size_t file_size = stats.file_size;

            if(expected_size.has_value())
            {
                switch(mode)
                {
                    case open_mode::undefined:
                        break;
                    case open_mode::read_write:
                    case open_mode::read_only:
                        if(file_size != expected_size.value())
                            co_return hedge::error("File size different than expected: " + std::to_string(file_size) + " != " + std::to_string(expected_size.value()));
                        break;
                    case open_mode::write_new:
                    case open_mode::read_write_new:
                        auto res = co_await executor->submit_request(async::fallocate_request{
                            .fd = fd,
                            .mode = 0,
                            .offset = 0,
                            .length = expected_size.value()});

                        if(res.error_code < 0)
                        {
                            auto err = std::string(strerror(-res.error_code));
                            co_return hedge::error("Failed to allocate space for file: " + err);

                            std::filesystem::remove(path); // todo: implement mailbox based remove function

                            auto close_result = co_await executor->submit_request(async::close_request{fd});

                            if(close_result.error_code < 0)
                            {
                                auto err = std::string(strerror(-close_result.error_code));
                                co_return hedge::error("Failed to close file descriptor after fallocate failure: " + err);
                            }

                            co_return hedge::error("Failed to allocate space for file: " + err);
                        }
                        break;
                };
            }

            file fd_wrapped{};

            fd_wrapped._fd = fd;
            fd_wrapped._file_size = file_size;
            fd_wrapped._path = path;
            fd_wrapped._mode = mode;
            fd_wrapped._use_direct = use_direct;

            co_return fd_wrapped;
        }

        file() = default;

        file(file&& other) noexcept : _fd(std::exchange(other._fd, -1)),
                                      _file_size(std::exchange(other._file_size, 0)),
                                      _path(std::move(other._path)),
                                      _mode(std::exchange(other._mode, open_mode::undefined)),
                                      _use_direct(std::exchange(other._use_direct, false)),
                                      _deletion_triggered(other._deletion_triggered.exchange(false))
        {
        }

        file& operator=(file&& other) noexcept
        {
            if(this == &other)
                return *this;

            this->_fd = std::exchange(other._fd, -1);
            this->_file_size = std::exchange(other._file_size, 0);
            this->_path = std::move(other._path);
            this->_mode = std::exchange(other._mode, open_mode::undefined);
            this->_use_direct = std::exchange(other._use_direct, false);
            this->_deletion_triggered = other._deletion_triggered.exchange(false);

            return *this;
        };

        file(const file&) = delete;
        file& operator=(const file&) = delete;

        ~file()
        {
            if(this->_fd < 0)
                return;

            close(this->_fd);

            if(this->_deletion_triggered.load())
                std::filesystem::remove(this->_path);
        }
    };

    class mmap_owning
    {
    private:
        file _fd_wrapper;
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;

    public:
        static hedge::expected<mmap_owning> from_fd_wrapper(file&& fd_w)
        {
            if(fd_w.fd() == -1)
            {
                return hedge::error("Cannot map an invalid file descriptor.");
            }
            if(fd_w.file_size() == 0)
            {
            }

            void* mapped_ptr = mmap(nullptr, fd_w.file_size(), PROT_READ, MAP_PRIVATE, fd_w.fd(), 0);

            if(mapped_ptr == MAP_FAILED)
            {
                auto err_msg = get_error_message();
                return hedge::error("Failed to mmap file: " + err_msg);
            }

            mmap_owning wrapper;
            wrapper._fd_wrapper = std::move(fd_w);
            wrapper._mapped_ptr = mapped_ptr;
            wrapper._mapped_size = wrapper._fd_wrapper.file_size();

            return wrapper;
        }

        static hedge::expected<mmap_owning> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
        {
            auto fd_res = file::from_path(path, file::open_mode::read_only, false, expected_size);
            if(!fd_res.has_value())
                return hedge::error("Failed to get file descriptor for mmap: " + fd_res.error().to_string());

            return from_fd_wrapper(std::move(fd_res.value()));
        }

        mmap_owning() = default;

        mmap_owning(mmap_owning&& other) noexcept
            : _fd_wrapper(std::move(other._fd_wrapper)),
              _mapped_ptr(other._mapped_ptr),
              _mapped_size(other._mapped_size)
        {
            other._mapped_ptr = nullptr;
            other._mapped_size = 0;
        }

        mmap_owning& operator=(mmap_owning&& other) noexcept
        {
            if(this != &other)
            {
                if(_mapped_ptr != nullptr && _mapped_ptr != MAP_FAILED)
                {
                    munmap(_mapped_ptr, _mapped_size);
                }

                _fd_wrapper = std::move(other._fd_wrapper);
                _mapped_ptr = other._mapped_ptr;
                _mapped_size = other._mapped_size;

                other._mapped_ptr = nullptr;
                other._mapped_size = 0;
            }
            return *this;
        }

        mmap_owning(const mmap_owning&) = delete;
        mmap_owning& operator=(const mmap_owning&) = delete;

        ~mmap_owning()
        {
            if(_mapped_ptr != nullptr && _mapped_ptr != MAP_FAILED)
            {
                if(munmap(_mapped_ptr, _mapped_size) == -1)
                {
                    perror("Error munmapping file in mmap_wrapper destructor");
                }
            }
        }

        [[nodiscard]] void* get_ptr() const
        {
            return _mapped_ptr;
        }

        [[nodiscard]] size_t size() const
        {
            return _mapped_size;
        }

        [[nodiscard]] int get_fd() const
        {
            return _fd_wrapper.fd();
        }
    };

    struct range
    {
        size_t start;
        size_t size;
    };

    class mmap_view
    {
    private:
        int32_t _fd{-1};
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;
        std::optional<range> _range;

    public:
        static hedge::expected<mmap_view> from_file(const file& fd_w, std::optional<range> range = std::nullopt)
        {
            if(fd_w.fd() == -1)
                return hedge::error("Cannot map an invalid file descriptor.");

            if(fd_w.file_size() == 0)
                return hedge::error("Cannot mmap an empty file.");

            void* mapped_ptr = mmap(nullptr, range ? range->size : fd_w.file_size(), PROT_READ | PROT_WRITE, MAP_SHARED, fd_w.fd(), range ? range->start : 0);

            if(mapped_ptr == MAP_FAILED)
            {
                auto err_msg = get_error_message();
                return hedge::error("Failed to mmap file: " + err_msg);
            }

            mmap_view wrapper;
            wrapper._fd = fd_w.fd();
            wrapper._mapped_ptr = mapped_ptr;
            wrapper._mapped_size = range ? range->size : fd_w.file_size();
            wrapper._range = range;

            return wrapper;
        }

        mmap_view() = default;

        mmap_view(mmap_view&& other) noexcept
            : _fd(std::exchange(other._fd, -1)),
              _mapped_ptr(std::exchange(other._mapped_ptr, nullptr)),
              _mapped_size(std::exchange(other._mapped_size, 0)),
              _range(std::exchange(other._range, std::nullopt))
        {
        }

        mmap_view& operator=(mmap_view&& other) noexcept
        {
            if(this != &other)
            {
                this->_fd = std::exchange(other._fd, -1);
                this->_mapped_ptr = std::exchange(other._mapped_ptr, nullptr);
                this->_mapped_size = std::exchange(other._mapped_size, 0);
                this->_range = std::exchange(other._range, std::nullopt);
            }
            return *this;
        }

        mmap_view(const mmap_view&) = delete;
        mmap_view& operator=(const mmap_view&) = delete;

        ~mmap_view()
        {
            if(this->_mapped_ptr != nullptr && this->_mapped_ptr != MAP_FAILED)
            {
                if(munmap(this->_mapped_ptr, this->_mapped_size) == -1)
                    perror("Error munmapping file in mmap_wrapper destructor");
            }
        }

        [[nodiscard]] void* get_ptr() const
        {
            return this->_mapped_ptr;
        }

        template <typename T>
        [[nodiscard]] T* get_ptr() const
        {
            return static_cast<T*>(this->_mapped_ptr);
        }

        [[nodiscard]] size_t size() const
        {
            return this->_mapped_size;
        }

        [[nodiscard]] int get_fd() const
        {
            return this->_fd;
        }
    };

} // namespace hedge::fs