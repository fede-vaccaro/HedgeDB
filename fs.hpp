#pragma once 

#include <cstddef>
#include <array>
#include <cstring>
#include <filesystem>
#include <optional>

#include <fcntl.h>
#include <sys/mman.h>

#include <error.hpp>

namespace hedgehog::fs
{

    inline std::string get_error_message_thread_safe()
    {
        constexpr size_t BUF_SIZE = 1024;
        std::array<char, BUF_SIZE> buf{};

        strerror_r(errno, buf.data(), buf.size());

        return {buf.data()};
    }

    class fd_wrapper
    {
        int _fd = -1;
        size_t _file_size{};

    public:
        [[nodiscard]] int get() const
        {
            return this->_fd;
        }

        [[nodiscard]] size_t file_size() const
        {
            return this->_file_size;
        }

        static hedgehog::expected<fd_wrapper> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
        {
            if(!std::filesystem::exists(path))
                return hedgehog::error("File does not exist: " + path.string());

            // Open the file;
            int fd = open(path.c_str(), O_RDONLY); // NOLINT

            if(fd == -1)
            {
                auto err = get_error_message_thread_safe();
                return hedgehog::error("Failed to open file descriptor " + err);
            }

            fd_wrapper fd_wrapped{};
            fd_wrapped._fd = fd;

            // Get the size of the file
            size_t file_size = std::filesystem::file_size(path);

            if(expected_size.has_value() && file_size != expected_size.value())
                return hedgehog::error("Invalid file size! " + std::to_string(file_size) + " != " + std::to_string(expected_size.value()));

            fd_wrapped._file_size = file_size;

            return std::move(fd_wrapped);
        }

        fd_wrapper() = default;

        fd_wrapper(fd_wrapper&& other) : _fd(other._fd), _file_size(other._file_size)
        {
            other._fd = -1;
            other._file_size = 0;
        }

        fd_wrapper& operator=(fd_wrapper&& other)
        {
            this->_fd = other._fd;
            this->_file_size = other._file_size;

            other._fd = -1;
            other._file_size = 0;

            return *this;
        };

        fd_wrapper(const fd_wrapper&) = delete;
        fd_wrapper& operator=(const fd_wrapper&) = delete;

        ~fd_wrapper()
        {
            if(this->_fd != 0)
                close(this->_fd);
        }
    };

    class mmap_wrapper
    {
    private:
        fd_wrapper _fd_wrapper;
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;

    public:
        static hedgehog::expected<mmap_wrapper> from_fd_wrapper(fd_wrapper&& fd_w)
        {
            if(fd_w.get() == -1)
            {
                return hedgehog::error("Cannot map an invalid file descriptor.");
            }
            if(fd_w.file_size() == 0)
            {
            }

            void* mapped_ptr = mmap(nullptr, fd_w.file_size(), PROT_READ, MAP_PRIVATE, fd_w.get(), 0);

            if(mapped_ptr == MAP_FAILED)
            {
                auto err_msg = get_error_message_thread_safe();
                return hedgehog::error("Failed to mmap file: " + err_msg);
            }

            mmap_wrapper wrapper;
            wrapper._fd_wrapper = std::move(fd_w);
            wrapper._mapped_ptr = mapped_ptr;
            wrapper._mapped_size = wrapper._fd_wrapper.file_size();

            return std::move(wrapper);
        }

        static hedgehog::expected<mmap_wrapper> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
        {
            auto fd_res = fd_wrapper::from_path(path, expected_size);
            if(!fd_res.has_value())
            {
                return hedgehog::error("Failed to get file descriptor for mmap: " + fd_res.error().to_string());
            }
            return from_fd_wrapper(std::move(fd_res.value()));
        }

        mmap_wrapper() = default;

        mmap_wrapper(mmap_wrapper&& other) noexcept
            : _fd_wrapper(std::move(other._fd_wrapper)),
              _mapped_ptr(other._mapped_ptr),
              _mapped_size(other._mapped_size)
        {
            other._mapped_ptr = nullptr;
            other._mapped_size = 0;
        }

        mmap_wrapper& operator=(mmap_wrapper&& other) noexcept
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

        mmap_wrapper(const mmap_wrapper&) = delete;
        mmap_wrapper& operator=(const mmap_wrapper&) = delete;

        ~mmap_wrapper()
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
            return _fd_wrapper.get();
        }
    };

}