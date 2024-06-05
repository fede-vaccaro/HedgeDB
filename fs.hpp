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

    class file_descriptor
    {
        int _fd = -1;
        size_t _file_size{};
        std::filesystem::path _path;

    public:
        [[nodiscard]] int get() const
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

        static hedgehog::expected<file_descriptor> from_path(const std::filesystem::path& path, std::optional<size_t> expected_size = std::nullopt)
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

            file_descriptor fd_wrapped{};
            fd_wrapped._fd = fd;

            // Get the size of the file
            size_t file_size = std::filesystem::file_size(path);

            if(expected_size.has_value() && file_size != expected_size.value())
                return hedgehog::error("Invalid file size! " + std::to_string(file_size) + " != " + std::to_string(expected_size.value()));

            fd_wrapped._file_size = file_size;
            fd_wrapped._path = path;

            return std::move(fd_wrapped);
        }

        file_descriptor() = default;

        file_descriptor(file_descriptor&& other) : _fd(other._fd), _file_size(other._file_size), _path(std::move(other._path))
        {
            other._fd = -1;
            other._file_size = 0;
        }

        file_descriptor& operator=(file_descriptor&& other)
        {
            this->_fd = other._fd;
            this->_file_size = other._file_size;
            this->_path = std::move(other._path);

            other._fd = -1;
            other._file_size = 0;

            return *this;
        };

        file_descriptor(const file_descriptor&) = delete;
        file_descriptor& operator=(const file_descriptor&) = delete;

        ~file_descriptor()
        {
            if(this->_fd != 0)
                close(this->_fd);
        }
    };

    class mmap_wrapper
    {
    private:
        file_descriptor _fd_wrapper;
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;

    public:
        static hedgehog::expected<mmap_wrapper> from_fd_wrapper(file_descriptor&& fd_w)
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
            auto fd_res = file_descriptor::from_path(path, expected_size);
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

    class tmp_mmap
    {
    private:
        file_descriptor* _fd_wrapper;
        void* _mapped_ptr = nullptr;
        size_t _mapped_size = 0;

    public:
        static hedgehog::expected<tmp_mmap> from_fd_wrapper(file_descriptor* fd_w)
        {
            if(fd_w == nullptr)
                return hedgehog::error("Cannot map a null file descriptor.");

            if(fd_w->get() == -1)
                return hedgehog::error("Cannot map an invalid file descriptor.");

            if(fd_w->file_size() == 0)
                return hedgehog::error("Cannot mmap an empty file.");

            void* mapped_ptr = mmap(nullptr, fd_w->file_size(), PROT_READ, MAP_PRIVATE, fd_w->get(), 0);

            if(mapped_ptr == MAP_FAILED)
            {
                auto err_msg = get_error_message_thread_safe();
                return hedgehog::error("Failed to mmap file: " + err_msg);
            }

            tmp_mmap wrapper;
            wrapper._fd_wrapper = fd_w;
            wrapper._mapped_ptr = mapped_ptr;
            wrapper._mapped_size = wrapper._fd_wrapper->file_size();

            return std::move(wrapper);
        }

        tmp_mmap() = default;

        tmp_mmap(tmp_mmap&& other) noexcept
            : _fd_wrapper(std::move(other._fd_wrapper)),
              _mapped_ptr(other._mapped_ptr),
              _mapped_size(other._mapped_size)
        {
            other._mapped_ptr = nullptr;
            other._mapped_size = 0;
        }

        tmp_mmap& operator=(tmp_mmap&& other) noexcept
        {
            if(this != &other)
            {
                _fd_wrapper = std::move(other._fd_wrapper);
                _mapped_ptr = other._mapped_ptr;
                _mapped_size = other._mapped_size;

                other._mapped_ptr = nullptr;
                other._mapped_size = 0;
            }
            return *this;
        }

        tmp_mmap(const tmp_mmap&) = delete;
        tmp_mmap& operator=(const tmp_mmap&) = delete;

        ~tmp_mmap()
        {
            if(_mapped_ptr != nullptr && _mapped_ptr != MAP_FAILED)
            {
                if(munmap(_mapped_ptr, _mapped_size) == -1)
                    perror("Error munmapping file in mmap_wrapper destructor");
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
            return _fd_wrapper->get();
        }
    };


}