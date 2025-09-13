#include "paginated_view.h"

namespace hedgehog::async
{
    paginated_view::paginated_view(int32_t fd, const paginated_view_config& config, std::shared_ptr<async::executor_context> executor)
        : _fd(fd), _config(config), _executor(std::move(executor)), _page_it(config.initial_page_it)
    {
    }

    async::task<expected<std::vector<uint8_t>>> paginated_view::next(size_t num_pages, bool clamp_at_end)
    {
        if(this->_page_it >= this->_config.last_page_it)
            co_return std::vector<uint8_t>{}; // EOF

        if(this->_page_it + num_pages > this->_config.last_page_it)
        {
            if(clamp_at_end)
                num_pages = this->_config.last_page_it - this->_page_it;
            else
                co_return hedgehog::error(std::format("Requested pages beyond limit: {} + {} >= {}", this->_page_it, num_pages, this->_config.last_page_it));
        }

        size_t current_offset = this->_page_it * this->_config.page_size_bytes;
        size_t size = num_pages * this->_config.page_size_bytes;

        this->_page_it += num_pages;

        auto response = co_await this->_executor->submit_request(async::read_request{.fd = this->_fd, .offset = current_offset, .size = size});

        if(response.error_code != 0)
            co_return hedgehog::error(std::format("An error occurred with request fd: {}, current_offset: {}, size: {}. Error: ", this->_fd, current_offset, size, strerror(-response.error_code)));

        if(response.bytes_read != num_pages * this->_config.page_size_bytes)
            co_return hedgehog::error(std::format("Unexpected bytes read: {}. Expected: {}", response.bytes_read, num_pages * this->_config.page_size_bytes));

        co_return std::vector<uint8_t>(response.data.get(), response.data.get() + response.bytes_read);
    }

    async::task<hedgehog::expected<std::vector<uint8_t>>> paginated_view::next(size_t bytes_to_read)
    {
        size_t num_pages = ceil(bytes_to_read, this->_config.page_size_bytes);
        co_return co_await this->next(num_pages, true);
    }

    size_t paginated_view::get_current_page_it() const
    {
        return this->_page_it;
    }

    bool paginated_view::is_eof() const
    {
        return this->_page_it >= this->_config.last_page_it;
    }

    void paginated_view::reset_it(size_t it)
    {
        this->_page_it = std::min(it, this->_config.last_page_it);
    }

} // namespace hedgehog::async