

#include <array>
#include <boost/asio.hpp>
#include <iostream>
#include <thread>

using boost::asio::ip::udp;
constexpr size_t buffer_size = 4096;

template <typename Storage>
class callback
{
    public:

    Storage buffer;
    udp::endpoint sender_endpoint;

    /* this function is the shim to access the async callback *with* data */
    template <typename Callback>
    auto operator()(const boost::system::error_code& ec, std::size_t bytes, Callback callback) const
    {
        auto buffer_begin = std::cbegin(buffer), buffer_end = std::cend(buffer);
        return callback(sender_endpoint, ec, bytes, buffer_begin,
                        buffer_begin + std::min((std::size_t)std::max(0l, std::distance(buffer_begin, buffer_end)), bytes));
    }
};

// just a demo callback with access to all the data. *This* is the target for all the surrounding effort!
template <typename Endpoint, typename Iterator>
void
print_to_cout(const Endpoint& endpoint, const boost::system::error_code& ec, std::size_t bytes, Iterator begin, Iterator end)
{
    std::cout << ": Buffer: " << std::string(begin, end) << ", error code: " << ec << ", with " << bytes << " bytes, from endpoint " << endpoint
              << std::endl;
}

class server
{
    public:

    // the storage for each callback has 4k, we have 10 of them… should be enough
    using buffer_t = std::array<char, buffer_size>;
    using callback_list_t = std::array<callback<buffer_t>, 3>;

    private:

    callback_list_t m_callback_list;
    udp::socket m_socket;

    public:

    server(boost::asio::io_service& io_service, short port)
      : m_socket(io_service, udp::endpoint(udp::v4(), port))
    {
        do_receive(m_socket, m_callback_list.begin(), m_callback_list.begin(), m_callback_list.end());
    }

    /* this is an exercise to pass the data to async_receive_from *without* capturing "this". Since we might call this
       recursively, we don't want to rely on "this" pointer. */
    static void do_receive(udp::socket& socket, callback_list_t::iterator current_callback, callback_list_t::iterator callback_begin,
                           callback_list_t::const_iterator callback_end)
    {
        // via the current_callback mechanism, we are able to compartmentalize the storage for each callback
        socket.async_receive_from(
          boost::asio::buffer(current_callback->buffer, buffer_size - 1), current_callback->sender_endpoint,
          [&socket, current_callback, callback_begin, callback_end](const boost::system::error_code& ec, std::size_t bytes_recvd) {
              // now, move on to the next storage-and-callback - with wraparound!
              auto next_callback = current_callback + 1;
              do_receive(socket, (next_callback != callback_end) ? next_callback : callback_begin, callback_begin, callback_end);

              // call our handler. we need to be sure to have access to the data - but we want to call do_receive as soon as possible!
              // if this takes too long, you might want to ship this off to a separate thead. Subsequent calls to do_receive are
              // *not* depending on data in current_callback!
              (*current_callback)(ec, bytes_recvd, print_to_cout<decltype(current_callback->sender_endpoint), buffer_t::const_iterator>);
              // since this is an echo service, call do_send() with the same data
              do_send(socket, current_callback, bytes_recvd);
          });
    }

    // do_send is much easier than do_receive - we *have* all the data, we don't need to advance the callback…
    static void do_send(udp::socket& socket, callback_list_t::iterator current_callback, std::size_t length)
    {
        socket.async_send_to(boost::asio::buffer(current_callback->buffer, std::min(length, buffer_size - 1)), current_callback->sender_endpoint,
                             [current_callback](const boost::system::error_code& ec, std::size_t bytes_sent) {
                                 (*current_callback)(ec, bytes_sent,
                                                     print_to_cout<decltype(current_callback->sender_endpoint), buffer_t::const_iterator>);
                             });
    }
};

int
main(int argc, char* argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: async_udp_echo_server <port>" << std::endl;
        return 1;
    }

    boost::asio::io_service io_service;

    server s(io_service, std::atoi(argv[1]));

    // now to the fun part - let's call the io_service several times
    std::thread thread1([&io_service]() { io_service.run(); });
    std::thread thread2([&io_service]() { io_service.run(); });
    std::thread thread3([&io_service]() { io_service.run(); });
    std::thread thread4([&io_service]() { io_service.run(); });
    thread1.join();
    thread2.join();
    thread3.join();
    thread4.join();
    return 0;
}
