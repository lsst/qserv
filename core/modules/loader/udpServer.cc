
#if 1

// System header
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

#include <stdio.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>

// Qserv headers
#include "loader/ServerUdpBase.h"


int main(int argc, char* argv[]) {
    try {
        if (argc != 2) {
            std::cerr << "Usage: async_udp_echo_server <port>\n";
            return 1;
        }

        std::string host = "127.0.0.1";

        struct ifaddrs* ifAddrStruct=NULL;
        struct ifaddrs* ifa=NULL;
        void* tmpAddrPtr=NULL;

        getifaddrs(&ifAddrStruct);

        for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
            if (!ifa->ifa_addr) {
                continue;
            }
            if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
                // is a valid IP4 Address
                tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
                char addressBuffer[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            } else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
                // is a valid IP6 Address
                tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
                char addressBuffer[INET6_ADDRSTRLEN];
                inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
                printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
            }
        }
        if (ifAddrStruct!=NULL) freeifaddrs(ifAddrStruct);
        /* example output of above:
           lo IP Address 127.0.0.1
           eth0 IP Address 134.79.208.18
           virbr0 IP Address 192.168.122.1
           docker0 IP Address 172.17.42.1
           lo IP Address ::1
           eth0 IP Address fe80::9a90:96ff:fe9e:8eb0
           docker0 IP Address fe80::469:ecff:fe70:391e
         */


        boost::asio::io_service ioService;
        //std::string host = argv[1];
        int port = std::atoi(argv[1]);
        std::cout << "host=" << host << " port=" << port << std::endl;
        lsst::qserv::loader::ServerUdpBase server(ioService, host, port);

        ioService.run();
    }
    catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}



#else
//
// async_tcp_echo_server.cpp
// ~~~~~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2017 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class session : public std::enable_shared_from_this<session> {
public:
  session(tcp::socket socket) : socket_(std::move(socket)) {}

  void start() {
    do_read();
  }

private:
  void do_read() {
      auto self(shared_from_this());
      socket_.async_read_some(boost::asio::buffer(data_, max_length),
              [this, self](boost::system::error_code ec, std::size_t length) {
          if (!ec) {
              std::string str(data_, length);
              std::cout << "do_read:" << str << std::endl;
              do_write(length);
          }
      });
  }

  void do_write(std::size_t length) {
      auto self(shared_from_this());
      std::string str(data_, length);
      std::cout << "do_write:" << str << std::endl;
      boost::asio::async_write(socket_, boost::asio::buffer(data_, length),
              [this, self](boost::system::error_code ec, std::size_t /*length*/) {
          if (!ec) {
              do_read();
          }
      });
  }

  tcp::socket socket_;
  enum { max_length = 1024 };
  char data_[max_length];
};

class server
{
public:
  server(boost::asio::io_context& io_context, short port)
    : acceptor_(io_context, tcp::endpoint(tcp::v4(), port))
  {
    do_accept();
  }

private:
  void do_accept()
  {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket)
        {
          if (!ec)
          {
            std::make_shared<session>(std::move(socket))->start();
          }

          do_accept();
        });
  }

  tcp::acceptor acceptor_;
};

int main(int argc, char* argv[])
{
  try
  {
    if (argc != 2)
    {
      std::cerr << "Usage: async_tcp_echo_server <port>\n";
      return 1;
    }

    boost::asio::io_context io_context;

    int port = std::atoi(argv[1]);

    std::cout << "port=" << port << std::endl;

    server s(io_context, port);

    io_context.run();
  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}


///////////////////////////////////////////////////////////////////////////
//
// server.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2017 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <ctime>
#include <iostream>
#include <string>
#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::udp;

std::string make_daytime_string()
{
  using namespace std; // For time_t, time and ctime;
  time_t now = time(0);
  return ctime(&now);
}

class udp_server
{
public:
  udp_server(boost::asio::io_context& io_context)
    : socket_(io_context, udp::endpoint(udp::v4(), 10013))
  {
    start_receive();
  }

private:
  void start_receive()
  {
      socket_.async_receive_from(
          boost::asio::buffer(recv_buffer_), remote_endpoint_,
          boost::bind(&udp_server::handle_receive, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
    }

    void handle_receive(const boost::system::error_code& error,
        std::size_t /*bytes_transferred*/)
    {
      if (!error || error == boost::asio::error::message_size)
      {
        boost::shared_ptr<std::string> message(
            new std::string(make_daytime_string()));

        socket_.async_send_to(boost::asio::buffer(*message), remote_endpoint_,
            boost::bind(&udp_server::handle_send, this, message,
              boost::asio::placeholders::error,
              boost::asio::placeholders::bytes_transferred));

        start_receive();
      }
    }

    void handle_send(boost::shared_ptr<std::string> /*message*/,
        const boost::system::error_code& /*error*/,
        std::size_t /*bytes_transferred*/)
    {
    }

    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    boost::array<char, 1> recv_buffer_;
  };

  int main()
  {
    try
    {
      boost::asio::io_context io_context;
      udp_server server(io_context);
      io_context.run();
    }
    catch (std::exception& e)
    {
      std::cerr << e.what() << std::endl;
    }

    return 0;
  }

#endif
