

//
// client.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2017 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <iostream>
#include <boost/array.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::udp;

int main(int argc, char* argv[])
{
  try
  {
    if (argc != 2)
    {
      std::cerr << "Usage: client <host>" << std::endl;
      return 1;
    }

    boost::asio::io_context io_context;

    udp::resolver resolver(io_context);
    udp::endpoint receiver_endpoint = *resolver.resolve(udp::v4(), argv[1], "10042").begin();

    udp::socket socket(io_context);
    socket.open(udp::v4());

    for (int j=0; j < 10000; ++j) {
        std::string msgOut("testing ::");
        msgOut += std::to_string(j);
        std::cout << "sending=" << msgOut << std::endl;
        socket.send_to(boost::asio::buffer(msgOut), receiver_endpoint);

        boost::array<char, 128> recv_buf;
        udp::endpoint sender_endpoint;
        size_t len = socket.receive_from(
                boost::asio::buffer(recv_buf), sender_endpoint);

        std::string msgIn(recv_buf.data(), len);
        std::cout << "recv(" << len << ")=" << msgIn << std::endl;
    }
  }
  catch (std::exception& e)
  {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}
