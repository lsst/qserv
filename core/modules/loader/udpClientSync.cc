

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


#include "loader/BufferUdp.h"

using boost::asio::ip::udp;


int main(int argc, char* argv[]){
    using namespace lsst::qserv;

    try {
        if (argc != 3) {
            std::cerr << "Usage: client <host> <port>" << std::endl;
            return 1;
        }

        boost::asio::io_context io_context;

        udp::resolver resolver(io_context);
        udp::endpoint receiverEndpoint = *resolver.resolve(udp::v4(), argv[1], argv[2]).begin();

        udp::socket socket(io_context);
        socket.open(udp::v4());

        for (int j=0; j < 10000; ++j) {
            std::string msgOut("testing Argh ::");
            msgOut += std::to_string(j);
            std::cout << "sending=" << msgOut << std::endl;

            // This is a test of advanceWriteCursor() more than anything else.
            loader::BufferUdp msgBuf((char*)(msgOut.data()), msgOut.length());
            msgBuf.advanceWriteCursor(msgOut.length());
            socket.send_to(boost::asio::buffer(msgBuf.getReadCursor(), msgBuf.getBytesLeftToRead()),
                           receiverEndpoint);


            loader::BufferUdp respBuf;
            udp::endpoint senderEndpoint;
            size_t len = socket.receive_from(boost::asio::buffer(respBuf.getWriteCursor(),
                                             respBuf.getAvailableWriteLength()), senderEndpoint);
            respBuf.advanceWriteCursor(len);

            std::cout << "resp=" << respBuf.dumpStr(true, true) << std::endl;
        }
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}

