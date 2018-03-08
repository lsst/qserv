
#if 1

#include <iostream>
#include <string>
#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
using namespace std;

#define OPEN_UDP_PORT               0
#define SEND_DATA_TO_SERVER         1
#define RECEIVE_ANSWER_FROM_SERVER  2
#define CLOSE_UDP_PORT              3
#define RECEIVE_DATA               20
#define ERROR_HANDLING             10

using boost::asio::ip::udp;

class UDPClient
{
public:
    UDPClient(
        boost::asio::io_service& io_service,
        const std::string& host,
        const std::string& port
    ) : io_service_(io_service), socket_(io_service, udp::endpoint(udp::v4(), 0)) {
        udp::resolver resolver(io_service_);
        udp::resolver::query query(udp::v4(), host, port);
        udp::resolver::iterator iter = resolver.resolve(query);

        boost::asio::socket_base::broadcast option(true); // Socket option to permit broadcast messages

        socket_.set_option(option);
        endpoint_ = *iter;
    }

    ~UDPClient()
    {
        socket_.close();
    }

    void send(const std::string& msg) {
        socket_.send_to(boost::asio::buffer(msg, msg.size()), endpoint_);
    }

    bool isOpen()
    {

        if(socket_.is_open())
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    void connect_handler(const boost::system::error_code &error )
    {
        if(!error)
        {
            cout << "Everything went well" << endl;
        }
        if(error)
        {
            cout << "Something went wront" << endl;
        }
        else
        {
            cout << "nothing returned" << endl;
        }
    }

    void write_handler(const boost::system::error_code& error, std::size_t byte_transferred)
    {
        if(!error)
        {
            cout << "Everything went well" << endl;
        }
        if(error)
        {
            cout << "Something went wront" << endl;
        }
        else
        {
            cout << "nothing returned" << endl;
        }
    }

    void sendToServer(string scannerAddress, int port)
    {
        boost::asio::ip::udp::endpoint endpoint(boost::asio::ip::address::from_string(scannerAddress),port);
        socket_.async_connect(endpoint,boost::bind(&UDPClient::connect_handler,this,boost::asio::placeholders::error)); //async connect to server
        int data[3] = {-1,-1,17230};
        socket_.async_send(boost::asio::buffer(data),0, boost::bind(&UDPClient::write_handler,this, boost::asio::placeholders::error,sizeof(data))); //async send to server

    }

private:
    boost::asio::io_service& io_service_;
    udp::socket socket_;
    udp::endpoint endpoint_;
};


int main()
{
    int state = OPEN_UDP_PORT;
    string scannerAddress = "127.0.0.1"; // "10.48.37.183";

    boost::asio::io_service io_service;
    UDPClient client(io_service, "localhost", "10043" ); // Client created;

    while(true)
    {
        switch (state)
        {
        case OPEN_UDP_PORT:
        {
            cout << "hello world" << endl;
            if(client.isOpen()) {
                cout << "UDP connection open!" << endl;
                state = SEND_DATA_TO_SERVER;
            } else {
                cout << "UDP connection is not open!" << endl;
                state = ERROR_HANDLING;
            }
            break;
        }

        case SEND_DATA_TO_SERVER:
        {
            //cout << "Send data to server" << endl;
            //client.sendToServer(scannerAddress,9008);
            client.sendToServer(scannerAddress,10042);
            break;
        }
        case RECEIVE_ANSWER_FROM_SERVER:
        {
            cout << "hello world" << endl;
            break;
        }
        case RECEIVE_DATA:
        {
            cout << "hello world" << endl;
            break;
        }
        case CLOSE_UDP_PORT:
        {
            cout << "hello world" << endl;
            break;
        }
        case ERROR_HANDLING:
        {
            cout << "hello world" << endl;
            break;
        }
        }
    }

    return 0;
}

#else
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
    udp::endpoint receiver_endpoint =
      *resolver.resolve(udp::v4(), argv[1], "daytime").begin();

    udp::socket socket(io_context);
    socket.open(udp::v4());

    boost::array<char, 1> send_buf  = {{ 0 }};
    socket.send_to(boost::asio::buffer(send_buf), receiver_endpoint);

    boost::array<char, 128> recv_buf;
    udp::endpoint sender_endpoint;
    size_t len = socket.receive_from(
        boost::asio::buffer(recv_buf), sender_endpoint);

    std::cout.write(recv_buf.data(), len);
  }
  catch (std::exception& e)
  {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}


#endif
