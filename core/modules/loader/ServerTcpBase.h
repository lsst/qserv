// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 *
 */
#ifndef LSST_QSERV_LOADER_SERVER_TCP_BASE_H_
#define LSST_QSERV_LOADER_SERVER_TCP_BASE_H_

// system headers
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/BufferUdp.h"


// &&& try to delete these
#include "loader/LoaderMsg.h"

namespace lsst {
namespace qserv {
namespace loader {

using boost::asio::ip::tcp; // &&& Using gotta go

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    typedef std::shared_ptr<TcpConnection> Ptr;

    static Ptr create(boost::asio::io_context& io_context) {
        return Ptr(new TcpConnection(io_context));
    }

    tcp::socket& socket() {
        return _socket;
    }

    void start() {
        _message = makeDaytimeString();

        boost::asio::async_write(_socket, boost::asio::buffer(_message),
                boost::bind(&TcpConnection::handleWrite, shared_from_this(),
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred));
    }

    std::string makeDaytimeString() {
        using namespace std; // For time_t, time and ctime;
        time_t now = time(0);
        return ctime(&now);
    }

private:
    TcpConnection(boost::asio::io_context& io_context) : _socket(io_context) {}

    void handleWrite(const boost::system::error_code&, size_t /*bytes_transferred*/) {}

    tcp::socket _socket;
    std::string _message;
};


class TcpServer {
public:
  TcpServer(boost::asio::io_context& io_context, int port) :
      _io_context(io_context), _acceptor(io_context, tcp::endpoint(tcp::v4(), port)), _port(port) {
      startAccept();
  }

  ~TcpServer() {
      _io_context.stop();
      for (std::thread& t : _threads) {
          t.join();
      }
  }

  void runThread() {
      auto func = [this]() {
          _io_context.run();
      };

      _threads.push_back(std::thread(func));
  }

  bool testConnect();

private:
  void startAccept() {
    TcpConnection::Ptr newConnection = TcpConnection::create(_acceptor.get_executor().context());
    _acceptor.async_accept(newConnection->socket(),
        boost::bind(&TcpServer::handleAccept, this, newConnection,
          boost::asio::placeholders::error));
  }

  void handleAccept(TcpConnection::Ptr newConnection,
      const boost::system::error_code& error) {
    if (!error) {
      newConnection->start();
    }
    startAccept();
  }

  boost::asio::io_context& _io_context;
  boost::asio::ip::tcp::acceptor _acceptor;
  int _port;
  std::vector<std::thread> _threads;
};


class TcpBaseConnection : public std::enable_shared_from_this<TcpBaseConnection> {
public:
    typedef std::shared_ptr<TcpBaseConnection> Ptr;

    static Ptr create(boost::asio::io_context& io_context) {
        return Ptr(new TcpBaseConnection(io_context));
    }

    tcp::socket& socket() {
        return _socket;
    }

    void start();

private:
    TcpBaseConnection(boost::asio::io_context& io_context) : _socket(io_context) {}

    void _readKind(const boost::system::error_code&, size_t /*bytes_transferred*/);
    void _recvKind(const boost::system::error_code&, size_t bytesTrans);


    tcp::socket _socket;
    BufferUdp _buf{500000};

    void _handleTest();
    void _handleTest2(const boost::system::error_code& ec, size_t bytesTrans);
};



class ServerTcpBase {
public:
    ServerTcpBase(boost::asio::io_context& io_context, int port) :
        _io_context(io_context), _acceptor(io_context, tcp::endpoint(tcp::v4(), port)), _port(port) {
        startAccept();
    }

    ~ServerTcpBase() {
        _io_context.stop();
        for (std::thread& t : _threads) {
            t.join();
        }
    }

    void runThread() {
        auto func = [this]() {
            _io_context.run();
        };
        _threads.push_back(std::thread(func));
    }

    bool testConnect();

private:
    void startAccept() {
        TcpBaseConnection::Ptr newConnection = TcpBaseConnection::create(_acceptor.get_executor().context());
        _acceptor.async_accept(newConnection->socket(),
                boost::bind(&ServerTcpBase::handleAccept, this, newConnection,
                        boost::asio::placeholders::error));
    }

    void handleAccept(TcpBaseConnection::Ptr newConnection,
            const boost::system::error_code& error) {
        if (!error) {
            newConnection->start();
        }
        startAccept();
    }

    bool _writeData(tcp::socket& socket, BufferUdp& data);

    boost::asio::io_context& _io_context;
    boost::asio::ip::tcp::acceptor _acceptor;
    int _port;
    std::vector<std::thread> _threads;
};




}}} // namespace lsst:qserv:loader

#endif // LSST_QSERV_LOADER_SERVER_TCP_BASE_H_
