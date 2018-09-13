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
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <cstdlib>
#include <iostream>
#include <set>

// Qserv headers
#include "loader/BufferUdp.h"


// &&& try to delete these
#include "loader/LoaderMsg.h"

namespace lsst {
namespace qserv {
namespace loader {

using boost::asio::ip::tcp; // &&& Using gotta go


class ServerTcpBase;

class TcpBaseConnection : public std::enable_shared_from_this<TcpBaseConnection> {
public:
    typedef std::shared_ptr<TcpBaseConnection> Ptr;

    static Ptr create(boost::asio::io_context& io_context, ServerTcpBase* tcpBase) {
        return Ptr(new TcpBaseConnection(io_context, tcpBase));
    }

    ~TcpBaseConnection() { shutdown(); }

    tcp::socket& socket() {
        return _socket;
    }

    void start();
    void shutdown();
    unsigned int getMsgStatus() const { return _msgStatus; }

private:
    TcpBaseConnection(boost::asio::io_context& io_context, ServerTcpBase* tcpBase) :
        _socket(io_context), _serverTcpBase(tcpBase) {}

    void _readKind(const boost::system::error_code&, size_t /*bytes_transferred*/);
    void _recvKind(const boost::system::error_code&, size_t bytesTrans);

    void _free();

    tcp::socket _socket;
    ServerTcpBase* _serverTcpBase; // _serverTcpBase controls this class' lifetime.
    BufferUdp _buf{500000};
    std::atomic<unsigned int> _msgStatus{LoaderMsg::WAITING};

    void _handleTest();
    void _handleTest2(const boost::system::error_code& ec, size_t bytesTrans);
    void _handleTest2b(const boost::system::error_code& ec, size_t bytesTrans);
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
        // &&& Is this thread safe? The server is expected to live until program termination.
        // &&& If a connection is doing something, and this is called, what happens?
        // &&& Check _connections empty before deleting?
        for (auto&& conn:_connections) {
            conn->shutdown();
        }
        _connections.clear();
    }

    void runThread() {
        auto func = [this]() {
            _io_context.run();
        };
        _threads.push_back(std::thread(func));
    }

    bool testConnect();

    void freeConnection(TcpBaseConnection::Ptr const& conn) {
        _connections.erase(conn);
    }

private:
    void startAccept() {
        TcpBaseConnection::Ptr newConnection = TcpBaseConnection::create(_acceptor.get_executor().context(), this);
        _acceptor.async_accept(newConnection->socket(),
                boost::bind(&ServerTcpBase::handleAccept, this, newConnection,
                        boost::asio::placeholders::error));
    }

    void handleAccept(TcpBaseConnection::Ptr newConnection,
            const boost::system::error_code& error) {
        if (!error) {
            _connections.insert(newConnection);
            newConnection->start();
        }
        startAccept();
    }

    bool _writeData(tcp::socket& socket, BufferUdp& data);

    boost::asio::io_context& _io_context;
    boost::asio::ip::tcp::acceptor _acceptor;
    int _port;
    std::vector<std::thread> _threads;
    std::set<TcpBaseConnection::Ptr> _connections;
};




}}} // namespace lsst:qserv:loader

#endif // LSST_QSERV_LOADER_SERVER_TCP_BASE_H_
