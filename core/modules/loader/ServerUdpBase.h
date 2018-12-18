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
#ifndef LSST_QSERV_LOADER_SERVER_UDP_BASE_H
#define LSST_QSERV_LOADER_SERVER_UDP_BASE_H

// system headers
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/BufferUdp.h"

namespace lsst {
namespace qserv {
namespace loader {


/// This class provides a basic UDP server. Derived classes can identify messages
/// and take appropriate action.
class ServerUdpBase {
public:
    using Ptr = std::shared_ptr<ServerUdpBase>;

    // This constructor can throw boost::system::system_error
    ServerUdpBase(boost::asio::io_service& io_service, std::string const& host, int port);

    ServerUdpBase() = delete;
    ServerUdpBase(ServerUdpBase const&) = delete;
    ServerUdpBase& operator=(ServerUdpBase const&) = delete;

    virtual ~ServerUdpBase() = default;

    virtual BufferUdp::Ptr parseMsg(BufferUdp::Ptr const& data,
                                    boost::asio::ip::udp::endpoint const& endpoint);

    uint64_t getNextMsgId() { return _msgIdSeq++; }
    std::string getOurHostName() const { return _hostName; }
    int getOurPort() const { return _port; }
    uint32_t getErrCount() const { return _errCount; }

    /// This waits for the message to be sent before returning.
    void sendBufferTo(std::string const& host, int port, BufferUdp& sendBuf);
    boost::asio::ip::udp::endpoint resolve(std::string const& hostName, int port);

protected:
    std::atomic<uint32_t> _errCount{0};

private:
    void _receivePrepare(); ///< Give the io_service our callback for receiving.
    void _receiveCallback(const boost::system::error_code& error, size_t bytes_recvd);
    void _sendCallback(const boost::system::error_code& error, size_t bytes_sent);
    void _sendResponse(); ///< Send the contents of _sendData as a response;

    static std::atomic<uint64_t> _msgIdSeq; ///< Counter for unique message ids from this server.
    boost::asio::io_service& _ioService;
    boost::asio::ip::udp::socket _socket;
    boost::asio::ip::udp::endpoint _senderEndpoint;

    BufferUdp::Ptr _data; ///< data buffer for receiving
    BufferUdp::Ptr _sendData; ///< data buffer for sending.
    std::string _hostName;
    int _port;
};



}}} // namespace lsst:qserv:loader

#endif // LSST_QSERV_LOADER_SERVER_UDP_BASE_H
