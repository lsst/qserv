// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
 */


// Class header
#include "loader/ServerTcpBase.h"

// System headers
#include <iostream>
#include <unistd.h>

// Third-party headers


// qserv headers
#include "loader/LoaderMsg.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.ServerTcpBase");

const int testNewNodeName = 73; // &&& Get rid of this, possibly make NodeName member of ServerTCPBase
unsigned int testNewNodeValuePairCount = 81;
const int testOldNodeName = 42; // &&& Get rid of this, possibly make NodeName member of ServerTCPBase
unsigned int testOldNodeKeyCount = 1231;
}

namespace lsst {
namespace qserv {
namespace loader {

#if 0 //  &&&
bool TcpServer::testConnect() {
    try
    {
        boost::asio::io_context io_context;

        /* &&&
        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints =
          resolver.resolve(argv[1], "daytime");
         */

        //boost::asio::ip::tcp::endpoint dest(boost::asio::ip::address::from_string("127.0.0.1"), _port);
        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints =
                  resolver.resolve("127.0.0.1", std::to_string(_port));

        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        for (;;) {
            LOGS(_log, LOG_LVL_INFO, "&&& testConnect");
            //boost::array<char, 128> buf;
            char buf[128];
            boost::system::error_code error;

            size_t len = socket.read_some(boost::asio::buffer(buf), error);

            if (error == boost::asio::error::eof) {
                break; // Connection closed cleanly by peer.
            } else if (error) {
                throw boost::system::system_error(error); // Some other error.
            }
            //std::cout.write(buf.data(), len);
            std::cout.write(buf, len);
            std::cout <<" ^that's it" <<std::endl;
        }
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
#endif


bool ServerTcpBase::_writeData(tcp::socket& socket, BufferUdp& data) {
    while (data.getBytesLeftToRead() > 0) {
        // Read cursor advances (manually in this case) as data is read from the buffer.
        auto res = boost::asio::write(socket,
                       boost::asio::buffer(data.getReadCursor(), data.getBytesLeftToRead()));

        data.advanceReadCursor(res);
    }

    return true;
}


bool ServerTcpBase::testConnect() {
    try
    {
        LOGS(_log, LOG_LVL_INFO, "&&& ServerTcpBase::testConnect 1");
        boost::asio::io_context io_context;

        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve("127.0.0.1", std::to_string(_port));

        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);


        // Get name from server
        BufferUdp data(500);
        auto msgElem = data.readFromSocket(socket, "ServerTcpBase::testConnect");
        // First element should be UInt32Element with the other worker's name
        UInt32Element::Ptr nghName = std::dynamic_pointer_cast<UInt32Element>(msgElem);
        if (nghName == nullptr) {
            throw LoaderMsgErr("ServerTcpBase::testConnect() first element wasn't correct type " +
                               msgElem->getStringVal(), __FILE__, __LINE__);
        }

        LOGS(_log, LOG_LVL_INFO, "server name=" << nghName->element);

        data.reset();
        UInt32Element kind(LoaderMsg::TEST);
        kind.appendToData(data);
        _writeData(socket, data);

        // send back our name and left neighbor message.
        data.reset();
        UInt32Element imRightKind(LoaderMsg::IM_YOUR_R_NEIGHBOR);
        imRightKind.appendToData(data);
        UInt32Element ourName(testNewNodeName);
        ourName.appendToData(data);
        UInt64Element valuePairCount(testNewNodeValuePairCount);
        valuePairCount.appendToData(data);
        _writeData(socket, data);

        // Get back left neighbor information
        auto msgKind = std::dynamic_pointer_cast<UInt32Element>(
                       data.readFromSocket(socket, "testConnect 2 kind"));
        auto msgLNName =  std::dynamic_pointer_cast<UInt32Element>(
                          data.readFromSocket(socket, "testConnect 2 LNName"));
        auto msgLKeyCount = std::dynamic_pointer_cast<UInt64Element>(
                            data.readFromSocket(socket, "testConnect 2 LKeyCount"));
        if (msgKind == nullptr || msgLNName == nullptr || msgLKeyCount == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, "ServerTcpBase::testConnect 2 - nullptr" <<
                  " msgKind=" << (msgKind ? "ok" : "null") <<
                  " msgLNName=" << (msgLNName ? "ok" : "null") <<
                  " msgLKeyCount=" << (msgLKeyCount ? "ok" : "null"));
            return false;
        }

        if (msgKind->element != LoaderMsg::IM_YOUR_L_NEIGHBOR ||
            msgLNName->element != testOldNodeName ||
            msgLKeyCount->element != testOldNodeKeyCount) {
            LOGS(_log, LOG_LVL_ERROR, "ServerTcpBase::testConnect 2 - incorrect data" <<
                                      " Kind=" << msgKind->element <<
                                      " LNName=" << msgLNName->element <<
                                      " LKeyCount=" << msgLKeyCount->element);
            return false;
        }
        LOGS(_log, LOG_LVL_INFO, "ServerTcpBase::testConnect 2 - ok data" <<
                                 " Kind=" << msgKind->element <<
                                 " LNName=" << msgLNName->element <<
                                 " LKeyCount=" << msgLKeyCount->element);

        data.reset();
        UInt32Element verified(LoaderMsg::NEIGHBOR_VERIFIED);
        verified.appendToData(data);
        _writeData(socket, data);

        boost::system::error_code ec;
        socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, "ServerTcpBase::testConnect shutdown ec=" << ec.message());
            return false;
        }
        // socket.close(); &&& should happen when socket falls out of scope
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}


void TcpBaseConnection::start() {
    /* &&&
    boost::asio::async_write(_socket, boost::asio::buffer(_message),
            boost::bind(&TcpBaseConnection::handleWrite, shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
    */
    //BufferUdp buf(100);
    UInt32Element name('j');
    name.appendToData(_buf);
    //UInt32Element kind(LoaderMsg::WORKER_RIGHT_NEIGHBOR); &&&
    //kind.appendToData(_buf); &&&
    boost::asio::async_write(_socket, boost::asio::buffer(_buf.getReadCursor(), _buf.getBytesLeftToRead()),
                    boost::bind(&TcpBaseConnection::_readKind, shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
}


void TcpBaseConnection::shutdown() {
    boost::system::error_code ec;
    _socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    _socket.close();
}


void TcpBaseConnection::_free() {
    _serverTcpBase->freeConnection(shared_from_this());
}


/// Find out what KIND of message is coming in.
void TcpBaseConnection::_readKind(const boost::system::error_code&, size_t /*bytes_transferred*/) {
    // &&&; need to read something
    _buf.reset();

    UInt32Element elem;
    size_t const bytes = elem.transmitSize();

    if (bytes > _buf.getAvailableWriteLength()) {
        /// &&& TODO close the connection
        LOGS(_log, LOG_LVL_ERROR, "_readKind Buffer would have overflowed");
        _free();
        return;
    }
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytes),
            boost::asio::transfer_at_least(bytes),
            boost::bind(
                    &TcpBaseConnection::_recvKind,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}


void TcpBaseConnection::_recvKind(const boost::system::error_code& ec, size_t bytesTrans) {
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind ec=" << ec);
        _free();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    auto msgElem = MsgElement::retrieve(_buf);
    auto msgKind = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (msgKind == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind unexpected type of msg");
        _free();
        return;
    }
    switch (msgKind->element) {
    case LoaderMsg::TEST:
        _msgStatus = LoaderMsg::TEST;
        LOGS(_log, LOG_LVL_INFO, "_recvKind TEST");
        _handleTest();
        break;
    case LoaderMsg::IM_YOUR_L_NEIGHBOR:
        LOGS(_log, LOG_LVL_INFO, "_recvKind IM_YOUR_L_NEIGHBOR");
        LOGS(_log, LOG_LVL_ERROR, "_recvKind IM_YOUR_L_NEIGHBOR NEEDS CODE!!!***!!!");
        break;
    default:
        LOGS(_log, LOG_LVL_ERROR, "_recvKind unexpected kind=" << msgKind->element);
        _free();
    }
}




void TcpBaseConnection::_handleTest() {
    // &&&; need to read something
    _buf.reset();

    UInt32Element kind;
    UInt32Element rNName;
    UInt64Element valuePairCount;
    size_t bytes = kind.transmitSize() + rNName.transmitSize() + valuePairCount.transmitSize();

    if (bytes > _buf.getAvailableWriteLength()) {
        /// &&& TODO close the connection
        LOGS(_log, LOG_LVL_ERROR, "_handleTest Buffer would have overflowed");
        _free();
        return;
    }
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytes),
            boost::asio::transfer_at_least(bytes),
            boost::bind(
                    &TcpBaseConnection::_handleTest2,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}


void TcpBaseConnection::_handleTest2(const boost::system::error_code& ec, size_t bytesTrans) {
    if (ec) {
         LOGS(_log, LOG_LVL_ERROR, "_recvKind ec=" << ec);
         _free();
         return;
     }
     // Fix the buffer with the information given.
     _buf.advanceWriteCursor(bytesTrans);
     auto msgElem = MsgElement::retrieve(_buf);
     auto msgKind = std::dynamic_pointer_cast<UInt32Element>(msgElem);
     msgElem = MsgElement::retrieve(_buf);
     auto msgName = std::dynamic_pointer_cast<UInt32Element>(msgElem);
     msgElem = MsgElement::retrieve(_buf);
     auto msgKeys = std::dynamic_pointer_cast<UInt64Element>(msgElem);


     // test that this is the neighbor that was expected. (&&& this test should be done by CentralWorker)
     if (msgKind->element != LoaderMsg::IM_YOUR_R_NEIGHBOR ||
         msgName->element != testNewNodeName ||
         msgKeys->element != testNewNodeValuePairCount)  {
         LOGS(_log, LOG_LVL_ERROR, "_handleTest2 unexpected element or name" <<
              " kind=" << msgKind->element << " msgName=" << msgName->element <<
              " keys=" << msgKeys->element);
         _free();
         return;
     } else {
         LOGS(_log, LOG_LVL_INFO, "_handleTest2 kind=" << msgKind->element << " msgName="
              << msgName->element << " keys=" << msgKeys->element);
     }

     // send im_left_neighbor message, how many elements we have. If it had zero elements, an element will be sent
     // so that new neighbor gets a range.
     _buf.reset();
     // build the protobuffer
     msgKind = std::make_shared<UInt32Element>(LoaderMsg::IM_YOUR_L_NEIGHBOR);
     msgKind->appendToData(_buf);
     UInt32Element ourName(testOldNodeName);
     ourName.appendToData(_buf);
     UInt64Element keyCount(testOldNodeKeyCount);
     keyCount.appendToData(_buf);
     boost::asio::async_write(_socket, boost::asio::buffer(_buf.getReadCursor(), _buf.getBytesLeftToRead()),
            boost::bind(&TcpBaseConnection::_handleTest2b, shared_from_this(),
              boost::asio::placeholders::error,
              boost::asio::placeholders::bytes_transferred));

}


void TcpBaseConnection::_handleTest2b(const boost::system::error_code& ec, size_t bytesTrans) {
    // get verified message and close connection
    // UInt32Element verified(LoaderMsg::NEIGHBOR_VERIFIED); &&&
    if (ec) {
         LOGS(_log, LOG_LVL_ERROR, "_recvKind ec=" << ec);
         return;
     }
     // Fix the buffer with the information given.
     _buf.advanceWriteCursor(bytesTrans);
     auto msgElem = MsgElement::retrieve(_buf);
     if (msgElem == nullptr) {
         LOGS(_log, LOG_LVL_ERROR, "_handleTest2b Kind nullptr error");
         return;
     }
     auto msgKind = std::dynamic_pointer_cast<UInt32Element>(msgElem);
     if (msgKind->element != LoaderMsg::NEIGHBOR_VERIFIED) {
         LOGS(_log, LOG_LVL_ERROR, "_handleTest2b NEIGHBOR_VERIFIED error" <<
              " kind=" << msgKind->element);
         return;
     }
     _msgStatus = LoaderMsg::NEIGHBOR_VERIFIED;
}



}}} // namespace lsst::qserrv::loader



