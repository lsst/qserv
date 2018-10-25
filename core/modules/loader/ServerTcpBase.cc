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
#include "loader/CentralWorker.h"
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


bool ServerTcpBase::_writeData(tcp::socket& socket, BufferUdp& data) {
    while (data.getBytesLeftToRead() > 0) {
        // Read cursor advances (manually in this case) as data is read from the buffer.
        auto res = boost::asio::write(socket,
                       boost::asio::buffer(data.getReadCursor(), data.getBytesLeftToRead()));

        data.advanceReadCursor(res);
    }
    return true;
}


uint32_t ServerTcpBase::getOurName() {
    return (_centralWorker == nullptr) ? 0 : _centralWorker->getOurName();
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
        UInt32Element bytes(1234); // dummy value
        bytes.appendToData(data);
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
    uint32_t ourName = _serverTcpBase->getOurName();
    UInt32Element name(ourName);
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


void TcpBaseConnection::_freeConnect() {
    auto centralW = _serverTcpBase->getCentralWorker();
    if (centralW != nullptr) {
        centralW->cancelShiftsFromRightNeighbor();
    }
    _serverTcpBase->freeConnection(shared_from_this());
}


/// Find out what KIND of message is coming in.
void TcpBaseConnection::_readKind(boost::system::error_code const&, size_t /*bytes_transferred*/) {
    _buf.reset();

    UInt32Element elem;
    size_t const bytes = 2*elem.transmitSize(); // uint32 for kind + uint32 for length of message

    if (bytes > _buf.getAvailableWriteLength()) {
        /// &&& TODO close the connection
        LOGS(_log, LOG_LVL_ERROR, "_readKind Buffer would have overflowed");
        _freeConnect();
        return;
    }

    LOGS(_log, LOG_LVL_INFO, "&&& TcpBaseConnection::_readKind _recvKind reset _buf=" << _buf.dumpStr());
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
    LOGS(_log, LOG_LVL_INFO, "&&& TcpBaseConnection::_recvKind bytes=" << bytesTrans << " ec=" << ec << " " << _buf.dumpStr());
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind ec=" << ec);
        _freeConnect();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    //LOGS(_log, LOG_LVL_INFO, "&&& TcpBaseConnection::_recvKind _buf=" << _buf.dumpStr());
    auto msgElem = MsgElement::retrieve(_buf);
    auto msgKind = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (msgKind == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind unexpected type of msg");
        _freeConnect();
        return;
    }
    //LOGS(_log, LOG_LVL_INFO, "&&& msgKind=" << msgKind->element);
    msgElem = MsgElement::retrieve(_buf);
    auto msgBytes = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (msgBytes == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind missing bytes");
        _freeConnect();
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "_recvKind kind=" << msgKind->element << " bytes=" << msgBytes->element);
    switch (msgKind->element) {
    case LoaderMsg::TEST:
        LOGS(_log, LOG_LVL_INFO, "_recvKind TEST");
        _handleTest();
        break;
    case LoaderMsg::IM_YOUR_L_NEIGHBOR:
        LOGS(_log, LOG_LVL_INFO, "_recvKind IM_YOUR_L_NEIGHBOR");
        _handleImYourLNeighbor(msgBytes->element);
        break;
    case LoaderMsg::SHIFT_TO_RIGHT:
        LOGS(_log, LOG_LVL_INFO, "_recvKind SHIFT_TO_RIGHT our left neighbor is shifting to us");
        _handleShiftToRight(msgBytes->element);
        break;
    case LoaderMsg::SHIFT_FROM_RIGHT:
        LOGS(_log, LOG_LVL_INFO, "_recvKind SHIFT_FROM_RIGHT our left neighbor needs keys shifted from this");
        _handleShiftFromRight(msgBytes->element);
        break;
    default:
        LOGS(_log, LOG_LVL_ERROR, "_recvKind unexpected kind=" << msgKind->element);
        _freeConnect();
    }
}




void TcpBaseConnection::_handleTest() {
    // &&&; need to read something
    _buf.reset();  // TODO - really shouldn't reset the buffer, it's possible something useful is in it.

    UInt32Element kind;
    UInt32Element rNName;
    UInt64Element valuePairCount;
    size_t bytes = kind.transmitSize() + rNName.transmitSize() + valuePairCount.transmitSize();

    if (bytes > _buf.getAvailableWriteLength()) {
        /// &&& TODO close the connection
        LOGS(_log, LOG_LVL_ERROR, "_handleTest Buffer would have overflowed");
        _freeConnect();
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
         _freeConnect();
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
         _freeConnect();
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
    UInt32Element kind;
    size_t bytes = kind.transmitSize();
    _buf.reset();
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytes),
            boost::asio::transfer_at_least(bytes),
            boost::bind(
                    &TcpBaseConnection::_handleTest2c,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}


void TcpBaseConnection::_handleTest2c(const boost::system::error_code& ec, size_t bytesTrans) {
    // get verified message and close connection
    // UInt32Element verified(LoaderMsg::NEIGHBOR_VERIFIED); &&&
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, "_recvKind ec=" << ec);
        _freeConnect();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    auto msgElem = MsgElement::retrieve(_buf);
    if (msgElem == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "_handleTest2b Kind nullptr error");
        _freeConnect();
        return;
    }
    auto msgKind = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (msgKind != nullptr && msgKind->element != LoaderMsg::NEIGHBOR_VERIFIED) {
        LOGS(_log, LOG_LVL_ERROR, "_handleTest2b NEIGHBOR_VERIFIED error" <<
                " kind=" << msgKind->element);
        _freeConnect();
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "TcpBaseConnection::_handleTest SUCCESS");
    _freeConnect(); // Close the connection at the end of the test.
}


void TcpBaseConnection::_handleImYourLNeighbor(uint32_t bytesInMsg) {
    // Need to figure out the difference between bytes read and bytes in _buf
    if (bytesInMsg > _buf.getAvailableWriteLength()) {
        /// &&& TODO close the connection
        LOGS(_log, LOG_LVL_ERROR, "_handleImYourLNeighbor Buffer would have overflowed");
        _freeConnect();
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "&&& _handleImYourLNeighbor bytes=" << bytesInMsg << " buf=" << _buf.dumpStr(false));
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytesInMsg),
            boost::asio::transfer_at_least(bytesInMsg),
            boost::bind(
                    &TcpBaseConnection::_handleImYourLNeighbor1,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}

void TcpBaseConnection::_handleImYourLNeighbor1(boost::system::error_code const& ec, size_t bytesTrans) {
    std::string const funcName = "_handleImYourLNeighbor2";
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, funcName << " ec=" << ec);
        _freeConnect();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    LOGS(_log, LOG_LVL_INFO, funcName << "&&& bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
    try {
        // &&& TODO move as much of this to CentralWorker as possible
        LOGS(_log, LOG_LVL_INFO, funcName <<  "&&& parsing bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
        auto protoItem = StringElement::protoParse<proto::WorkerKeysInfo>(_buf);
        if (protoItem == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }
        NeighborsInfo nInfo;
        auto workerName = protoItem->name();
        nInfo.keyCount = protoItem->mapsize();
        nInfo.recentAdds = protoItem->recentadds();
        proto::WorkerRangeString protoRange = protoItem->range();
        LOGS(_log, LOG_LVL_INFO, "&&& MasterServer WorkerKeysInfo aaaaa name=" << workerName << " keyCount=" << nInfo.keyCount << " recentAdds=" << nInfo.recentAdds);
        bool valid = protoRange.valid();
        StringRange leftRange;
        StringRange newLeftRange;
        if (valid) {
            std::string min   = protoRange.min();
            std::string max   = protoRange.max();
            bool unlimited = protoRange.maxunlimited();
            leftRange.setMinMax(min, max, unlimited);
            LOGS(_log, LOG_LVL_WARN, "&&& TcpBaseConnection::_handleImYourLNeighbor2  leftRange=" << leftRange);
            newLeftRange = _serverTcpBase->getCentralWorker()->updateLeftNeighborRange(leftRange);
        }
        proto::Neighbor protoLeftNeigh = protoItem->left();
        nInfo.neighborLeft->update(protoLeftNeigh.name());  // Not really useful in this case.
        proto::Neighbor protoRightNeigh = protoItem->right();
        nInfo.neighborRight->update(protoRightNeigh.name()); // This should be our name
        if (nInfo.neighborRight->get() != _serverTcpBase->getOurName()) {
            LOGS(_log, LOG_LVL_ERROR, "Our (" << _serverTcpBase->getOurName() <<
                                      ") left neighbor does not have our name as its right neighbor" );
        }

        _serverTcpBase->getCentralWorker()->setNeighborInfoLeft(workerName, nInfo.keyCount, newLeftRange);

        // Need to send newLeftRange back to left neighbor so it can figure out what to do with its range.
        _buf.reset();
        StringElement strElem;
        std::unique_ptr<proto::WorkerKeysInfo> protoWKI = _serverTcpBase->getCentralWorker()->_workerKeysInfoBuilder();
        protoWKI->SerializeToString(&(strElem.element));
        UInt32Element bytesInMsg(strElem.transmitSize());
        // Must send the number of bytes in the message so TCP client knows how many bytes to read.
        bytesInMsg.appendToData(_buf);
        strElem.appendToData(_buf);
        ServerTcpBase::_writeData(_socket, _buf);
        LOGS(_log, LOG_LVL_INFO, funcName << "&&& done");
    } catch (LoaderMsgErr &msgErr) {
        LOGS(_log, LOG_LVL_ERROR, funcName << " Buffer failed " << msgErr.what());
        _freeConnect();
        return;
    }
    boost::system::error_code ecode;
    _readKind(ecode, 0); // get next message
}


// Our left neighbor is shifting key value pairs to this.
void TcpBaseConnection::_handleShiftToRight(uint32_t bytesInMsg) {
    // Need to figure out the difference between bytes read and bytes in _buf
    if (bytesInMsg > _buf.getAvailableWriteLength()) {
        LOGS(_log, LOG_LVL_ERROR, "_handleShiftToRight Buffer would have overflowed bytes=" << bytesInMsg);
        _freeConnect();
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "&&&ggggS _handleShiftToRight bytes=" << bytesInMsg << " buf=" << _buf.dumpStr(false));
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytesInMsg),
            boost::asio::transfer_at_least(bytesInMsg),
            boost::bind(
                    &TcpBaseConnection::_handleShiftToRight1,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}


void TcpBaseConnection::_handleShiftToRight1(boost::system::error_code const& ec, size_t bytesTrans) {
    std::string const funcName = "_handleShiftToRight1";
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, funcName << " ec=" << ec);
        _freeConnect();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    LOGS(_log, LOG_LVL_INFO, funcName << "&&&ggggS bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
    try {
        // &&& TODO move as much of this to CentralWorker as possible
        LOGS(_log, LOG_LVL_INFO, funcName << "&&&ggggS parsing bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
        auto protoKeyList = StringElement::protoParse<proto::KeyList>(_buf);
        if (protoKeyList == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }
        // Extract key pairs from the protobuffer
        int keyCount = protoKeyList->keycount(); // &&& this could be redundant
        int sz = protoKeyList->keypair_size();
        if (keyCount != sz) {
            LOGS(_log, LOG_LVL_WARN, funcName << " keyCount(" << keyCount << ") != sz(" << sz << ")");
        }
        std::vector<CentralWorker::StringKeyPair> keyList;
        for (int j=0; j < sz; ++j) {
            proto::KeyInfo const& protoKI = protoKeyList->keypair(j);
            ChunkSubchunk chSub(protoKI.chunk(), protoKI.subchunk());
            keyList.push_back(std::make_pair(protoKI.key(), chSub));
        }

        // Now that the proto buffer was read without error, insert into map and adjust our range.
        _serverTcpBase->getCentralWorker()->insertKeys(keyList, true);

        // Send the SHIFT_TO_RIGHT_KEYS_RECEIVED response back.
        _buf.reset();
        UInt32Element elem(LoaderMsg::SHIFT_TO_RIGHT_RECEIVED);
        elem.appendToData(_buf);
        ServerTcpBase::_writeData(_socket, _buf);
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&ggggS done dumpKeys " <<
                                             _serverTcpBase->getCentralWorker()->dumpKeysStr(3));
    } catch (LoaderMsgErr &msgErr) {
        LOGS(_log, LOG_LVL_ERROR, msgErr.what());
        LOGS(_log, LOG_LVL_ERROR, funcName << " key shift failed");
        _freeConnect();
        return;
    }
    boost::system::error_code ecode;
    _readKind(ecode, 0); // get next message
}

// Our left neighbor wants this node to shift key value pairs to it.
void TcpBaseConnection::_handleShiftFromRight(uint32_t bytesInMsg) {
    // Need to figure out the difference between bytes read and bytes in _buf
    if (bytesInMsg > _buf.getAvailableWriteLength()) {
        LOGS(_log, LOG_LVL_ERROR, "_handleShiftToRight Buffer would have overflowed bytes=" << bytesInMsg);
        _freeConnect();
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "&&&hhhhS _handleShiftFromRight bytes=" << bytesInMsg << " buf=" << _buf.dumpStr(false));
    boost::asio::async_read(_socket, boost::asio::buffer(_buf.getWriteCursor(), bytesInMsg),
            boost::asio::transfer_at_least(bytesInMsg),
            boost::bind(
                    &TcpBaseConnection::_handleShiftFromRight1,
                    shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred
            )
    );
}


void TcpBaseConnection::_handleShiftFromRight1(boost::system::error_code const& ec, size_t bytesTrans) {
    std::string const funcName = "_handleShiftFromRight1";
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, funcName << " ec=" << ec);
        _freeConnect();
        return;
    }
    // Fix the buffer with the information given.
    _buf.advanceWriteCursor(bytesTrans);
    LOGS(_log, LOG_LVL_INFO, funcName << "&&&hhhhS bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
    try {
        // &&& TODO move as much of this to CentralWorker as possible
        LOGS(_log, LOG_LVL_INFO, funcName << "&&&hhhhS parsing bytes=" << bytesTrans << " _buf" << _buf.dumpStr(false));
        auto protoKeyShiftReq = StringElement::protoParse<proto::KeyShiftRequest>(_buf);
        if (protoKeyShiftReq == nullptr) {
            throw LoaderMsgErr(funcName + " KeyShiftRequest parse failure ", __FILE__, __LINE__);
        }
        // Extract keysToShift from the protobuffer
        int keyShiftReq = protoKeyShiftReq->keystoshift();
        if (keyShiftReq < 1) {
            throw LoaderMsgErr(funcName + " KeyShiftRequest for < 1 key", __FILE__, __LINE__);
        }
        // Build and send the KeyList message back (send smallest keys to right node)
        /* &&&
        StringElement::UPtr keyList = _serverTcpBase->getCentralWorker()->buildKeyList(keyShiftReq);
        UInt32Element kindShiftFromRight(LoaderMsg::SHIFT_FROM_RIGHT);
        auto keyListTransmitSz = keyList->transmitSize();
        UInt32Element bytesInMsg(keyListTransmitSz);
        BufferUdp data(kindShiftFromRight.transmitSize() + bytesInMsg.transmitSize() + keyListTransmitSz);
        kindShiftFromRight.appendToData(data);
        bytesInMsg.appendToData(data);
        keyList->appendToData(data);
        */
        StringElement::UPtr keyList = _serverTcpBase->getCentralWorker()->buildKeyList(keyShiftReq);
        auto keyListTransmitSz = keyList->transmitSize();
        BufferUdp data(keyListTransmitSz);
        keyList->appendToData(data);
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhC 8");
        ServerTcpBase::_writeData(_socket, data);


        // Wait for the SHIFT_FROM_RIGHT_KEYS_RECEIVED response back.
        _buf.reset();
        auto msgElem = _buf.readFromSocket(_socket, funcName +" waiting for SHIFT_FROM_RIGHT_KEYS_RECEIVED");
        UInt32Element::Ptr received = std::dynamic_pointer_cast<UInt32Element>(msgElem);
        if (received == nullptr || received->element !=  LoaderMsg::SHIFT_FROM_RIGHT_RECEIVED) {
            LOGS(_log, LOG_LVL_INFO, funcName << " did not get SHIFT_FROM_RIGHT_RECEIVED");
            throw new LoaderMsgErr(funcName + " receive failure");
        }
        _serverTcpBase->getCentralWorker()->finishShiftFromRight();
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhS done dumpKeys " <<
                                 _serverTcpBase->getCentralWorker()->dumpKeysStr(3));
    } catch (LoaderMsgErr &msgErr) {
        LOGS(_log, LOG_LVL_ERROR, msgErr.what());
        LOGS(_log, LOG_LVL_ERROR, funcName << " key shift failed");
        _freeConnect();
        return;
    }
    boost::system::error_code ecode;
    _readKind(ecode, 0); // get next message
}


}}} // namespace lsst::qserrv::loader



