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
#include "CentralWorker.h"

// system headers
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers


// qserv headers
#include "loader/BufferUdp.h"
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"


// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.CentralWorker");
}

namespace lsst {
namespace qserv {
namespace loader {


CentralWorker::CentralWorker(boost::asio::io_service& ioService,
    std::string const& masterHostName,   int masterPort,
    std::string const& hostName,         int udpPort,
    boost::asio::io_context& io_context, int tcpPort)  /// &&& move tcpPort next to udpPort
    : Central(ioService, masterHostName, masterPort),
      _hostName(hostName), _udpPort(udpPort),
      _ioContext(io_context), _tcpPort(tcpPort) {

    _server = std::make_shared<WorkerServer>(_ioService, _hostName, _udpPort, this);
    _tcpServer = std::make_shared<ServerTcpBase>(_ioContext, _tcpPort, this);
    _tcpServer->runThread();
    _startMonitoring();
}

CentralWorker::~CentralWorker() {
    _wWorkerList.reset();
    _tcpServer.reset();
}


std::string CentralWorker::getOurLogId() {
    std::stringstream os;
    os << "(w name=" << _ourName << " addr=" << _hostName <<
            ":udp=" << _udpPort << " tcp=" << _tcpPort << ")";
    return os.str();
}

void CentralWorker::_startMonitoring() {
    // Add _workerList to _doList so it starts checking new entries.
    // LOGS(_log, LOG_LVL_INFO, "&&& CentralWorker::_monitorWorkers()");
    _centralWorkerDoListItem = std::make_shared<CentralWorkerDoListItem>(this);
    _doList.addItem(_wWorkerList);
    _doList.addItem(_centralWorkerDoListItem);
}


void CentralWorker::_monitor() {
    LOGS(_log, LOG_LVL_INFO, "&&& CentralWorker::_monitor");
    // If data gets shifted, check everything again as ranges will have
    // changed and there may be a lot more data to shift.
    bool dataShifted = false;
    do {
        // TODO Check if we've heard from left neighbor (possibly kill connection if nothing heard???)

        // Check the right neighbor connection, kill and restart if needed.
        // Check if data needs to be shifted with the right node
        std::lock_guard<std::mutex> lck(_rightMtx); // This mutex is locked for a long time &&&
        LOGS(_log, LOG_LVL_INFO, "_monitor " << _ourName << " checking right neighbor " << _neighborRight.getName());
        if (_neighborRight.getName() != 0) {
            try {
                if (not _neighborRight.getEstablished()) {
                    LOGS(_log, LOG_LVL_INFO, "_monitor " << _ourName << " trying to connect");
                    auto nAddr = _neighborRight.getAddressTcp();
                    if (nAddr.ip == "") {
                        // look up the network address for the rightNeighbor
                        WWorkerListItem::Ptr nWorker = _wWorkerList->getWorkerNamed(_neighborRight.getName());
                        if (nWorker != nullptr) {
                            auto addr = nWorker->getAddressTcp();
                            auto addrUdp = nWorker->getAddressUdp();
                            LOGS(_log, LOG_LVL_INFO, "_monitor neighbor right " << _neighborRight.getName() <<
                                    " T=" << addr << " U=" << addrUdp);
                            _neighborRight.setAddressTcp(addr);
                            _neighborRight.setAddressUdp(addrUdp);
                        }
                    }

                    LOGS(_log, LOG_LVL_INFO, "_monitor trying to establish TCP connection with " <<
                            _neighborRight.getName() << " " << _neighborRight.getAddressTcp());
                    _rightConnect(); // calls _determineRange() while establishing connection
                } else {
                    LOGS(_log, LOG_LVL_INFO, "_monitor " << _ourName << " getting range info");
                    _determineRange();
                }
                dataShifted = _shiftIfNeeded();
            } catch (LoaderMsgErr const& ex) {
                LOGS(_log, LOG_LVL_WARN, "_monitor() " << ex.what());
                _rightDisconnect();
            }
        } else {
            // If there is a connection, close it.
            _rightDisconnect();
        }
    } while (dataShifted);
}


void CentralWorker::_determineRange() {
    std::string const funcName("CentralWorker::_determineRange");
    BufferUdp data(2000);
    {
        data.reset();
        UInt32Element imLeftKind(LoaderMsg::IM_YOUR_L_NEIGHBOR);
        imLeftKind.appendToData(data);
        StringElement strElem;
        std::unique_ptr<proto::WorkerKeysInfo> protoWKI = _workerKeysInfoBuilder();
        protoWKI->SerializeToString(&(strElem.element));
        UInt32Element bytesInMsg(strElem.transmitSize());
        // Must send the number of bytes in the message so TCP server knows how many bytes to read.
        bytesInMsg.appendToData(data);
        strElem.appendToData(data);
        ServerTcpBase::_writeData(*_rightSocket, data);
    }
    // Get back their basic info
    {
        data.reset();
        auto msgElem = data.readFromSocket(*_rightSocket, funcName + " - range");
        LOGS(_log, LOG_LVL_INFO, funcName << "&&&  parsing data=" << data.dumpStr());
        auto protoItem = StringElement::protoParse<proto::WorkerKeysInfo>(data); // shouldn't this be looking at msgElem &&& ???
                                                                                 // probably getting lucky that this is in the buffer &&&
        if (protoItem == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, "CentralWorker::_determineRange protoItem parse issue!!!!!");
            exit(-1); // &&& remove after checking readFromSocket and parse data, the code looks wrong.
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }
        NeighborsInfo nInfoR;
        auto workerName = protoItem->name();
        nInfoR.keyCount = protoItem->mapsize();
        _neighborRight.setKeyCount(nInfoR.keyCount); // TODO add a timestamp to this data.
        nInfoR.recentAdds = protoItem->recentadds();
        proto::WorkerRangeString protoRange = protoItem->range();
        LOGS(_log, LOG_LVL_INFO, funcName << "&&& rightNeighbor name=" << workerName << " keyCount=" << nInfoR.keyCount << " recentAdds=" << nInfoR.recentAdds);
        bool valid = protoRange.valid();
        StringRange rightRange;
        if (valid) {
            std::string min   = protoRange.min();
            std::string max   = protoRange.max();
            bool unlimited = protoRange.maxunlimited();
            rightRange.setMinMax(min, max, unlimited);
            LOGS(_log, LOG_LVL_INFO, funcName << "&&& rightRange=" << rightRange);
            _neighborRight.setRange(rightRange);
            // Adjust our max range given the the right minimum information.
            {
                std::lock_guard<std::mutex> lckMap(_idMapMtx);
                // Can't be unlimited anymore
                _strRange.setMax(min, false);
            }
        }
        proto::Neighbor protoLeftNeigh = protoItem->left();
        nInfoR.neighborLeft->update(protoLeftNeigh.name());  // Not really useful in this case.
        proto::Neighbor protoRightNeigh = protoItem->right();
        nInfoR.neighborRight->update(protoRightNeigh.name()); // This should be our name
        if (nInfoR.neighborLeft->get() != getOurName()) {
            LOGS(_log, LOG_LVL_ERROR, "Our (" << getOurName() <<
                    ") right neighbor does not have our name as its left neighbor" );
        }
    }

}


// must hold _rightMtx before calling
bool CentralWorker::_shiftIfNeeded() {
    // There should be reasonably recent information from our neighbors. Use that
    // and our status to ask the right neighbor to give us entries or we send entries
    // to the right neighbor.
    // If right connection is not established, return
    if (not _neighborRight.getEstablished()) {
        LOGS(_log, LOG_LVL_INFO, "_shiftIfNeeded no right neighbor, no shift.");
        return false;
    }
    if (_shiftWithRightInProgress) {
        LOGS(_log, LOG_LVL_INFO, "_shiftIfNeeded shift already in progress.");
        return false;
    }

    // Get local copies of range and map info.
    StringRange range;
    size_t mapSize;
    {
        std::lock_guard<std::mutex> lck(_idMapMtx);
        range = _strRange;
        mapSize = _directorIdMap.size();
    }

    // If this worker has more keys than the rightNeighbor, consider shifting keys to the right neighbor.
        // If this worker has _thresholdAverage more keys than average or _thresholdNeighborShift more keys than the right neighbor
            // send enough keys to the right to balance (min 1 key, max _maxShiftKeys, never shift more than 1/3 of our keys)
    int rightKeyCount = 0;
    StringRange rightRange;
    _neighborRight.getKeyData(rightKeyCount, rightRange);
    if (range > rightRange) {
        LOGS(_log, LOG_LVL_ERROR, "Right neighbor range is less than ours!!!! our=" << range << " right=" << rightRange);
        return false;
    }
    int keysToShift = 0;
    CentralWorker::Direction direction = NONE0;
    int sourceSize = 0;
    LOGS(_log, LOG_LVL_INFO, "_shiftIfNeeded _monitor thisSz=" << mapSize << " rightSz=" << rightKeyCount);
    if (mapSize > rightKeyCount*_thresholdNeighborShift) { // TODO add average across workers check
        keysToShift = (mapSize - rightKeyCount)/2; // Try for nearly equal number of keys on each.
        direction = TORIGHT1;
        sourceSize = mapSize;
    } else if (mapSize*_thresholdNeighborShift < rightKeyCount) { // TODO add average across workers check
        keysToShift = (rightKeyCount - mapSize)/2; // Try for nearly equal number of keys on each.
        direction = FROMRIGHT2;
        sourceSize = rightKeyCount;
    }
    if (keysToShift > _maxKeysToShift) keysToShift = _maxKeysToShift;
    if (keysToShift > sourceSize/3) keysToShift = sourceSize/3;
    if (keysToShift < 1) {
        LOGS(_log, LOG_LVL_WARN, "Worker doesn't have enough keys to shift.");
        return false;
    }
    _shiftWithRightInProgress = true;
    LOGS(_log, LOG_LVL_INFO, "shift dir(TO1 FROM2)=" << direction << " keys=" << keysToShift <<
                             " szThis=" << mapSize << " szRight=" << rightKeyCount);
    _shift(direction, keysToShift);
    return true;
}


void CentralWorker::_shift(Direction direction, int keysToShift) {
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 1");
    if (direction == FROMRIGHT2) {
        BufferUdp data(1000000);
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift FROMRIGHT &&&hhhhC 2");
        // Construct a message asking for keys to shift (it will shift its lowest keys, which will be our highest keys)
        proto::KeyShiftRequest protoKeyShiftRequest;
        protoKeyShiftRequest.set_keystoshift(keysToShift);
        {
            StringElement keyShiftReq;
            protoKeyShiftRequest.SerializeToString(&(keyShiftReq.element));
            // Send the message kind, followed by the transmit size, and then the protobuffer.
            UInt32Element kindShiftFromRight(LoaderMsg::SHIFT_FROM_RIGHT);
            UInt32Element bytesInMsg(keyShiftReq.transmitSize());
            BufferUdp data(kindShiftFromRight.transmitSize() + bytesInMsg.transmitSize() + keyShiftReq.transmitSize());
            kindShiftFromRight.appendToData(data);
            bytesInMsg.appendToData(data);
            keyShiftReq.appendToData(data);
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&hhhhC 1");
            ServerTcpBase::_writeData(*_rightSocket, data);
        }
        // &&& Wait for the KeyList response
        {
            auto msgElem = data.readFromSocket(*_rightSocket, "CentralWorker::_shift &&&hhhhC waiting for  FROMRIGHT KeyList");
            auto keyListElem = std::dynamic_pointer_cast<StringElement>(msgElem);
            if (keyListElem == nullptr) {
                throw new LoaderMsgErr("CentralWorker::_shift FROMRIGHT failure to get KeyList");
            }
            auto protoKeyList = keyListElem->protoParse<proto::KeyList>();
            if (protoKeyList == nullptr) {
                throw new LoaderMsgErr("CentralWorker::_shift FROMRIGHT failure to parse KeyList");
            }

            // TODO This is very similar to code in TcpBaseConnection::_handleShiftToRight and they should be merged.
            int sz = protoKeyList->keypair_size();
            std::vector<CentralWorker::StringKeyPair> keyList;
            for (int j=0; j < sz; ++j) {
                proto::KeyInfo const& protoKI = protoKeyList->keypair(j);
                ChunkSubchunk chSub(protoKI.chunk(), protoKI.subchunk());
                keyList.push_back(std::make_pair(protoKI.key(), chSub));
            }
            insertKeys(keyList, false);
        }
        // &&& send received message
        data.reset();
        UInt32Element elem(LoaderMsg::SHIFT_FROM_RIGHT_RECEIVED);
        elem.appendToData(data);
        ServerTcpBase::_writeData(*_rightSocket, data);

        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&hhhhC direction=" << direction << " keys=" << keysToShift);
        // LOGS(_log, LOG_LVL_ERROR, "&&& CentralWorker::_shift NEEDS CODE"); &&&
        // exit (-1); &&&
    } else if (direction == TORIGHT1) {
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift TORIGHT &&&ggggC 2");
        // TODO this is very similar to CentralWorker::buildKeyList() and should be merged with that.
        // Construct a message with that many keys and send it (sending the highest keys)
        proto::KeyList protoKeyList;
        protoKeyList.set_keycount(keysToShift);
        std::string minKey(""); // smallest value of a key sent to right neighbor
        std::string maxKey("");
        {
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 3");
            std::lock_guard<std::mutex> lck(_idMapMtx);
            if (not _transferListToRight1.empty()) {
                throw new LoaderMsgErr("CentralWorker::_shift _transferList not empty");
            }
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 4");
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 5");
            bool firstPass = true;
            for (int j=0; j < keysToShift && _directorIdMap.size() > 1; ++j) {
                LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 6");
                auto iter = _directorIdMap.end();
                --iter; // rbegin() returns a reverse iterator which doesn't work with erase().
                if (firstPass) {
                    maxKey = iter->first;
                }
                _transferListToRight1.push_back(std::make_pair(iter->first, iter->second));
                proto::KeyInfo* protoKI = protoKeyList.add_keypair();
                minKey = iter->first;
                protoKI->set_key(minKey);
                protoKI->set_chunk(iter->second.chunk);
                protoKI->set_subchunk(iter->second.subchunk);
                _directorIdMap.erase(iter);
            }
            // Adjust our range;
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 7");
            _strRange.setMax(minKey);
        }
        StringElement keyList;
        protoKeyList.SerializeToString(&(keyList.element));
        // Send the message kind, followed by the transmit size, and then the protobuffer.
        UInt32Element kindShiftRight(LoaderMsg::SHIFT_TO_RIGHT);
        UInt32Element bytesInMsg(keyList.transmitSize());
        BufferUdp data(kindShiftRight.transmitSize() + bytesInMsg.transmitSize() + keyList.transmitSize());
        kindShiftRight.appendToData(data);
        bytesInMsg.appendToData(data);
        keyList.appendToData(data);
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 8");
        ServerTcpBase::_writeData(*_rightSocket, data);

        // read back LoaderMsg::SHIFT_TO_RIGHT_KEYS_RECEIVED
        data.reset();
        auto msgElem = data.readFromSocket(*_rightSocket, "CentralWorker::_shift SHIFT_TO_RIGHT_KEYS_RECEIVED");
        UInt32Element::Ptr received = std::dynamic_pointer_cast<UInt32Element>(msgElem);
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 9");
        if (received == nullptr || received->element !=  LoaderMsg::SHIFT_TO_RIGHT_RECEIVED) {
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 9error");
            throw new LoaderMsgErr("CentralWorker::_shift receive failure");
        }
        _finishShiftToRight();
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 10");
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift end direction=" << direction << " keys=" << keysToShift);
    }
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 11");
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC DumpKeys " << dumpKeysStr(3));
    _shiftWithRightInProgress = false;
}


void CentralWorker::_finishShiftToRight() {
    std::lock_guard<std::mutex> lck(_idMapMtx);
    _transferListToRight1.clear();
}


void CentralWorker::finishShiftFromRight() {
    std::lock_guard<std::mutex> lck(_idMapMtx);
    _transferListFromRight.clear();
}


/// Return a list of the smallest keys from our map. Put keys in _transferList in case the copy fails.
/// TODO add argument for smallest or largest and code to build list from smallest or largest keys.
StringElement::UPtr CentralWorker::buildKeyList(int keysToShift) {
    std::string funcName = "CentralWorker::buildKeyList";
    proto::KeyList protoKeyList;
    std::string minKey(""); // smallest value of a key sent to right neighbor
    std::string maxKey("");
    {
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhC 3");
        std::lock_guard<std::mutex> lck(_idMapMtx);
        if (not _transferListFromRight.empty()) {
            throw new LoaderMsgErr("CentralWorker::_shift _transferListFromRight not empty");
        }
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhC 4");
        int maxKeysToShift = _directorIdMap.size()/3;
        if (keysToShift > maxKeysToShift) keysToShift = maxKeysToShift;
        protoKeyList.set_keycount(keysToShift);
        LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhC 5");
        bool firstPass = true;
        for (int j=0; j < keysToShift && _directorIdMap.size() > 1; ++j) {
            LOGS(_log, LOG_LVL_INFO, funcName << " &&&hhhhhC 6");
            auto iter = _directorIdMap.begin();
            if (firstPass) {
                minKey = iter->first;
            }
            _transferListFromRight.push_back(std::make_pair(iter->first, iter->second));
            proto::KeyInfo* protoKI = protoKeyList.add_keypair();
            maxKey = iter->first;
            protoKI->set_key(maxKey);
            protoKI->set_chunk(iter->second.chunk);
            protoKI->set_subchunk(iter->second.subchunk);
            _directorIdMap.erase(iter);
        }
        // Adjust our range;
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 7");
        auto iter = _directorIdMap.begin();
        if (_strRange.getMin() != "") {
            _strRange.setMin(iter->first);
        }
    }
    StringElement::UPtr keyList(new StringElement());
    protoKeyList.SerializeToString(&(keyList->element));
    return keyList;
}


/// Must hold _rightMtx before calling
void CentralWorker::_rightConnect() {
    std::string const funcName("CentralWorker::_rightConnect");
        if(_rightConnectStatus == VOID0) {
            _rightConnectStatus = STARTING1;
            // Connect to the right neighbor server
            tcp::resolver resolver(_ioContext);
            auto addr = _neighborRight.getAddressTcp();
            tcp::resolver::results_type endpoints = resolver.resolve(addr.ip, std::to_string(addr.port));
            _rightSocket.reset(new tcp::socket(_ioContext));
            boost::system::error_code ec;
            boost::asio::connect(*_rightSocket, endpoints, ec);
            if (ec) {
                _rightSocket.reset();
                LOGS(_log, LOG_LVL_WARN, "failed to connect to " << _neighborRight.getName() << " " <<
                     addr << " ec=" << ec.value() << ":" << ec.message());
                return;
            }

            // Get name from server
            BufferUdp data(2000);
            {
                auto msgElem = data.readFromSocket(*_rightSocket, "CentralWorker::_rightConnect");
                // First element should be UInt32Element with the other worker's name
                UInt32Element::Ptr nghName = std::dynamic_pointer_cast<UInt32Element>(msgElem);
                if (nghName == nullptr) {
                    throw LoaderMsgErr("first element wasn't correct type " +
                            msgElem->getStringVal(), __FILE__, __LINE__);
                }

                // Check if correct name
                if (nghName->element != _neighborRight.getName()) {
                    throw LoaderMsgErr("wrong name expected " + std::to_string(_neighborRight.getName()) +
                            " got " + std::to_string(nghName->element), __FILE__, __LINE__);
                }
            }

            // Send our basic info (&&& put this in separate function to be used in other communications !!! &&&)
            _determineRange();

            // Unless they disconnect.
            _rightConnectStatus = ESTABLISHED2;
            _neighborRight.setEstablished(true);
        }
}


void CentralWorker::setNeighborInfoLeft(uint32_t name, int keyCount, StringRange const& range) {
    _neighborLeft.setName(name);
    if (name != _neighborLeft.getName()) {
        LOGS(_log, LOG_LVL_ERROR, "disconnecting left since setNeighborInfoLeft name(" << name <<
                                  ") != neighborLeft.name(" << _neighborLeft.getName() << ")");
        _neighborLeft.setEstablished(false);
        return;
    }
    _neighborLeft.setKeyCount(keyCount);
    _neighborLeft.setRange(range);
    _neighborLeft.setEstablished(true);
}


/// Must hold _rightMtx before calling
void CentralWorker::_rightDisconnect() {
    LOGS(_log, LOG_LVL_WARN, "CentralWorker::_rightDisconnect");
    if (_rightSocket != nullptr) {
        _rightSocket->shutdown(boost::asio::ip::tcp::socket::shutdown_both);
        _rightSocket->close();
    }
    _rightConnectStatus = VOID0;
    _cancelShiftsToRightNeighbor();
}


void CentralWorker::_cancelShiftsToRightNeighbor() {
    // Client side of connection. Was sending largest keys right.
    LOGS(_log, LOG_LVL_WARN, "Canceling shiftToRight neighbor");
    std::lock_guard<std::mutex> lck(_idMapMtx);
    if (_shiftWithRightInProgress.exchange(false)) {
        // Restore the transfer list to the id map
        for (auto&& elem:_transferListToRight1) {
            auto res = _directorIdMap.insert(std::make_pair(elem.first, elem.second));
            if (not res.second) {
                LOGS(_log, LOG_LVL_WARN, "_cancelShiftsRightNeighbor Possible duplicate " <<
                        elem.first << ":" << elem.second);
            }
        }
        _transferListToRight1.clear();
        // Leave the reduced range until fixed by our right neighbor.
    }
}


void CentralWorker::cancelShiftsFromRightNeighbor() {
    // Server side of connection. Was sending smallest keys left.
    LOGS(_log, LOG_LVL_WARN, "Canceling shiftFromRight neighbor");
    std::lock_guard<std::mutex> lck(_idMapMtx); // &&& check that this does not cause deadlock
    if (not _transferListFromRight.empty()) {
        // Restore the transfer list to the id map
        for (auto&& elem:_transferListFromRight) {
            auto res = _directorIdMap.insert(std::make_pair(elem.first, elem.second));
            if (not res.second) {
                LOGS(_log, LOG_LVL_WARN, "_cancelShiftsRightNeighbor Possible duplicate " <<
                        elem.first << ":" << elem.second);
            }
        }
        _transferListFromRight.clear();

        // Fix the bottom of the range.
        if (_strRange.getMin() != "") {
            _strRange.setMin(_directorIdMap.begin()->first);
        }
    }
}


void CentralWorker::registerWithMaster() {
    // &&& TODO: add a one shot DoList item to keep calling _registerWithMaster until we have our name.
    _registerWithMaster();
}


bool CentralWorker::workerInfoReceive(BufferUdp::Ptr const&  data) {
    // LOGS(_log, LOG_LVL_INFO, " ******&&& workerInfoRecieve data=" << data->dump());
    // Open the data protobuffer and add it to our list.
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }
    std::unique_ptr<proto::WorkerListItem> protoList = sData->protoParse<proto::WorkerListItem>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }

    // &&& TODO move this call to another thread
    _workerInfoReceive(protoList);
    return true;
}


void CentralWorker::_workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoL) {
    std::unique_ptr<proto::WorkerListItem> protoList(std::move(protoL));

    // Check the information, if it is our network address, set or check our name.
    // Then compare it with the map, adding new/changed information.
    uint32_t name = protoList->name();
    std::string ipUdp("");
    int portUdp = 0;
    int portTcp = 0;
    if (protoList->has_address()) {
        proto::LdrNetAddress protoAddr = protoList->address();
        ipUdp = protoAddr.ip();
        portUdp = protoAddr.udpport();
        portTcp = protoAddr.tcpport();
    }
    StringRange strRange;
    if (protoList->has_rangestr()) {
        proto::WorkerRangeString protoRange= protoList->rangestr();
        bool valid        = protoRange.valid();
        if (valid) {
            std::string min   = protoRange.min();
            std::string max   = protoRange.max();
            bool unlimited = protoRange.maxunlimited();
            strRange.setMinMax(min, max, unlimited);
            //LOGS(_log, LOG_LVL_WARN, "&&& CentralWorker::workerInfoRecieve range=" << strRange);
        }
    }

    // If the address matches ours, check the name.
    if (getHostName() == ipUdp && getUdpPort() == portUdp) {
        if (isOurNameInvalid()) {
            LOGS(_log, LOG_LVL_INFO, "Setting our name " << name);
            setOurName(name);
        } else if (getOurName() != name) {
            LOGS(_log, LOG_LVL_ERROR, "Our name doesn't match address from master! name=" <<
                                      getOurName() << " masterName=" << name);
        }

        // It is this worker. If there is a valid range in the message and our range is not valid,
        // take the range given as our own.
        if (strRange.getValid()) {
            std::lock_guard<std::mutex> lckM(_idMapMtx);
            if (not _strRange.getValid()) {
                LOGS(_log, LOG_LVL_INFO, "Setting our range " << strRange);
                _strRange.setMinMax(strRange.getMin(), strRange.getMax(), strRange.getUnlimited());
            }
        }
    }

    // Make/update entry in map.
    _wWorkerList->updateEntry(name, ipUdp, portUdp, portTcp, strRange);
}


StringRange CentralWorker::updateLeftNeighborRange(StringRange const& leftNeighborRange) {
    // If our range is invalid
    //    our min is their max incremented (stringRange increment function)
    //    if their max is unlimited, our max becomes unlimited
    //    else max = increment(min)
    //    send range to master
    //    return our new range
    StringRange newLeftNeighborRange(leftNeighborRange);
    {
        std::unique_lock<std::mutex> lck(_idMapMtx);
        if (not _strRange.getValid()) {
            // Our range has not been set, so base it on the range of the left neighbor.
            // The left range will need to be changed
            auto min = StringRange::incrementString(leftNeighborRange.getMax());
            auto max = min;
            _strRange.setMinMax(min, max, leftNeighborRange.getUnlimited());
            newLeftNeighborRange.setMax(min, false);
        } else {
            // Our range is valid already, it should be > than the left neighbor range.
            if (_strRange < leftNeighborRange) {
                LOGS(_log, LOG_LVL_ERROR, "LeftNeighborRange(" << leftNeighborRange <<
                        ") is greater than our range(" << _strRange << ")");
                // TODO corrective action?
            }
            // The left neighbor's max should be the minimum value in our keymap, unless the
            // map is empty.
            if (_directorIdMap.empty()) {
                // Don't do anything to left neighbor range.
            } else {
                auto min = _directorIdMap.begin()->first;
                newLeftNeighborRange.setMax(min, false);
            }
        }
    }

    return newLeftNeighborRange;
}


bool CentralWorker::workerKeyInsertReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data) {
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInsertReq Failed to parse list");
        return false;
    }
    auto protoData = sData->protoParse<proto::KeyInfoInsert>();
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInsertReq Failed to parse list");
        return false;
    }

    // &&& TODO move this to another thread
    _workerKeyInsertReq(inMsg, protoData);
    return true;
}


void CentralWorker::_workerKeyInsertReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf) {
    std::unique_ptr<proto::KeyInfoInsert> protoData(std::move(protoBuf));

    // Get the source of the request
    proto::LdrNetAddress protoAddr = protoData->requester();
    NetworkAddress nAddr(protoAddr.ip(), protoAddr.udpport());

    proto::KeyInfo protoKeyInfo = protoData->keyinfo();
    std::string key = protoKeyInfo.key();
    ChunkSubchunk chunkInfo(protoKeyInfo.chunk(), protoKeyInfo.subchunk());

    /// see if the key should be inserted into our map
    std::unique_lock<std::mutex> lck(_idMapMtx);
    auto min = _strRange.getMin();
    auto leftAddress = _neighborLeft.getAddressUdp();
    auto rightAddress = _neighborRight.getAddressUdp();
    if (_strRange.isInRange(key)) {
        // insert into our map
        auto res = _directorIdMap.insert(std::make_pair(key, chunkInfo));
        lck.unlock();
        if (not res.second) {
            // &&& element already found, check file id and row number. Bad if not the same.
            // TODO send back duplicate key mismatch message to the original requester and return
        }
        LOGS(_log, LOG_LVL_INFO, "Key inserted=" << key << "(" << chunkInfo << ")");
        // TODO Send this item to the keyLogger (which would then send KEY_INSERT_COMPLETE back to the requester),
        // for now this function will send the message back for proof of concept.
        LoaderMsg msg(LoaderMsg::KEY_INSERT_COMPLETE, inMsg.msgId->element, getHostName(), getUdpPort());
        BufferUdp msgData;
        msg.serializeToData(msgData);
        // protoKeyInfo should still be the same
        proto::KeyInfo protoReply;
        protoReply.set_key(key);
        protoReply.set_chunk(chunkInfo.chunk);
        protoReply.set_subchunk(chunkInfo.subchunk);
        StringElement strElem;
        protoReply.SerializeToString(&(strElem.element));
        strElem.appendToData(msgData);
        LOGS(_log, LOG_LVL_INFO, "&&& sending complete " << key << " to " << nAddr << " from " << _ourName);
        sendBufferTo(nAddr.ip, nAddr.port, msgData);
    } else {
        lck.unlock();
        // Find the target range in the list and send the request there
        auto targetWorker = _wWorkerList->findWorkerForKey(key);
        if (targetWorker != nullptr && targetWorker->getName() != _ourName) {
            _forwardKeyInsertRequest(targetWorker->getAddressUdp(), inMsg, protoData);
        } else {
            // Send request to left or right neighbor
            if (key < min && leftAddress.ip != "") {
                _forwardKeyInsertRequest(leftAddress, inMsg, protoData);
            } else if (key > min && rightAddress.ip != "") {
                _forwardKeyInsertRequest(rightAddress, inMsg, protoData);
            }
        }
    }
}


void CentralWorker::_forwardKeyInsertRequest(NetworkAddress const& targetAddr, LoaderMsg const& inMsg,
                                             std::unique_ptr<proto::KeyInfoInsert>& protoData) {
    // Aside from hops, the proto buffer should be the same.
    proto::KeyInfo protoKeyInfo = protoData->keyinfo();
    auto key = protoKeyInfo.key();
    // The proto buffer should be the same, just need a new message.
    int hops = protoData->hops() + 1;
    if (hops > 4) {
        LOGS(_log, LOG_LVL_INFO, "Too many hops, dropping insert request hops=" << hops << " key=" << key);
        return;
    }
    LOGS(_log, LOG_LVL_INFO, "Forwarding key insert hops=" << hops << " key=" << key);
    LoaderMsg msg(LoaderMsg::KEY_INSERT_REQ, inMsg.msgId->element, getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);

    StringElement strElem;
    protoData->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);
    sendBufferTo(targetAddr.ip, targetAddr.port, msgData);
}


bool CentralWorker::workerKeyInfoReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data) {
    LOGS(_log, LOG_LVL_INFO, " &&& CentralWorker::workerKeyInfoReq");
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInfoReq Failed to parse list");
        return false;
    }
    auto protoData = sData->protoParse<proto::KeyInfoInsert>();  /// &&& KeyInfoInsert <- more generic name or new type for key lookup?
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInfoReq Failed to parse list");
        return false;
    }

    // &&& TODO move this to another thread
    _workerKeyInfoReq(inMsg, protoData);
    return true;
}


// &&& alter
void CentralWorker::_workerKeyInfoReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf) {
    std::unique_ptr<proto::KeyInfoInsert> protoData(std::move(protoBuf));

    // Get the source of the request
    proto::LdrNetAddress protoAddr = protoData->requester();
    NetworkAddress nAddr(protoAddr.ip(), protoAddr.udpport());

    proto::KeyInfo protoKeyInfo = protoData->keyinfo();
    std::string key = protoKeyInfo.key();
    //    ChunkSubchunk chunkInfo(protoKeyInfo.chunk(), protoKeyInfo.subchunk());  &&&


    /// see if the key is in our map
    std::unique_lock<std::mutex> lck(_idMapMtx);
    if (_strRange.isInRange(key)) {
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_workerKeyInfoReq " << _ourName << " looking for key=" << key);
        // check out map
        auto iter = _directorIdMap.find(key);
        lck.unlock();

        // Key found or not, message will be returned.
        LoaderMsg msg(LoaderMsg::KEY_INFO, inMsg.msgId->element, getHostName(), getUdpPort());
        BufferUdp msgData;
        msg.serializeToData(msgData);
        proto::KeyInfo protoReply;
        protoReply.set_key(key);
        if (iter == _directorIdMap.end()) {
            // key not found message.
            protoReply.set_chunk(0);
            protoReply.set_subchunk(0);
            protoReply.set_success(false);
            LOGS(_log, LOG_LVL_INFO, "Key info not found key=" << key);
        } else {
            // key found message.
            auto elem = iter->second;
            protoReply.set_chunk(elem.chunk);
            protoReply.set_subchunk(elem.subchunk);
            protoReply.set_success(true);
            LOGS(_log, LOG_LVL_INFO, "Key info lookup key=" << key <<
                 " (" << protoReply.chunk() << ", " << protoReply.subchunk() << ")");
        }
        StringElement strElem;
        protoReply.SerializeToString(&(strElem.element));
        strElem.appendToData(msgData);
        LOGS(_log, LOG_LVL_INFO, "&&& sending key lookup " << key << " to " << nAddr << " from " << _ourName);
        sendBufferTo(nAddr.ip, nAddr.port, msgData);
    } else {
        // Find the target range in the list and send the request there
        auto targetWorker = _wWorkerList->findWorkerForKey(key);
        if (targetWorker == nullptr) {
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_workerKeyInfoReq " << _ourName << " could not forward key=" << key);
            return;
        } // Client will have to try again.
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_workerKeyInfoReq " << _ourName << " forwarding key=" << key << " to " << *targetWorker);
        _forwardKeyInfoRequest(targetWorker, inMsg, protoData);
    }
}


bool CentralWorker::workerWorkerSetRightNeighbor(LoaderMsg const& inMsg, BufferUdp::Ptr const& data) {
    auto msgElem = MsgElement::retrieve(*data);
    UInt32Element::Ptr neighborName = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (neighborName == nullptr) {
        return false;
    }

    LOGS(_log, LOG_LVL_INFO, "&&& workerWorkerSetRightNeighbor ourName=" << _ourName << " rightN=" << neighborName->element);
    // Just setting the name, so it can stay here. See CentralWorker::_monitor(), which establishes/maintains connections.
    _neighborRight.setName(neighborName->element);
    return true;
}


bool CentralWorker::workerWorkerSetLeftNeighbor(LoaderMsg const& inMsg, BufferUdp::Ptr const& data) {
    auto msgElem = MsgElement::retrieve(*data);
    UInt32Element::Ptr neighborName = std::dynamic_pointer_cast<UInt32Element>(msgElem);
    if (neighborName == nullptr) {
        return false;
    }

    LOGS(_log, LOG_LVL_INFO, "&&& workerWorkerSetLeftNeighbor ourName=" << _ourName << " leftN=" << neighborName->element);
    // &&& TODO move to separate thread
    _neighborLeft.setName(neighborName->element);
    // Just setting the name. See CentralWorker::_monitor(), which establishes/maintains connections.
    return true;
}


bool CentralWorker::workerWorkerKeysInfoReq(LoaderMsg const& inMsg, BufferUdp::Ptr const& data) {
    // Send a message containing information about the range and number of keys handled by this worker back
    // to the sender. Nothing in data

    // &&& TODO move this to another thread
    _workerWorkerKeysInfoReq(inMsg);
    return true;

}


// &&& alter
void CentralWorker::_workerWorkerKeysInfoReq(LoaderMsg const& inMsg) {
    // Use the address from inMsg as this kind of request is pointless to forward.
    NetworkAddress nAddr(inMsg.senderHost->element, inMsg.senderPort->element);
    uint64_t msgId = inMsg.msgId->element;

    // &&& TODO put this in a separate function, adding a DoListItem to send it occasionally or when size changes significantly.
    // Build message containing Range, size of map, number of items added.
    LoaderMsg msg(LoaderMsg::WORKER_KEYS_INFO, msgId, getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_workerWorkerKeysInfoReq building protoWKI &&&");
    std::unique_ptr<proto::WorkerKeysInfo> protoWKI = _workerKeysInfoBuilder();
    StringElement strElem;
    protoWKI->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);
    LOGS(_log, LOG_LVL_INFO, "&&& sending WorkerKeysInfo name=" << _ourName <<
         " mapsize=" << protoWKI->mapsize() << " recentAdds=" << protoWKI->recentadds() <<
         " to " << nAddr);
    sendBufferTo(nAddr.ip, nAddr.port, msgData);
}


std::unique_ptr<proto::WorkerKeysInfo> CentralWorker::_workerKeysInfoBuilder() {
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_workerKeysInfoBuilder &&&");
    std::unique_ptr<proto::WorkerKeysInfo> protoWKI(new proto::WorkerKeysInfo());
    // Build message containing Range, size of map, number of items added. &&& duplicate code
    StringRange range;
    size_t mapSize;
    size_t recentAdds;
    {
        std::lock_guard<std::mutex> lck(_idMapMtx);
        range = _strRange;
        mapSize = _directorIdMap.size();
        _removeOldEntries();
        recentAdds = _recentAdds.size();
    }
    LOGS(_log, LOG_LVL_INFO, "&&& CentralWorker WorkerKeysInfo aaaaa name=" << _ourName << " keyCount=" << mapSize << " recentAdds=" << recentAdds);
    protoWKI->set_name(_ourName);
    protoWKI->set_mapsize(mapSize);
    protoWKI->set_recentadds(recentAdds);
    proto::WorkerRangeString *protoRange = protoWKI->mutable_range();  // TODO &&& make a function to load WorkerRangeString, happens a bit.
    protoRange->set_valid(range.getValid());
    protoRange->set_min(range.getMin());
    protoRange->set_max(range.getMax());
    protoRange->set_maxunlimited(range.getUnlimited());
    proto::Neighbor *protoLeft = protoWKI->mutable_left();
    protoLeft->set_name(_neighborLeft.getName());
    proto::Neighbor *protoRight = protoWKI->mutable_right();
    protoRight->set_name(_neighborRight.getName());
    return protoWKI;
}


// &&& alter
// TODO This looks a lot like the other _forward*** functions, try to combine them.
void CentralWorker::_forwardKeyInfoRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                             std::unique_ptr<proto::KeyInfoInsert> const& protoData) {
    // The proto buffer should be the same, just need a new message.
    LoaderMsg msg(LoaderMsg::KEY_INFO_REQ, inMsg.msgId->element, getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);

    StringElement strElem;
    protoData->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    auto nAddr = target->getAddressUdp();
    sendBufferTo(nAddr.ip, nAddr.port, msgData);
}


void CentralWorker::_registerWithMaster() {
    LoaderMsg msg(LoaderMsg::MAST_WORKER_ADD_REQ, getNextMsgId(), getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    // create the proto buffer
    lsst::qserv::proto::LdrNetAddress protoBuf;
    protoBuf.set_ip(getHostName());
    protoBuf.set_udpport(getUdpPort());
    protoBuf.set_tcpport(getTcpPort());

    StringElement strElem;
    protoBuf.SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    sendBufferTo(getMasterHostName(), getMasterPort(), msgData);
}


void CentralWorker::testSendBadMessage() {
    uint16_t kind = 60200;
    LoaderMsg msg(kind, getNextMsgId(), getHostName(), getUdpPort());
    LOGS(_log, LOG_LVL_INFO, "testSendBadMessage msg=" << msg);
    BufferUdp msgData(128);
    msg.serializeToData(msgData);
    sendBufferTo(getMasterHostName(), getMasterPort(), msgData);
}


void CentralWorker::_removeOldEntries() {
    // _idMapMtx must be held when this is called.
    auto now = std::chrono::system_clock::now();
    auto then = now - _recent;
    while (_recentAdds.size() > 0 && _recentAdds.front() < then) {
        _recentAdds.pop_front();
    }
}


void CentralWorker::insertKeys(std::vector<StringKeyPair> const& keyList, bool mustSetMin) {
    std::unique_lock<std::mutex> lck(_idMapMtx);
    auto maxKey = _strRange.getMax();
    bool maxKeyChanged = false;
    for (auto&& elem:keyList) {
        auto const& key = elem.first;
        auto res = _directorIdMap.insert(std::make_pair(key, elem.second));
        if (key > maxKey) {
            maxKey = key;
            maxKeyChanged = true;
        }
        if (not res.second) {
            LOGS(_log, LOG_LVL_WARN, "insertKeys Possible duplicate " <<
                                     elem.first << ":" << elem.second);
        }
    }

    // On all nodes except the left most, the minimum should be reset.
    if (mustSetMin && _directorIdMap.size() > 0) {
        auto minKeyPair = _directorIdMap.begin();
        _strRange.setMin(minKeyPair->first);
    }

    if (maxKeyChanged) {
        // if unlimited is false, range will be slightly off until corrected by the right neighbor.
        bool unlimited = _strRange.getUnlimited();
        _strRange.setMax(maxKey, unlimited);
    }
}


std::string CentralWorker::dumpKeysStr(int count) {
    std::stringstream os;
    std::lock_guard<std::mutex> lck(_idMapMtx);
    os << "name=" << getOurName() << " count=" << _directorIdMap.size() << " range(" << _strRange << ") pairs: ";

    if (count < 1) {
        for (auto&& elem:_directorIdMap) {
            os << elem.first << "{" << elem.second << "} ";
        }
    } else {
        auto iter = _directorIdMap.begin();
        for (int j=0; j < count && iter != _directorIdMap.end(); ++iter, ++j) {
            os << iter->first << "{" << iter->second << "} ";
        }
        os << " ... ";
        auto rIter = _directorIdMap.rbegin();
        for (int j=0; j < count && rIter != _directorIdMap.rend(); ++rIter, ++j) {
            os << rIter->first << "{" << rIter->second << "} ";
        }

    }
    return os.str();
}
}}} // namespace lsst::qserv::loader
