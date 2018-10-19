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
    // TODO Check if we've heard from left neighbor (possibly kill connection if nothing heard???)


    // TODO &&& check the right neighbor connection, kill and restart if needed.
    std::lock_guard<std::mutex> lck(_rightMtx); // This mutex is locked for a long time &&&
    if (_neighborRight.getName() != 0) {
        try {
            if (not _neighborRight.getEstablished()) {
                auto nAddr = _neighborRight.getAddress();
                if (nAddr.ip == "") {
                    // look up the network address for the rightNeighbor
                    WWorkerListItem::Ptr nWorker = _wWorkerList->getWorkerNamed(_neighborRight.getName());
                    if (nWorker != nullptr) {
                        auto addr = nWorker->getAddressTcp();
                        LOGS(_log, LOG_LVL_INFO, "_monitor neighbor right " << _neighborRight.getName() <<
                                " " << addr);
                        _neighborRight.setAddress(addr);
                    }
                }

                LOGS(_log, LOG_LVL_INFO, "_monitor trying to establish TCP connection with " <<
                        _neighborRight.getName() << " " << _neighborRight.getAddress());
                _rightConnect(); // calls _determineRange() while establishing connection
            } else {
                _determineRange();
            }
            _shiftIfNeeded();
        } catch (LoaderMsgErr const& ex) {
            LOGS(_log, LOG_LVL_WARN, "_monitor() " << ex.what());
            _rightDisconnect();
        }
    } else {
        // If there is a connection, close it.
        _rightDisconnect();
    }
}


void CentralWorker::_determineRange() {
    std::string const funcName("CentralWorker::_rightConnect");
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
        auto msgElem = data.readFromSocket(*_rightSocket, "CentralWorker::_rightConnect - range");
        LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect() parsing data=" << data.dump());
        auto protoItem = StringElement::protoParse<proto::WorkerKeysInfo>(data);
        if (protoItem == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }
        NeighborsInfo nInfoR;
        auto workerName = protoItem->name();
        nInfoR.keyCount = protoItem->mapsize();
        _neighborRight.setKeyCount(nInfoR.keyCount); // TODO add a timestamp to this data.
        nInfoR.recentAdds = protoItem->recentadds();
        proto::WorkerRangeString protoRange = protoItem->range();
        LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect() rightNeighbor name=" << workerName << " keyCount=" << nInfoR.keyCount << " recentAdds=" << nInfoR.recentAdds);
        bool valid = protoRange.valid();
        StringRange rightRange;
        if (valid) {
            std::string min   = protoRange.min();
            std::string max   = protoRange.max();
            bool unlimited = protoRange.maxunlimited();
            rightRange.setMinMax(min, max, unlimited);
            LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect rightRange=" << rightRange);
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
void CentralWorker::_shiftIfNeeded() {
    // There should be reasonably recent information from our neighbors. Use that
    // and our status to ask the right neighbor to give us entries or we send entries
    // to the right neighbor.
    // If right connection is not established, return
    if (not _neighborRight.getEstablished()) {
        LOGS(_log, LOG_LVL_INFO, "_shiftIfNeeded no right neighbor, no shift.");
        return;
    }
    if (_shiftWithRightInProgress) {
        LOGS(_log, LOG_LVL_INFO, "_shiftIfNeeded shift already in progress.");
        return;
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
            // send enough keys to the right to balance (min 1 key, max _maxShiftKeys, never shift more than 20% of our keys)
    int rightKeyCount = 0;
    StringRange rightRange;
    _neighborRight.getKeyData(rightKeyCount, rightRange);
    if (range > rightRange) {
        LOGS(_log, LOG_LVL_ERROR, "Right neighbor range is less than ours!!!! our=" << range << " right=" << rightRange);
        return;
    }
    int keysToShift = 0;
    CentralWorker::Direction direction = NONE0;
    int sourceSize = 0;
    if (mapSize > rightKeyCount*_thresholdNeighborShift) { // TODO add average check
        keysToShift = mapSize - rightKeyCount;
        direction = TORIGHT1;
        sourceSize = mapSize;
    } else if (mapSize*_thresholdNeighborShift < rightKeyCount) {
        keysToShift = rightKeyCount - mapSize;
        direction = FROMRIGHT2;
        sourceSize = rightKeyCount;
    }
    if (keysToShift > _maxKeysToShift) keysToShift = _maxKeysToShift;
    if (keysToShift > sourceSize/3) keysToShift = sourceSize/3;
    if (keysToShift < 1) {
        LOGS(_log, LOG_LVL_WARN, "Worker doesn't have enough keys to shift.");
        return;
    }
    _shiftWithRightInProgress = true;
    _shift(direction, keysToShift);
}


void CentralWorker::_shift(Direction direction, int keysToShift) {
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 1");
    if (direction == FROMRIGHT2) {
        // &&& construct a message asking for keys to shift keys
        // &&& Wait for the response
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC direction=" << direction << " keys=" << keysToShift);
        LOGS(_log, LOG_LVL_ERROR, "&&& CentralWorker::_shift NEEDS CODE");
        exit (-1);
    } else if (direction == TORIGHT1) {
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 2");
        // &&& construct a message with that many keys and send it
        proto::KeyList protoKeyList;
        protoKeyList.set_keycount(keysToShift);
        std::string minKey(""); // smallest value of a key sent to right neighbor
        std::string maxKey("");
        {
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 3");
            if (not _transferList.empty()) {
                throw new LoaderMsgErr("CentralWorker::_shift _transferList not empty");
            }
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 4");
            std::lock_guard<std::mutex> lck(_idMapMtx);
            LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 5");
            bool firstPass = true;
            for (int j=0; j < keysToShift && _directorIdMap.size() > 1; ++j) {
                LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 6");
                auto iter = _directorIdMap.end();
                --iter; // rbegin() returns a reverse iterator which doesn't work with erase().
                if (firstPass) {
                    maxKey = iter->first;
                }
                _transferList.push_back(std::make_pair(iter->first, iter->second));
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
            {
                // Restore the original range
                std::lock_guard<std::mutex> lck(_idMapMtx);
                _strRange.setMax(maxKey);
            }
            throw new LoaderMsgErr("CentralWorker::_shift receive failure");
        }
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 10");
        LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift end direction=" << direction << " keys=" << keysToShift);
    }
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC 11");
    LOGS(_log, LOG_LVL_INFO, "CentralWorker::_shift &&&ggggC DumpKeys " << dumpKeys()); // &&& make debug or delete or limit number of keys printed.
    _shiftWithRightInProgress = false;
}


/// Must hold _rightMtx before calling
void CentralWorker::_rightConnect() {
    std::string const funcName("CentralWorker::_rightConnect");
        if(_rightConnectStatus == VOID0) {
            _rightConnectStatus = STARTING1;
            // Connect to the right neighbor server
            tcp::resolver resolver(_ioContext);
            auto addr = _neighborRight.getAddress();
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
#if 0 // &&&
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
                auto msgElem = data.readFromSocket(*_rightSocket, "CentralWorker::_rightConnect - range");
                LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect() parsing data=" << data.dump());
                auto protoItem = StringElement::protoParse<proto::WorkerKeysInfo>(data);
                if (protoItem == nullptr) {
                    throw LoaderMsgErr(funcName, __FILE__, __LINE__);
                }
                NeighborsInfo nInfoR;
                auto workerName = protoItem->name();
                nInfoR.keyCount = protoItem->mapsize();
                _neighborRight.setKeyCount(nInfoR.keyCount); // TODO add a timestamp to this data.
                nInfoR.recentAdds = protoItem->recentadds();
                proto::WorkerRangeString protoRange = protoItem->range();
                LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect() rightNeighbor name=" << workerName << " keyCount=" << nInfoR.keyCount << " recentAdds=" << nInfoR.recentAdds);
                bool valid = protoRange.valid();
                StringRange rightRange;
                if (valid) {
                    std::string min   = protoRange.min();
                    std::string max   = protoRange.max();
                    bool unlimited = protoRange.maxunlimited();
                    rightRange.setMinMax(min, max, unlimited);
                    LOGS(_log, LOG_LVL_INFO, "&&& _rightConnect rightRange=" << rightRange);
                    _neighborRight.setRange(rightRange);
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
#endif

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
    if (_rightSocket != nullptr) {
        _rightSocket->shutdown(boost::asio::ip::tcp::socket::shutdown_both);
        _rightSocket->close();
    }
    _rightConnectStatus = VOID0;
    _cancelShiftsRightNeighbor();
}


void CentralWorker::_cancelShiftsRightNeighbor() {
    LOGS(_log, LOG_LVL_WARN, "Canceling shifts with right neighbor");
    std::lock_guard<std::mutex> lck(_idMapMtx); // &&& check that this does not cause deadlock
    if (_shiftWithRightInProgress.exchange(false)) {
        // TODO What else needs to done?
        { // // &&& make this _restoreTransferList()
            // Restore the transfer list to the id map
            for (auto&& elem:_transferList) {
                auto res = _directorIdMap.insert(std::make_pair(elem.first, elem.second));
                if (not res.second) {
                    LOGS(_log, LOG_LVL_WARN, "_cancelShiftsRightNeighbor Possible duplicate " <<
                                             elem.first << ":" << elem.second);
                }
            }
            _transferList.clear();
        }
        LOGS(_log, LOG_LVL_ERROR, "CentralWorker::_cancelShiftsRightNeighbor needs code");
        exit(-1);
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
        if (targetWorker == nullptr) { return; }
        if (targetWorker->getName() != _ourName) {
            _forwardKeyInsertRequest(targetWorker, inMsg, protoData);
        } else {
            // TODO send request to left or right neighbor
            LOGS(_log, LOG_LVL_ERROR, "CentralWorker::_workerKeyInsertReq needs code to send key to neighbor");
        }
    }
}


void CentralWorker::_forwardKeyInsertRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                             std::unique_ptr<proto::KeyInfoInsert> const& protoData) {
    // The proto buffer should be the same, just need a new message.
    LoaderMsg msg(LoaderMsg::KEY_INSERT_REQ, inMsg.msgId->element, getHostName(), getUdpPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);

    StringElement strElem;
    protoData->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    auto nAddr = target->getAddressUdp();
    sendBufferTo(nAddr.ip, nAddr.port, msgData);
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


// TODO add option to print limited number of keys (maybe first 3 keyval pairs and last 3)
std::string CentralWorker::dumpKeys() {
    std::stringstream os;
    std::lock_guard<std::mutex> lck(_idMapMtx);
    os << "name=" << getOurName() << " count=" << _directorIdMap.size() << " range(" << _strRange << ") pairs: ";

    for (auto&& elem:_directorIdMap) {
        os << elem.first << "{" << elem.second << "} ";
    }
    return os.str();
}
}}} // namespace lsst::qserv::loader
