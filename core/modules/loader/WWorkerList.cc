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
#include "loader/WWorkerList.h"

// System headers
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers


// qserv headers
#include "loader/CentralWorker.h"
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"


// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.WWorkerList");
}

namespace lsst {
namespace qserv {
namespace loader {





util::CommandTracked::Ptr WWorkerList::createCommand() {
    return createCommandWorker(_central);
}


util::CommandTracked::Ptr WWorkerList::createCommandWorker(CentralWorker* centralW) {
    // On the worker, need to occasionally ask for a list of workers from the master
    // and make sure each of those workers is on the doList
    class MastWorkerListReqCmd : public util::CommandTracked {
    public:
        MastWorkerListReqCmd(CentralWorker* centralW, std::map<uint32_t, WWorkerListItem::Ptr> nameMap)
            : _centralW(centralW), _nameMap(nameMap) {}

        void action(util::CmdData *data) override {
            /// Request a list of all workers.
            LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::createCommand::WorkerReqCmd::action");

            // TODO make a function for this, it's always going to be the same.
            proto::LdrNetAddress protoOurAddress;
            protoOurAddress.set_ip(_centralW->getHostName());
            protoOurAddress.set_udpport(_centralW->getUdpPort());
            protoOurAddress.set_tcpport(_centralW->getTcpPort());
            StringElement eOurAddress(protoOurAddress.SerializeAsString());

            LoaderMsg workerInfoReqMsg(LoaderMsg::MAST_WORKER_LIST_REQ, _centralW->getNextMsgId(),
                                       _centralW->getHostName(), _centralW->getUdpPort());
            BufferUdp sendBuf(1000);
            workerInfoReqMsg.serializeToData(sendBuf);
            eOurAddress.appendToData(sendBuf);

            // Send the request to master.
            auto masterHost = _centralW->getMasterHostName();
            auto masterPort = _centralW->getMasterPort();
            _centralW->sendBufferTo(masterHost, masterPort, sendBuf);

            /// Go through the existing list and add any that have not been add to the doList
            for (auto const& item : _nameMap) {
                item.second->addDoListItems(_centralW);
                //_centralW->addDoListItem(item.second);
            }
        }

    private:
        CentralWorker* _centralW;
        std::map<uint32_t, WWorkerListItem::Ptr> _nameMap;
    };

    LOGS(_log, LOG_LVL_INFO, "&&& WorkerList::createCommandWorker");
    return std::make_shared<MastWorkerListReqCmd>(centralW, _nameMap);
}


bool WWorkerList::workerListReceive(BufferUdp::Ptr const& data) {
    LOGS(_log, LOG_LVL_INFO, " &&& workerListReceive data=" << data->dump());
    // Open the data protobuffer and add it to our list.
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }
    auto protoList = sData->protoParse<proto::LdrMastWorkerList>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }

    // &&& TODO put this in separate thread, the part above can probably be put in a separate function in _central
    int sizeChange = 0;
    std::string strNames;
    {
        std::lock_guard<std::mutex> lock(_mapMtx);
        size_t initialSize = _nameMap.size();
        _totalNumberOfWorkers = protoList->workercount(); // There may be more workers than will fit in a message.
        int sz = protoList->worker_size();

        for (int j=0; j < sz; ++j) {
            proto::WorkerListItem const& protoItem = protoList->worker(j);
            uint32_t name = protoItem.name();
            // Most of the time, the worker will already be in the map.
            auto item = _nameMap[name];
            LOGS(_log, LOG_LVL_INFO, j << "&&& _nameMap got name=" << name << " item=" << item);
            if (item == nullptr) {
                LOGS(_log, LOG_LVL_INFO, "&&& nullptr name=" << name);
                item = WWorkerListItem::create(name, _central);
                _nameMap[name] = item;
                strNames += std::to_string(name) + ",";
                item->addDoListItems(_central);
            }
        }
        sizeChange = _nameMap.size() - initialSize;
        if (sizeChange > 0) {
            _flagListChange();
        }
    }

    infoReceived(); // Avoid asking for this info for a while.
    LOGS(_log, LOG_LVL_INFO, "workerListReceive added " << sizeChange << " names=" << strNames);

    return true;
}

// must lock _mapMtx before calling this function
void WWorkerList::_flagListChange() {
    _wListChanged = true;
}


bool WWorkerList::equal(WWorkerList& other) {
    // Have to lock it this way as other could call it's own equal function which
    // would try to lock in reverse order.
    std::lock(_mapMtx, other._mapMtx);
    std::lock_guard<std::mutex> lk1(_mapMtx,       std::adopt_lock);
    std::lock_guard<std::mutex> lk2(other._mapMtx, std::adopt_lock);

    if (_nameMap.size() != other._nameMap.size()) {
        LOGS(_log, LOG_LVL_INFO, "&&& map sizes do not match this=" << _nameMap.size() << " other=" << other._nameMap.size());
        return false;
    }
    auto thisIter = _nameMap.begin();
    auto otherIter = other._nameMap.begin();
    for (;thisIter != _nameMap.end() && otherIter != other._nameMap.end();
            ++thisIter, ++otherIter) {
        if (thisIter->first != otherIter->first) {
            LOGS(_log, LOG_LVL_INFO, "&&& map first not equal");
            return false;
        }
        if (not thisIter->second->equal(*(otherIter->second))) {
            LOGS(_log, LOG_LVL_INFO, "&&& map second not equal");
            return false;
        }
    }
    return true;
}


std::string WWorkerList::dump() const {
    std::stringstream os;
    os << "WWorkerList name:\n";
    {
        std::lock_guard<std::mutex> lck(_mapMtx);
        for (auto elem:_nameMap) {
            os << "  " << *elem.second << "\n";
        }

        os << "WWorkerList ip:\n";
        for (auto elem:_ipMap) {
            os << "  " << *elem.second << "\n";
        }
    }
    return os.str();
}


/// There must be a name. However, ip, port, and range may be invalid.
//  TODO believe our neighbors range over the master
void WWorkerList::updateEntry(uint32_t name,
                              std::string const& ip, int portUdp, int portTcp,
                              StringRange& strRange) {
    std::unique_lock<std::mutex> lk(_mapMtx);
    auto iter = _nameMap.find(name);
    if (iter == _nameMap.end()) {
        // This should rarely happen, make an entry for it
        auto newItem = WWorkerListItem::create(name, _central);
        auto res = _nameMap.insert(std::make_pair(name, newItem));
        iter = res.first;
        LOGS(_log, LOG_LVL_INFO, "updateEntry created entry for name=" << name <<
                                 " res=" << res.second);
    }
    WWorkerListItem::Ptr const& item = iter->second;
    if (ip != "") {
        if (item->getAddressUdp().ip == "" ) {
            item->setUdpAddress(ip, portUdp);
            item->setTcpAddress(ip, portTcp);
            NetworkAddress nAddr(ip, portUdp);
            auto res = _ipMap.insert(std::make_pair(nAddr, item));
            LOGS(_log, LOG_LVL_INFO, "updateEntry set name=" << name << " Udp=" << nAddr <<
                                     " res=" << res.second);
        }
    }


    // TODO probably special action should be taken if this is our name.
    LOGS(_log, LOG_LVL_INFO, "&&&& updateEntry strRange=" << strRange);
    if (strRange.getValid()) {
        // Does the new range match the old range?
        auto oldRange = item->setRangeStr(strRange);
        LOGS(_log, LOG_LVL_INFO, "updateEntry set name=" << name << " range=" << strRange);
        if (not oldRange.equal(strRange)) {
            // Since the value changed, it needs to be removed and reinserted.
            // No invalid ranges should be in the map.
            if (oldRange.getValid()) {
                // The old value was valid, so it is likely in the map.
                auto rangeIter = _rangeMap.find(oldRange);
                if (rangeIter != _rangeMap.end()) {
                    _rangeMap.erase(rangeIter);
                }
            }
            if (strRange.getValid()) {
                _rangeMap[strRange] = item;
            }
        }
    }
}


WWorkerListItem::Ptr WWorkerList::findWorkerForKey(std::string const& key) {
    std::unique_lock<std::mutex> lk(_mapMtx);
    // TODO Really could use a custom container for _rangeMap to speed this up.
    for (auto const& elem : _rangeMap) {
        LOGS(_log, LOG_LVL_INFO, "&&& WWorkerList::findWorkerForKey checking " << elem.first << " -> " << *(elem.second));
        if (elem.second->containsKey(key)) {
            return elem.second;
        }
    }
    LOGS(_log, LOG_LVL_WARN, "WWorkerList::findWorkerForKey did not find worker for key=" << key);
    return nullptr;
}



void WWorkerListItem::addDoListItems(Central *central) {
    LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::addDoListItems");
    std::lock_guard<std::mutex> lck(_mtx);
    if (_workerUpdateNeedsMasterData == nullptr) {
        _workerUpdateNeedsMasterData.reset(new WorkerNeedsMasterData(shared_from_this(), _central));
        central->addDoListItem(_workerUpdateNeedsMasterData);
    }
}


bool WWorkerListItem::equal(WWorkerListItem& other) {
    std::lock(_mtx, other._mtx);
    std::lock_guard<std::mutex> lck1(_mtx,       std::adopt_lock);
    std::lock_guard<std::mutex> lck2(other._mtx, std::adopt_lock);
    if (_name != other._name) {
        LOGS(_log, LOG_LVL_INFO, "&&& item name not equal t=" << _name << " o=" << other._name);
        return false;
    }
    if (*_udpAddress != *other._udpAddress) {
        LOGS(_log, LOG_LVL_INFO, "&&& item addr != name=" << _name <<
                                 " t=" << *_udpAddress << " o=" << *other._udpAddress);
        return false;
    }
    /// TODO add range check &&&
    if (not _range.equal(other._range)) {
        LOGS(_log, LOG_LVL_INFO, "&&& item range != name=" << _name <<
                                 " t=" << _range << " o=" << other._range);
    }
    return true;
}


void WWorkerListItem::setUdpAddress(std::string const& ip, int port) {
    std::lock_guard<std::mutex> lck(_mtx);
    _udpAddress.reset(new NetworkAddress(ip, port));
    LOGS(_log, LOG_LVL_INFO, "set udpAddress name=" << _name << " addr=" << *_udpAddress);
}


void WWorkerListItem::setTcpAddress(std::string const& ip, int port) {
    std::lock_guard<std::mutex> lck(_mtx);
    _tcpAddress.reset(new NetworkAddress(ip, port));
    LOGS(_log, LOG_LVL_INFO, "set tcpAddress name=" << _name << " addr=" << *_tcpAddress);
}

StringRange WWorkerListItem::setRangeStr(StringRange const& strRange) {
    std::lock_guard<std::mutex> lck(_mtx);
    /* &&&
    if (strRange.equal(_range)) { return true; }
    _range = strRange;
    LOGS(_log, LOG_LVL_INFO, "setRangeStr name=" << _name << " range=" << _range);
    return false;
    */
    auto oldRange = _range;
    _range = strRange;
    LOGS(_log, LOG_LVL_INFO, "setRangeStr name=" << _name << " range=" << _range <<
                             " oldRange=" << oldRange);
    return oldRange;
}

StringRange WWorkerListItem::getRange() const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _range;
}


util::CommandTracked::Ptr WWorkerListItem::WorkerNeedsMasterData::createCommand() {
    auto item = wWorkerListItem.lock();
    if (item == nullptr) {
        // TODO: should mark set the removal flag for this doListItem
        return nullptr;
    }
    return item->createCommandWorkerInfoReq(central);
}

util::CommandTracked::Ptr WWorkerListItem::createCommandWorkerInfoReq(CentralWorker* centralW) {
     // Create a command to put on the pool to
     //  - ask the master about a server with our _name

    class WorkerReqCmd : public util::CommandTracked {
    public:
        WorkerReqCmd(CentralWorker* centralW, uint32_t name) : _centralW(centralW), _name(name) {}

        void action(util::CmdData *data) override {
            /// Request all information the master has for one worker.
            LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::createCommand::WorkerReqCmd::action " <<
                    "ourName=" << _centralW->getOurLogId() << " req name=" << _name);

            // TODO make a function for this, it's always going to be the same.
            proto::LdrNetAddress protoOurAddress;
            protoOurAddress.set_ip(_centralW->getHostName());
            protoOurAddress.set_udpport(_centralW->getUdpPort());
            protoOurAddress.set_tcpport(_centralW->getTcpPort());
            StringElement eOurAddress(protoOurAddress.SerializeAsString());

            proto::WorkerListItem protoItem;
            protoItem.set_name(_name);
            StringElement eItem(protoItem.SerializeAsString());

            LoaderMsg workerInfoReqMsg(LoaderMsg::MAST_WORKER_INFO_REQ, _centralW->getNextMsgId(),
                                       _centralW->getHostName(), _centralW->getUdpPort());
            BufferUdp sendBuf(1000);
            workerInfoReqMsg.serializeToData(sendBuf);
            eOurAddress.appendToData(sendBuf);
            eItem.appendToData(sendBuf);

            // Send the request to master.
            auto masterHost = _centralW->getMasterHostName();
            auto masterPort = _centralW->getMasterPort();
            LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::createCommand::WorkerReqCmd::action sending " << _centralW->getOurLogId());
            _centralW->sendBufferTo(masterHost, masterPort, sendBuf);
            LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::createCommand::WorkerReqCmd::action sent" << _centralW->getOurLogId());
        }

    private:
        CentralWorker* _centralW;
        uint32_t _name;
    };

    LOGS(_log, LOG_LVL_INFO, "&&& WWorkerListItem::createCommandWorker this=" << centralW->getOurLogId() << " name=" << _name);
    return std::make_shared<WorkerReqCmd>(centralW, _name);
}

bool WWorkerListItem::containsKey(std::string const& key) const {
    std::lock_guard<std::mutex> lck(_mtx);
    return _range.isInRange(key);
}


std::ostream& operator<<(std::ostream& os, WWorkerListItem const& item) {
    os << "name=" << item._name << " address=" << *(item._udpAddress) << " range(" << item._range << ")";
    return os;
}


}}} // namespace lsst::qserv::loader



