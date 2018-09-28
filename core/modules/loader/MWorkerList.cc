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
#include "loader/MWorkerList.h"

// System headers
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers


// qserv headers
#include "loader/CentralMaster.h"
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"


// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.MWorkerList");
}

namespace lsst {
namespace qserv {
namespace loader {



util::CommandTracked::Ptr SetNeighborOneShot::createCommand() {
    struct SetNeighborCmd : public util::CommandTracked {
        SetNeighborCmd(SetNeighborOneShot::Ptr const& ptr) : oneShotData(ptr) {}
        void action(util::CmdData*) override {
            auto data = std::dynamic_pointer_cast<SetNeighborOneShot>(oneShotData.lock());
            if (data != nullptr) {
                data->central->setWorkerNeighbor(data->target, data->message, data->neighborName);
            }
        }
        std::weak_ptr<SetNeighborOneShot> oneShotData;
    };
    auto ptr = std::dynamic_pointer_cast<SetNeighborOneShot>(getDoListItemPtr());
    return std::make_shared<SetNeighborCmd>(ptr);
}



util::CommandTracked::Ptr MWorkerListItem::createCommandMaster(CentralMaster* centralMaster) {
    LOGS(_log, LOG_LVL_ERROR, "&&& MWorkerListItem::createCommandMaster This function needs to do something!!!!!!!!!");
    // &&& ask worker for current range, neighbors, make sure worker is still alive
    return nullptr;
}


util::CommandTracked::Ptr MWorkerList::createCommand() {
    return createCommandMaster(_central);
}


util::CommandTracked::Ptr MWorkerList::createCommandMaster(CentralMaster* centralM) {
    // &&& The master probably doesn't need to make any checks on the list, it just
    // &&& wants to make sure all of its items are on the doList.
    return nullptr;
}


// Returns pointer to new item when new worker added, otherwise nullptr.
MWorkerListItem::Ptr MWorkerList::addWorker(std::string const& ip, int port) {
    NetworkAddress address(ip, port);

    // If it is already in the map, do not change its name.
    std::lock_guard<std::mutex> lock(_mapMtx);
    auto iter = _ipMap.find(address);
    if (iter != _ipMap.end()) {
        LOGS(_log, LOG_LVL_WARN, "addWorker, Could not add worker as worker already exists. " <<
                ip << ":" << port);
        return nullptr;
    }
    // Get an id and make new worker item
    auto workerListItem = MWorkerListItem::create(_sequenceName++, address, _central);
    _ipMap.insert(std::make_pair(address, workerListItem));
    _nameMap.insert(std::make_pair(workerListItem->getName(), workerListItem));
    LOGS(_log, LOG_LVL_INFO, "Added worker " << *workerListItem);
    _flagListChange();

    return workerListItem;
}

bool MWorkerList::sendListTo(uint64_t msgId, std::string const& ip, short port,
                            std::string const& ourHostName, short ourPort) {
    NetworkAddress address(ip, port);
    StringElement workerList;
    {
        std::lock_guard<std::mutex> lock(_mapMtx);
        if (_wListChanged || _stateListData == nullptr) {
            _wListChanged = false;
            /// At this time, all workers should easily fit in a single message.
            /// TODO send multiple messages (if needed) with each having the address and range of 100 workers.
            ///      This version is useful for testing. _stateListData becomes a vector.
            proto::LdrMastWorkerList protoList;
            protoList.set_workercount(_nameMap.size());
            for (auto const& item : _nameMap ) {
                proto::WorkerListItem* protoItem = protoList.add_worker();
                MWorkerListItem::Ptr wListItem = item.second;
                protoItem->set_name(wListItem->getName());
            }
            protoList.SerializeToString(&(workerList.element));
            LoaderMsg workerListMsg(LoaderMsg::MAST_WORKER_LIST, msgId, ourHostName, ourPort);
            _stateListData = std::make_shared<BufferUdp>();
            workerListMsg.serializeToData(*_stateListData);
            workerList.appendToData(*_stateListData);
        }
    }

    /* &&&
    // TODO: &&&(creating a client socket here is odd. Should use master socket to send or make a pool of contexts (pool of agents with contexts?)
    {
        using namespace boost::asio;
        io_context ioContext;
        ip::udp::resolver resolver(ioContext);
        ip::udp::socket socket(ioContext);
        socket.open(ip::udp::v4());
        ip::udp::endpoint endpoint = *resolver.resolve(ip::udp::v4(), ip, std::to_string(port)).begin(); // there has got to be a better way &&&
        socket.send_to(buffer(_stateListData->begin(), _stateListData->getCurrentWriteLength()), endpoint);
    }
    */
    _central->sendBufferTo(ip, port, *_stateListData);

    // See if this worker is know.
    MWorkerListItem::Ptr workerItem;
    {
        // See if this is a worker in our map
        std::lock_guard<std::mutex> lock(_mapMtx);
        auto iter = _ipMap.find(address);
        if (iter != _ipMap.end()) {
            workerItem = iter->second;
        }
    }
    if (workerItem != nullptr) {
        workerItem->sendListToWorkerInfoReceived();
    }
    return true;
}




std::pair<std::vector<MWorkerListItem::Ptr>, std::vector<MWorkerListItem::Ptr>>
MWorkerList::getActiveInactiveWorkerLists() {
    std::vector<MWorkerListItem::Ptr> active;
    std::vector<MWorkerListItem::Ptr> inactive;
    std::lock_guard<std::mutex> lck(_mapMtx);
    for(auto const& elem : _nameMap) {
        auto item = elem.second;
        if (item->isActive()) {
            active.push_back(item);
        } else {
            inactive.push_back(item);
        }
    }
    auto pair = std::make_pair(active, inactive);
    return pair;
}


// must lock _mapMtx before calling this function
void MWorkerList::_flagListChange() {
    _wListChanged = true;
    // TODO: &&& on Master only, flag each worker in the list that it needs to send an updated list to it's worker.
    for (auto const& elem : _nameMap) {
        auto const& item = elem.second;
        item->flagNeedToSendList();
    }
}


std::string MWorkerList::dump() const {
    std::stringstream os;
    os << "MWorkerList:\n";
    {
        std::lock_guard<std::mutex> lck(_mapMtx);
        for (auto elem:_nameMap) {
            os << "  " << *elem.second << "\n";
        }
        os << "MWorkerList ip:\n";
        for (auto elem:_ipMap) {
            os << "  " << *elem.second << "\n";
        }
    }
    return os.str();
}


void MWorkerListItem::addDoListItems(Central *central) {
    LOGS(_log, LOG_LVL_INFO, "&&& MWorkerListItem::addDoListItems a");
    std::lock_guard<std::mutex> lck(_doListItemsMtx);
    if (_sendListToWorker == nullptr) {
        LOGS(_log, LOG_LVL_INFO, "&&& MWorkerListItem::addDoListItems b");
        _sendListToWorker = std::make_shared<SendListToWorker>(shared_from_this(), _central);
        _central->addDoListItem(_sendListToWorker);
    }
    if (_reqWorkerKeyInfo == nullptr) {
        LOGS(_log, LOG_LVL_INFO, "&&& MWorkerListItem::addDoListItems c");
        _reqWorkerKeyInfo = std::make_shared<ReqWorkerKeyInfo>(shared_from_this(), _central);
        _central->addDoListItem(_reqWorkerKeyInfo);
    }
}


void MWorkerListItem::flagNeedToSendList() {
    auto slw = _sendListToWorker;
    if (slw != nullptr) { slw->setNeedInfo(); }
}


void MWorkerListItem::sendListToWorkerInfoReceived() {
    auto slw = _sendListToWorker;
    if (slw != nullptr) {
        // _sendListToWorker is a tough one to tell if the worker got the info, so
        // it is assumed that this worked when the list is sent. The worker
        // will either ask for it or it will be sent again later.
        slw->infoReceived();
    }
}


void MWorkerListItem::setRangeStr(StringRange const& strRange) {
    std::lock_guard<std::mutex> lck(_mtx);
    _range = strRange;
}


void MWorkerListItem::setAllInclusiveRange() {
    LOGS(_log, LOG_LVL_INFO, "&&& MWorkerListItem::setAllInclusiveRange for name=" << _name);
    std::lock_guard<std::mutex> lck(_mtx);
    _range.setAllInclusiveRange();
    _active = true;  /// First worker.
    LOGS(_log, LOG_LVL_INFO, "&&& MWorkerListItem::setAllInclusiveRange " << _range);
}


void MWorkerListItem::setKeyCounts(NeighborsInfo const& nInfo) { // &&& change name to updateNeighbor
    std::lock_guard<std::mutex> lck(_mtx);
    _neighborsInfo.keyCount = nInfo.keyCount;
    _neighborsInfo.recentAdds = nInfo.recentAdds;

    auto old = _neighborsInfo.neighborLeft->get();
    if (old != 0) {
        LOGS(_log, LOG_LVL_WARN, "Worker=" << _name <<
                "neighborLeft changing from valid old=" << old <<
                " to new=" << nInfo.neighborLeft->get());
    }
    if (old != nInfo.neighborLeft->get()) {
        LOGS(_log, LOG_LVL_INFO, "Worker=" << _name <<
                "neighborLeft=" << nInfo.neighborLeft->get());
    }
    _neighborsInfo.neighborLeft->update(nInfo.neighborLeft->get());


    old = _neighborsInfo.neighborRight->get();
    if (old != 0) {
        LOGS(_log, LOG_LVL_WARN, "Worker=" << _name <<
                "neighborRight changing from valid old=" << old <<
                " to new=" << nInfo.neighborRight->get());
    }
    if (old != nInfo.neighborRight->get()) {

        LOGS(_log, LOG_LVL_INFO, "Worker=" << _name <<
                "neighborRight=" << nInfo.neighborRight->get());
    }
    _neighborsInfo.neighborRight->update(nInfo.neighborRight->get());
}


int MWorkerListItem::getKeyCount() const {
    return _neighborsInfo.keyCount;
}


std::ostream& operator<<(std::ostream& os, MWorkerListItem const& item) {
    os << "name=" << item._name << " address=" << *item._address << " range(" << item._range << ")";
    return os;
}


/// Set this worker's RIGHT neighbor to the worker described in 'item'.
void MWorkerListItem::setRightNeighbor(MWorkerListItem::Ptr const& item) {
    // Create a one shot to send a message to the worker.
    // we know it has worked when the worker sends a message back saying it
    // has the correct right neighbor.
    LOGS(_log, LOG_LVL_INFO," &&& MWorkerListItem::setRightNeighbor");

    std::shared_ptr<SetNeighborOneShot> oneShot =
        std::make_shared<SetNeighborOneShot>(_central,
                                             shared_from_this(),
                                             LoaderMsg::WORKER_RIGHT_NEIGHBOR,
                                             item->getName(),
                                             _neighborsInfo.neighborRight);
    _central->addDoListItem(oneShot);
}


void MWorkerListItem::setLeftNeighbor(MWorkerListItem::Ptr const& item) {
    // Create a one shot to send a message to the worker.
    // we know it has worked when the worker sends a message back saying it
    // has the correct left neighbor.
    LOGS(_log, LOG_LVL_INFO," &&& MWorkerListItem::setLeftNeighbor");

    SetNeighborOneShot::Ptr oneShot =
        std::make_shared<SetNeighborOneShot>(_central,
                                             shared_from_this(),
                                             LoaderMsg::WORKER_LEFT_NEIGHBOR,
                                             item->getName(),
                                             _neighborsInfo.neighborLeft);
    _central->addDoListItem(oneShot);
}


util::CommandTracked::Ptr MWorkerListItem::SendListToWorker::createCommand() {
    auto item = mWorkerListItem.lock();
    if (item == nullptr) {
        // TODO: should mark set the removal flag for this doListItem
        return nullptr;
    }

    struct SendListToWorkerCmd : public util::CommandTracked {
        SendListToWorkerCmd(CentralMaster *centM_, MWorkerListItem::Ptr const& tItem_) : centM(centM_), tItem(tItem_) {}
        void action(util::CmdData*) override {
            LOGS(_log, LOG_LVL_INFO, "&&& SendListToWorkerCmd::action");
            centM->getWorkerList()->sendListTo(centM->getNextMsgId(),
                    tItem->_address->ip, tItem->_address->port,
                    centM->getMasterHostName(), centM->getMasterPort());
        }
        CentralMaster *centM;
        MWorkerListItem::Ptr tItem;
    };
    LOGS(_log, LOG_LVL_INFO, "&&& SendListToWorker::createCommand");
    return std::make_shared<SendListToWorkerCmd>(central, item);
}



util::CommandTracked::Ptr MWorkerListItem::ReqWorkerKeyInfo::createCommand() {
    auto item = mWorkerListItem.lock();
    if (item == nullptr) {
        // TODO: should mark set the removal flag for this doListItem
        return nullptr;
    }

    struct ReqWorkerKeysInfoCmd : public util::CommandTracked {
        ReqWorkerKeysInfoCmd(CentralMaster *centM_, MWorkerListItem::Ptr const& tItem_) : centM(centM_), tItem(tItem_) {}
        void action(util::CmdData*) override {
            LOGS(_log, LOG_LVL_INFO, "&&& ReqWorkerKeyInfoCmd::action");
            centM->reqWorkerKeysInfo(centM->getNextMsgId(),
                    tItem->_address->ip, tItem->_address->port,
                    centM->getMasterHostName(), centM->getMasterPort());
        }
        CentralMaster *centM;
        MWorkerListItem::Ptr tItem;
    };
    LOGS(_log, LOG_LVL_INFO, "&&& SendListToWorker::createCommand");
    return std::make_shared<ReqWorkerKeysInfoCmd>(central, item);
}


}}} // namespace lsst::qserv::loader






