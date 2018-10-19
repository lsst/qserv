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
#include "loader/CentralMaster.h"

// system headers
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers

// qserv headers

// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.CentralMaster");
}


namespace lsst {
namespace qserv {
namespace loader {


void CentralMaster::addWorker(std::string const& ip, int udpPort, int tcpPort) {
    // LOGS(_log, LOG_LVL_INFO, "&&& Master::addWorker");
    auto item = _mWorkerList->addWorker(ip, udpPort, tcpPort);

    if (item != nullptr) {
        // If that was the first worker added, it gets unlimited range.
        if (_firstWorkerRegistered.exchange(true) == false) {
            LOGS(_log, LOG_LVL_INFO, "setAllInclusiveRange for name=" << item->getName());
            item->setAllInclusiveRange();
        }

        item->addDoListItems(this);
        LOGS(_log, LOG_LVL_INFO, "Master::addWorker " << *item);
    }
}


void CentralMaster::updateNeighbors(uint32_t workerName, NeighborsInfo const& nInfo) {
    if (workerName == 0) {
        return;
    }
    auto item = getWorkerNamed(workerName);
    if (item == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralMaster::updateNeighbors nullptr for workerName=" << workerName);
        return;
    }
    item->setKeyCounts(nInfo);
    _assignNeighborIfNeeded();
}


void CentralMaster::setWorkerNeighbor(MWorkerListItem::WPtr const& target, int message, uint32_t neighborName) {
    // Get the target worker's network address
    auto targetWorker = target.lock();
    LOGS(_log, LOG_LVL_INFO, "&&& CentralMaster::setWorkerNeighbor a " << neighborName);
    if (targetWorker == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralMaster::setWorkerNeighbor nullptr for" << neighborName);
        return;
    }

    LOGS(_log, LOG_LVL_INFO, "&&& CentralMaster::setWorkerNeighbor b " << neighborName << " " << *targetWorker);
    // Build and send the message
    LoaderMsg msg(message, getNextMsgId(), getMasterHostName(), getMasterPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    UInt32Element neighborNameElem(neighborName);
    neighborNameElem.appendToData(msgData);
    auto addr = targetWorker->getUdpAddress();
    sendBufferTo(addr.ip, addr.port, msgData);
}


void CentralMaster::_assignNeighborIfNeeded() {
    LOGS(_log, LOG_LVL_INFO, "&&& CentralMaster::_assignNeighborIfNeeded");
    // Go through the list and see if all the workers are full.
    // If they are, assign a worker to the end (rightmost) worker
    // and increase the maximum by an order of magnitude, max 10 million.
    // TODO Make a better algorithm, insert workers at busiest worker.
    // TODO maybe rate limit this check.
    std::string funcName("_assignNeighborIfNeeded");
    if (_addingWorker) {
        // Already in process of adding a worker, so don't add another one.
        // check if it failed &&& IMPORTANT
        return;
    }
    // Only one thread should ever be working on this logic at a time.
    std::lock_guard<std::mutex> lck(_assignMtx);
    auto pair = _mWorkerList->getActiveInactiveWorkerLists();
    std::vector<MWorkerListItem::Ptr>& activeList = pair.first;
    std::vector<MWorkerListItem::Ptr>& inactiveList = pair.second;
    if (inactiveList.empty()) { return; } // not much point if no workers to assign.
    double sum = 0.0;
    int max = 0;
    uint32_t maxName = 0;
    uint32_t rightMostName = 0; // Name of the rightmost worker, unlimited upper range.
    MWorkerListItem::Ptr rightMostItem;
    for(auto& item : activeList) {
        int keyCount = item->getKeyCount();
        sum += keyCount;
        if (keyCount > max) {
            max = keyCount;
            maxName = item->getName();
        }
        auto range = item->getRangeString();
        if (range.getValid() && range.getUnlimited()) {
            if (rightMostName != 0) {
                LOGS(_log, LOG_LVL_ERROR, "_assignNeighborIfNeeded Multiple rightMost workers " <<
                                           " name=" << rightMostName <<
                                           " name=" << item->getName());
                throw LoaderMsgErr(funcName + " Multiple rightMost workers " +
                        " name=" + std::to_string(rightMostName) +
                        " name=" + std::to_string(item->getName()),
                        __FILE__, __LINE__);
            }
            rightMostName = item->getName();
            rightMostItem = item;
        }
    }
    double avg = sum/(double)(activeList.size());
    LOGS(_log, LOG_LVL_INFO, "max=" << max << " maxName=" << maxName << " avg=" << avg);
    if (avg > getMaxKeysPerWorker()) {
        // Assign a neighbor to the rightmost worker, if there are any unused nodes.
        // TODO Probably better to assign a new neighbor next to the node with the most recent activity.
        //      but that's much more complicated.
        LOGS(_log, LOG_LVL_INFO, "\n\n\n\n\n\n\n\n\n &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&" <<
                 "\n &&&   ADDING WORKER avg=" << avg);
        auto inactiveItem = inactiveList.front();
        if (inactiveItem == nullptr) {
            throw LoaderMsgErr(funcName + " _assignNeighborIfNeeded unexpected inactiveList nullptr",
                               __FILE__, __LINE__);
            return;
        }
        _addingWorker = true;
        /// Fun part !!! &&&
        // Sequence of events goes something like
        // 1) left item gets message from master that it is getting a right neighbor, and writes it down
        // 2) Right item get message from master that it is getting a left neighbor, writes it down.
        // 3) Right connects to left item, which should be expecting it. (keeps retrying for a minute &&& check that it only tries)
        // 4) left item sends its largest key value to right
        // 5) right node sets it range to rightMost and whatever left sent it + 1  (must make max unsigned int illegal, strings, add a 0 to the end)
        //    5a) verifies this with master.
        // 6) right indicates it is valid
        // 7) left verifies. -  done. - shifting can now occur between nodes. -- set _addingWorker = false
        // failure) ... if it goes 2 minutes without completing, cleanup + try different worker -- maybe set addingWorker = false

        // Steps 1 and 2
        rightMostItem->setRightNeighbor(inactiveItem);
        inactiveItem->setLeftNeighbor(rightMostItem);
    }
}

MWorkerListItem::Ptr CentralMaster::getWorkerNamed(uint32_t name) {
    return _mWorkerList->getWorkerNamed(name);
}


void CentralMaster::reqWorkerKeysInfo(uint64_t msgId, std::string const& ip, short port,
                                     std::string const& ourHostName, short ourPort) {
    LoaderMsg reqMsg(LoaderMsg::WORKER_KEYS_INFO_REQ, msgId, ourHostName, ourPort);
    BufferUdp data;
    reqMsg.serializeToData(data);
    sendBufferTo(ip, port, data);
}

}}} // namespace lsst::qserv::loader
