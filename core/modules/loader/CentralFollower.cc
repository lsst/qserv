// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST.
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
#include "loader/CentralFollower.h"

// system headers
#include <boost/asio.hpp>
#include <iostream>

// qserv headers
#include "proto/loader.pb.h"
#include "proto/ProtoImporter.h"


// LSST headers
#include "lsst/log/Log.h"


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.CentralFollower");
}

namespace lsst {
namespace qserv {
namespace loader {


CentralFollower::~CentralFollower() {
    // Members that contain pointers to this. Deleting while this != null.
    // TODO: wait for reference count to drop to one on _wWorkerList
    _wWorkerList.reset();
}


void CentralFollower::startMonitoring() {
    LOGS(_log, LOG_LVL_INFO, "CentralFollower::startMonitoring");
    doList->addItem(_wWorkerList);
}


bool CentralFollower::workerInfoReceive(BufferUdp::Ptr const&  data) {
    // Open the data protobuffer and add it to our list.
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data, "CentralFollower::workerInfoReceive"));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralFollower::workerInfoRecieve Failed to parse list");
        return false;
    }
    std::unique_ptr<proto::WorkerListItem> protoList = sData->protoParse<proto::WorkerListItem>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralFollower::workerInfoRecieve Failed to parse list");
        return false;
    }

    // TODO: move this call to another thread
    _workerInfoReceive(protoList);
    return true;
}


void CentralFollower::_workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoL) {
    std::unique_ptr<proto::WorkerListItem> protoList(std::move(protoL));

    // Check the information, if it is our network address, set or check our id.
    // Then compare it with the map, adding new/changed information.
    uint32_t wId = protoList->wid();
    std::string ipUdp("");
    int portUdp = 0;
    int portTcp = 0;
    if (protoList->has_address()) {
        proto::LdrNetAddress protoAddr = protoList->address();
        ipUdp = protoAddr.ip();
        portUdp = protoAddr.udpport();
        portTcp = protoAddr.tcpport();
    }
    KeyRange strRange;
    if (protoList->has_range()) {
        proto::WorkerRange protoRange = protoList->range();
        bool valid        = protoRange.valid();
        if (valid) {
            CompositeKey min(protoRange.minint(), protoRange.minstr());
            CompositeKey max(protoRange.maxint(), protoRange.maxstr());
            bool unlimited = protoRange.maxunlimited();
            strRange.setMinMax(min, max, unlimited);
        }
    }

    checkForThisWorkerValues(wId, ipUdp, portUdp, portTcp, strRange);

    // Make/update entry in map.
    _wWorkerList->updateEntry(wId, ipUdp, portUdp, portTcp, strRange);
}


}}} // namespace lsst::qserv::loader
