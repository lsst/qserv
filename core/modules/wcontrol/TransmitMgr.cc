// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2020 LSST.
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
#include "wcontrol/TransmitMgr.h"

#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.TransmitMgr");
}

namespace lsst {
namespace qserv {
namespace wcontrol {


int TransmitMgr::getTotalCount(qmeta::CzarId czarId) const {
    unique_lock<mutex> uLock(_mtx);
    auto const& iter = _czarTransmitMap.find(czarId);
    if (iter != _czarTransmitMap.end()) {
        return iter->second._totalCount;
    }
    return 0;
}


int TransmitMgr::getTransmitCount(qmeta::CzarId czarId) const {
    unique_lock<mutex> uLock(_mtx);
    auto const& iter = _czarTransmitMap.find(czarId);
    if (iter != _czarTransmitMap.end()) {
        return iter->second._transmitCount;
    }
    return 0;
}


void TransmitMgr::_take(bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId) {
    unique_lock<mutex> uLock(_mtx);
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr take locking " << dump());
    TransmitInfo& info = _czarTransmitMap[czarId];

    ++(info._takeCalls);
    ++(info._totalCount);
    if (not interactive) {
        // Check if the caller needs to wait.
        // Interactive transmits never need to wait.
        if (info._transmitCount >= _maxTransmits) {
            _tCv.wait(uLock, [this, &info](){ return info._transmitCount < _maxTransmits; });
        }
        ++(info._transmitCount);
    }
    --(info._takeCalls);

    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr take locking done " << dump());
}

void TransmitMgr::_release(bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId) {
    {
        bool eraseInfo = false;
        unique_lock<mutex> uLock(_mtx);
        LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr release locking " << dump());
        auto const& iter = _czarTransmitMap.find(czarId);
        if (iter != _czarTransmitMap.end()) {
            auto& info = iter->second;
            --(info._totalCount);
            if (not interactive) {
                --(info._transmitCount);
            }
            // If _doNotDelete is false and all the counts are 0, delete it from the map.
            // it is possible for _takeCalls to be >0 and all other values be zero if
            // _take is waiting.
            if (info._takeCalls == 0 && info._totalCount == 0 && info._transmitCount == 0) {
                eraseInfo = true;
            }
        }
        if (eraseInfo) {
            LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr release erasing Info for " << czarId);
            _czarTransmitMap.erase(iter);
        }
        LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr release locking done " << dump());
    }
    // There could be several threads waiting on _alreadyTransCount or
    // it needs to make sure to wake the thread waiting only on _transmitCount.
    _tCv.notify_all();
}


ostream& TransmitMgr::dump(ostream &os) const {
    lock_guard<mutex> lock(_mtx);
    return dumpBase(os);
}


std::string TransmitMgr::dump() const {
    // Thread must hold _mtx before calling this.
    std::ostringstream os;
    dumpBase(os);
    return os.str();
}


ostream& TransmitMgr::dumpBase(ostream &os) const {
    // Thread must hold _mtx before calling this.
    os << "maxTransmits=" << _maxTransmits;
    for (auto const& iter:_czarTransmitMap) {
        auto const& czarId = iter.first;
        auto const& info = iter.second;
        os << "(czar=" << czarId
           << " totalC=" << info._totalCount
           << " transmitC=" << info._transmitCount
           << " takeCalls=" << info._takeCalls << ")";
    }
    return os;
}


ostream& operator<<(ostream &os, TransmitMgr const& mgr) {
    return mgr.dump(os);
}


}}} // namespace lsst::qserv::wcontrol
