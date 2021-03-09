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


int TransmitMgr::getAlreadyTransCount(qmeta::CzarId czarId) const {
    unique_lock<mutex> uLock(_mtx);
    auto const& iter = _czarTransmitMap.find(czarId);
    if (iter != _czarTransmitMap.end()) {
        return iter->second._alreadyTransCount;
    }
    return 0;
}


void TransmitMgr::_take(bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId) {
    unique_lock<mutex> uLock(_mtx);
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr take locking " << dump());
    TransmitInfo& info = _czarTransmitMap[czarId];
    ++(info._takeCalls);
    ++(info._totalCount);
    // Check if the caller needs to wait.
    // This is trying to prioritize transmits that are 'alreadyTransmitting' so
    // they finish and stop using system resources on the czar (with czarId).
    // '_maxAlreadyTran' should be significantly smaller than '_maxTransmits', which
    // causes all new transmits to have to wait until some of the already transmitting
    // items have cleared.
    // '_maxTransmits' may be 50 while '_maxAlreadyTran' may be 10.
    // Interactive transmits never need to wait.
    if (not interactive
        || info._transmitCount >= _maxTransmits
        || info._alreadyTransCount >= _maxAlreadyTran) {
        // If not alreadyTransmitting, it needs to wait until the number of already transmitting
        // jobs drops below _maxAlreadyTran before it can start transmitting.
        if (alreadyTransmitting) {
            ++(info._alreadyTransCount);
            LOGS(_log, LOG_LVL_DEBUG, "czar=" << czarId
                 << " ++_alreadyTransCount=" << info._alreadyTransCount);
            _tCv.wait(uLock, [this, &info](){ return info._transmitCount < _maxTransmits; });
        } else {
            _tCv.wait(uLock, [this, &info](){
                return (info._transmitCount < _maxTransmits)
                        && (info._alreadyTransCount < _maxAlreadyTran);
            });
        }
    }
    ++(info._transmitCount);
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
            --(info._transmitCount);
            if (not interactive && alreadyTransmitting) {
                --(info._alreadyTransCount);
            }
            // If _doNotDelete is false and all the counts are 0, delete it from the map.
            // it is possible for _takeCalls to be >0 and all other values be zero if
            // _take is waiting.
            if (info._takeCalls == 0 && info._totalCount == 0
                && info._transmitCount == 0 && info._alreadyTransCount == 0) {
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
    os << "maxTransmits=" << _maxTransmits << " maxAlreadyTransmitting=" << _maxAlreadyTran;
    for (auto const& iter:_czarTransmitMap) {
        auto const& czarId = iter.first;
        auto const& info = iter.second;
        os << "(czar=" << czarId
           << " totalC=" << info._totalCount
           << " transmitC=" << info._transmitCount
           << " alreadyTransC=" << info._alreadyTransCount
           << " takeCalls=" << info._takeCalls << ")";
    }
    return os;
}


ostream& operator<<(ostream &os, TransmitMgr const& mgr) {
    return mgr.dump(os);
}


}}} // namespace lsst::qserv::wcontrol
