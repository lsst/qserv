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

// Qserv headers
#include "util/Bug.h"

#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.TransmitMgr");
}

namespace lsst::qserv::wcontrol {

// _mapMtx must be locked before calling, except for constructor.
void QidMgr::_setMaxCount(int uniqueQidCount) {
    // There's no point in doing anything for uniqueQidCount < 1
    if (uniqueQidCount <= 0) uniqueQidCount = 1;
    // If nothing changed, return.
    if (uniqueQidCount == _prevUniqueQidCount) return;
    _prevUniqueQidCount = uniqueQidCount;
    // _maxCount must be > 0 and <= _maxPerQid
    // Otherwise, it should try to give an equal number of transmits to each QID
    int maxCount = _maxTransmits / uniqueQidCount;
    if (maxCount < 1) maxCount = 1;
    if (maxCount > _maxPerQid) maxCount = _maxPerQid;
    if (_maxCount == maxCount) return;
    bool notify = maxCount > _maxCount;
    _maxCount = maxCount;
    // send the new value to all LockCounts in the map.
    for (auto&& elem : _qidLocks) {
        LockCount& lc = elem.second;
        lc.lcMaxCount.store(_maxCount);
        if (notify) lc.lcCv.notify_one();
    }
}

void QidMgr::_take(QueryId const& qid) {
    unique_lock<mutex> uLock(_mapMtx);
    LockCount& lockCount = _qidLocks[qid];
    lockCount.lcMaxCount.store(_maxCount);
    _setMaxCount(_qidLocks.size());
    uLock.unlock();
    lockCount.take();
}

void QidMgr::_release(QueryId const& qid) {
    LockCount* lockCount;
    bool changed = false;
    int qidsSize = 0;
    int tCount = 0;
    {
        lock_guard<mutex> uLock(_mapMtx);
        lockCount = &(_qidLocks[qid]);
        tCount = lockCount->release();
        if (tCount == 0) {
            _qidLocks.erase(qid);
            changed = true;
            qidsSize = _qidLocks.size();
            _setMaxCount(qidsSize);
        }
    }
    if (changed) {
        LOGS(_log, LOG_LVL_DEBUG, "QidMgr::_release freed counts for " << qid << " diffQids=" << qidsSize);
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QidMgr::_release total counts for " << qid << " =" << tCount);
    }
}

int QidMgr::LockCount::take() {
    {
        unique_lock<mutex> uLock(lcMtx);
        ++lcTotalCount;
        lcCv.wait(uLock, [this]() { return (lcCount < lcMaxCount); });
        ++lcCount;
    }
    return lcTotalCount;
}

int QidMgr::LockCount::release() {
    int totalCount = 0;
    {
        unique_lock<mutex> uLock(lcMtx);
        --lcTotalCount;
        --lcCount;
        if (lcTotalCount <= 0 && lcCount > 0) {
            throw util::Bug(ERR_LOC, "LockCount::_release() _count > _totalCount " + to_string(lcCount) +
                                             " > " + to_string(lcTotalCount));
        }
        totalCount = lcTotalCount;
    }
    lcCv.notify_one();
    return totalCount;
}

void TransmitMgr::_take(bool interactive) {
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_take locking " << *this);
    unique_lock<mutex> uLock(_mtx);
    ++_totalCount;
    if (not interactive || _transmitCount >= _maxTransmits) {
        _tCv.wait(uLock, [this]() { return (_transmitCount < _maxTransmits); });
    }
    ++_transmitCount;
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr take locking done " << dump());
}

void TransmitMgr::_release(bool interactive) {
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_release locking " << *this);
    {
        unique_lock<mutex> uLock(_mtx);
        --_totalCount;
        --_transmitCount;
    }
    // There could be several threads waiting on _alreadyTransCount or
    // it needs to make sure to wake the thread waiting only on _transmitCount.
    _tCv.notify_all();
}

ostream& TransmitMgr::dump(ostream& os) const {
    os << "(totalCount=" << _totalCount << " transmitCount=" << _transmitCount << ":max=" << _maxTransmits
       << ")";
    return os;
}

std::string TransmitMgr::dump() const {
    std::ostringstream os;
    dumpBase(os);
    return os.str();
}

ostream& TransmitMgr::dumpBase(ostream& os) const {
    // No mtx as long as everything is atomic. Some risk
    // of counts from different threads.
    os << "maxTransmits=" << _maxTransmits << "(totalC=" << _totalCount << " transmitC=" << _transmitCount
       << ")";
    return os;
}

ostream& operator<<(ostream& os, TransmitMgr const& mgr) { return mgr.dump(os); }

}  // namespace lsst::qserv::wcontrol
