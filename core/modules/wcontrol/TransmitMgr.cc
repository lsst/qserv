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
#include "global/Bug.h"

#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.TransmitMgr");
}

namespace lsst {
namespace qserv {
namespace wcontrol {

int LockCount::_take() {
    {
        unique_lock<mutex> uLock(_lMtx);
        ++_totalCount;
        _lCv.wait(uLock, [this](){ return (_count < _maxCount); });
        ++_count;
    }
    return _totalCount;
}


int LockCount::_release() {
    int totalCount = 0;
    {
        unique_lock<mutex> uLock(_lMtx);
        --_totalCount;
        --_count;
        if (_totalCount <=0 && _count > 0) {
            throw Bug("LockCount::_release() _count > _totalCount "
                      + to_string(_count ) + " > " + to_string(_totalCount) );
        }
        totalCount = _totalCount;
    }
    _lCv.notify_one();
    return totalCount;
}


void QidMgr::_take(QueryId const& qid) {
    LockCount* lockCount;
    {
        lock_guard<mutex> uLock(_mapMtx);
        lockCount = &(_qidLocks[qid]);
    }
    lockCount->_take();
}


void QidMgr::_release(QueryId const& qid) {
    LockCount* lockCount;
    bool changed = false;
    int qidsSize = 0;
    int tCount = 0;
    {
        lock_guard<mutex> uLock(_mapMtx);
        lockCount = &(_qidLocks[qid]);
        tCount = lockCount->_release();
        if (tCount == 0) {
            _qidLocks.erase(qid);
            changed = true;
            qidsSize = _qidLocks.size();
        }
    }
    if (changed) {
        LOGS(_log, LOG_LVL_DEBUG, "QidMgr::_release freed counts for " << qid << " diffQids=" << qidsSize);
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "QidMgr::_release total counts for " << qid << " =" << tCount);
    }
}


void TransmitMgr::_take(bool interactive) {
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_take locking " << *this);
    unique_lock<mutex> uLock(_mtx);
    ++_totalCount;
    if (not interactive || _transmitCount >= _maxTransmits) {
        _tCv.wait(uLock, [this](){ return (_transmitCount < _maxTransmits); });
    }
    ++_transmitCount;
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_take locking done " << *this);
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
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_release locking done " << *this);
    _tCv.notify_all();
}


ostream& TransmitMgr::dump(ostream &os) const {
    os << "(totalCount=" << _totalCount
       << " transmitCount=" << _transmitCount
       << ":max=" << _maxTransmits << ")";
    return os;
}


string TransmitMgr::dump() const {
    ostringstream os;
    dump(os);
    return os.str();
}


ostream& operator<<(ostream &os, TransmitMgr const& mgr) {
    return mgr.dump(os);
}


}}} // namespace lsst::qserv::wcontrol
