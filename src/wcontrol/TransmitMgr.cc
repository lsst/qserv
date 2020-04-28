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


void TransmitMgr::_take(bool interactive, bool alreadyTransmitting) {
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_take locking " << *this);
    unique_lock<mutex> uLock(_mtx);
    ++_totalCount;
    if (not interactive || _transmitCount >= _maxTransmits || _alreadyTransCount >= _maxAlreadyTran) {
        // If not alreadyTransmitting, it needs to wait until the number of already transmitting
        // jobs drops below _maxAlreadyTran before it can start transmitting.
        if (alreadyTransmitting) {
            ++_alreadyTransCount;
            LOGS(_log, LOG_LVL_DEBUG, " ++_alreadyTransCount=" << _alreadyTransCount);
            _tCv.wait(uLock, [this](){ return _transmitCount < _maxTransmits; });
        } else {
            _tCv.wait(uLock, [this](){
                return (_transmitCount < _maxTransmits) && (_alreadyTransCount < _maxAlreadyTran);
            });
        }
    }
    ++_transmitCount;
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_take locking done " << *this);
}


void TransmitMgr::_release(bool interactive, bool alreadyTransmitting) {
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_release locking " << *this);
    {
        unique_lock<mutex> uLock(_mtx);
        --_totalCount;
        --_transmitCount;
        if (not interactive && alreadyTransmitting) {
            --_alreadyTransCount;
            LOGS(_log, LOG_LVL_DEBUG, "--_alreadyTransCount=" << _alreadyTransCount);
        }
    }
    // There could be several threads waiting on _alreadyTransCount or
    // it needs to make sure to wake the thread waiting only on _transmitCount.
    LOGS(_log, LOG_LVL_DEBUG, "TransmitMgr::_release locking done " << *this);
    _tCv.notify_all();
}


ostream& TransmitMgr::dump(ostream &os) const {
    os << "(totalCount=" << _totalCount
       << " transmitCount=" << _transmitCount
       << ":max=" << _maxTransmits
       << " alreadyTransCount=" << _alreadyTransCount
       << ":max=" << _maxAlreadyTran << ")";
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
