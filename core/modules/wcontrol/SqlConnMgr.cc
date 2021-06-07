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
#include "wcontrol/SqlConnMgr.h"

#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.SqlConnMgr");
}

using namespace std;

namespace lsst {
namespace qserv {
namespace wcontrol {

void SqlConnMgr::_take(bool scanQuery) {
    ++_totalCount;
    std::unique_lock<std::mutex> uLock(_mtx);
    _tCv.wait(uLock, [this, scanQuery](){
        bool ok = _sqlConnCount < _maxScanSqlConnections;
        if (not scanQuery) {
            ok = _sqlConnCount < _maxSqlConnections;
        }
        return ok;
    });
    ++_sqlConnCount;
}


void SqlConnMgr::_release() {
    --_sqlConnCount;
    --_totalCount;
    // All threads threads must be checked as nothing will happen if one thread is
    // notified and it is waiting for _maxScanSqlConnections, but a different
    // thread could use _maxSqlConnections.
    // This shouldn't  hurt performance too much, since at any given time,
    // very few threads should be waiting. (They can only wait when first scheduled
    // and the scheduler is limited to about 20-30 threads.)
    // If things are backed up, it's terribly important to run any runable
    // threads found.
    _tCv.notify_all();
}


ostream& SqlConnMgr::dump(ostream &os) const {
    os << "(totalCount=" << _totalCount
       << " sqlConnCount=" << _sqlConnCount
       << ":max=" << _maxSqlConnections
       << " maxScanSqlConnections=" << _maxScanSqlConnections << ")";
    return os;
}


string SqlConnMgr::dump() const {
    ostringstream os;
    dump(os);
    return os.str();
}


ostream& operator<<(ostream &os, SqlConnMgr const& mgr) {
    return mgr.dump(os);
}

}}} // namespace lsst::qserv::wcontrol
