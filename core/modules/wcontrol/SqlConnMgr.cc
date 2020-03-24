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
    _tCv.notify_one();
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
