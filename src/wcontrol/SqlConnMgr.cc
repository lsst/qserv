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

// qserv headers
#include "wbase/SendChannelShared.h"

#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.SqlConnMgr");
}

using namespace std;

namespace lsst {
namespace qserv {
namespace wcontrol {


SqlConnMgr::ConnType SqlConnMgr::_take(bool scanQuery,
                                       std::shared_ptr<wbase::SendChannelShared> const& sendChannelShared,
                                       bool firstChannelSqlConn) {
    ++_totalCount;
    std::unique_lock<std::mutex> uLock(_mtx);

    SqlConnMgr::ConnType connType = SCAN;
    if (not scanQuery) {
        // high priority interactive queries
        connType = INTERACTIVE;
    } else if (firstChannelSqlConn) {
        // normal shared scan, low priority as far as SqlConnMgr is concerned.
        connType = SCAN;
    } else {
        // SendChannelShared, every SQL connection after the first one.
        // High priority to SqlConnMgr as these need to run to free up resources.
        if (sendChannelShared != nullptr) {
            connType = SHARED;
        } else {
            connType = SCAN;
        }
    }

    _tCv.wait(uLock, [this, scanQuery, sendChannelShared, connType](){
        bool ok = false;
        switch (connType) {
            case INTERACTIVE:
                ok = _sqlSharedConnCount < _maxSqlSharedConnections;
                break;
            case SHARED:
                // High priority, but only if at least one has already gotten
                // a connection.
                if (sendChannelShared->getSqlConnectionCount() > 0) {
                    ok = _sqlSharedConnCount < _maxSqlSharedConnections;
                } else {
                    ok = false;
                }
                break;
            case SCAN:
                // _maxSqlScanConnections should be much smaller than _maxSqlSharedConnections
                ok = _sqlScanConnCount < _maxSqlScanConnections;
                break;
            default:
                throw Bug("SqlConnMgr::_take unexpected type " + to_string(connType));
        }
        return ok;
    });

    // requestor got its sql connection, increment counts
    if (sendChannelShared != nullptr) {
        int newCount = sendChannelShared->incrSqlConnectionCount();
        LOGS(_log, LOG_LVL_DEBUG, "SqlConnMgr::_take newCount=" << newCount);
    }

    if (connType == SCAN) {
        ++_sqlScanConnCount;
    } else {
        ++_sqlSharedConnCount;
    }
    return connType;
}


void SqlConnMgr::_release(SqlConnMgr::ConnType connType) {
    // The sendChannelShared count does not get decremented. Once it has started
    // transmitting sendChannelShared must be allowed to continue or xrootd could
    // block and lead to deadlock.
    // Decrementing the sendChannelShared count could result in the count
    // being 0 before all transmits on the sendChannelShared have finished,
    // causing _take() to block when it really should not.
    // When the SendChannel is finished, it is thrown away, effectively clearing
    // its count.
    if (connType == SCAN) {
        --_sqlScanConnCount;
    } else {
        --_sqlSharedConnCount;
    }
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
       << " sqlScanConnCount=" << _sqlScanConnCount
       << ":max=" << _maxSqlScanConnections
       << " sqlSharedConnCount=" << _sqlSharedConnCount
       << ":max=" << _maxSqlSharedConnections << ")";
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


SqlConnLock::SqlConnLock(SqlConnMgr& sqlConnMgr, bool scanQuery,
            std::shared_ptr<wbase::SendChannelShared> const& sendChannelShared)
  : _sqlConnMgr(sqlConnMgr) {
    bool firstChannelSqlConn = true;
    if (sendChannelShared != nullptr) {
        firstChannelSqlConn = sendChannelShared->getFirstChannelSqlConn();
    }
    _connType = _sqlConnMgr._take(scanQuery, sendChannelShared, firstChannelSqlConn);
}


}}} // namespace lsst::qserv::wcontrol
