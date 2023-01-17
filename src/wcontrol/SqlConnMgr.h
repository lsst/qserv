// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2020 LSST Corporation.
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

#ifndef LSST_QSERV_WCONTROL_SQLCONNMGR_H
#define LSST_QSERV_WCONTROL_SQLCONNMGR_H

// System headers
#include <atomic>
#include <exception>
#include <condition_variable>
#include <memory>
#include <mutex>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers

namespace lsst::qserv {

namespace wbase {
class SendChannelShared;
}

namespace wcontrol {

class SqlConnLock;

/// A way to limit the number of simultaneous MySQL connections related to
/// user queries and the worker scheduler. The total number of
/// maxSqlConnections should be significantly lower than MySQL max_connections
/// since other things may need to make connections to MySQL and running out
/// of connections is extremely painful for qserv.
/// The number of connections for shared scan connections
/// (maxScanSqlConnections)should be lower than the total (maxSqlConnections).
/// This allows interactive queries to go through even when shared scans
/// have the system heavily loaded.
///
class SqlConnMgr {
public:
    enum ConnType { INTERACTIVE = 0, SCAN = 1, SHARED = 2 };

    using Ptr = std::shared_ptr<SqlConnMgr>;
    SqlConnMgr(int maxSqlConnections, int maxScanSqlConnections) {
        if (maxSqlConnections <= 1 || maxScanSqlConnections <= 1 ||
            maxSqlConnections < maxScanSqlConnections) {
            throw std::invalid_argument(
                    std::string("SqlConnMgr maxSqlConnections must be >= maxScanSqlConnections ") +
                    " and both must be greater than 1." +
                    " maxSqlConnections=" + std::to_string(maxSqlConnections) +
                    " maxScanSqlConnections=" + std::to_string(maxScanSqlConnections));
        }
        // TODO Change configuration files to use normal values.
        _maxSqlScanConnections = maxScanSqlConnections;
        _maxSqlSharedConnections = maxSqlConnections - maxScanSqlConnections;
        if (_maxSqlSharedConnections <= _maxSqlScanConnections) {
            throw std::invalid_argument(
                    std::string("_maxSqlSharedConnections must be greater than _maxSqlScanConnections") +
                    " maxSqlConnections=" + std::to_string(maxSqlConnections) +
                    " maxScanSqlConnections=" + std::to_string(maxScanSqlConnections));
        }
    }
    SqlConnMgr() = delete;
    SqlConnMgr(SqlConnMgr const&) = delete;
    SqlConnMgr& operator=(SqlConnMgr const&) = delete;
    virtual ~SqlConnMgr() = default;

    int getTotalCount() const { return _totalCount; }
    int getSqlScanConnCount() const { return _sqlScanConnCount; }
    int getSqlSharedConnCount() const { return _sqlSharedConnCount; }

    /// @return a JSON representation of the object's status for the monitoring
    nlohmann::json statusToJson() const;

    virtual std::ostream& dump(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& out, SqlConnMgr const& mgr);

    friend class SqlConnLock;

private:
    ConnType _take(bool scanQuery, std::shared_ptr<wbase::SendChannelShared> const& sendChannelShared,
                   bool firstChannelSqlConn);
    void _release(ConnType connType);

    std::atomic<int> _totalCount{0};
    std::atomic<int> _sqlScanConnCount{0};    ///< Current number of new scan SQL conections
    std::atomic<int> _sqlSharedConnCount{0};  ///< Current number of shared and interactive SQL connections.
    int _maxSqlScanConnections;               ///< max number of connections for new shared scans
    int _maxSqlSharedConnections;  ///< max number of connections for shared connection scans and interactive.
    std::mutex _mtx;
    std::condition_variable _tCv;
};

/// RAII class to support SqlConnMgr
class SqlConnLock {
public:
    SqlConnLock(SqlConnMgr& sqlConnMgr, bool scanQuery,
                std::shared_ptr<wbase::SendChannelShared> const& sendChannelShared);
    SqlConnLock() = delete;
    SqlConnLock(SqlConnLock const&) = delete;
    SqlConnLock& operator=(SqlConnLock const&) = delete;

    ~SqlConnLock() { _sqlConnMgr._release(_connType); }

private:
    SqlConnMgr& _sqlConnMgr;
    SqlConnMgr::ConnType _connType;
};

}  // namespace wcontrol
}  // namespace lsst::qserv

#endif  // LSST_QSERV_WCONTROL_TRANSMITMGR_H
