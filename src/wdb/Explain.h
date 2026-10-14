// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#ifndef LSST_QSERV_WDB_EXPLAIN_H
#define LSST_QSERV_WDB_EXPLAIN_H

// System headers
#include <memory>
#include <string>

// Third-party headers
#include "nlohmann/json_fwd.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "util/MultiError.h"
#include "wbase/Task.h"
#include "wdb/ChunkResource.h"

namespace lsst::qserv::wcontrol {
class SqlConnMgr;
}  // namespace lsst::qserv::wcontrol

namespace lsst::qserv::wdb {

/** &&& doc
 *
 */
class Explain {
public:
    using Ptr = std::shared_ptr<Explain>;

    static Ptr create(ChunkResourceMgr::Ptr const& chunkResourceMgr,
                      mysql::MySqlConfig const& mySqlConfig, std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                      std::string const& db, int chunkId,
                      std::shared_ptr<wbase::Task::DbTblsAndSubchunks> dbTblsAndSubchunks,
                      std::string const& userName, std::string const& queryString) {
        return Ptr(new Explain(chunkResourceMgr, mySqlConfig, sqlConnMgr, db, chunkId, dbTblsAndSubchunks,
                               userName, queryString));
    }

    Explain(ChunkResourceMgr::Ptr const& chunkResourceMgr,
            mysql::MySqlConfig const& mySqlConfig, std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
            std::string const& db, int chunkId, std::shared_ptr<wbase::Task::DbTblsAndSubchunks> dbTblsAndSubchunks,
            std::string const& userName, std::string const& queryString)
    : _chunkResourceMgr(chunkResourceMgr), _mySqlConfig(mySqlConfig), _sqlConnMgr(sqlConnMgr),
      _db(db), _chunkId(chunkId), _dbTblsAndSubchunks(dbTblsAndSubchunks), _userName(userName),
      _queryString(queryString) {}
    Explain(Explain const&) = delete;
    Explain& operator=(Explain const&) = delete;

    std::string cName(const char* funcName) const {
        return "Explain::" + std::string(funcName) + " db=" + _db + " chunkId=" + std::to_string(_chunkId);
    }

    /** &&& doc
     */
    bool runQuery(std::string& errMsg);

    std::string printRes(std::string const& query, MYSQL_RES* res) const;

private:
    bool _initConnection();
    bool _dispatchChannel(std::string& errStr);
    MYSQL_RES* _primeResult(std::string const& query);

    void _sendError();

    ChunkResourceMgr::Ptr _chunkResourceMgr;

    mysql::MySqlConfig const _mySqlConfig;
    std::shared_ptr<wcontrol::SqlConnMgr> const _sqlConnMgr;
    std::unique_ptr<mysql::MySqlConnection> _mysqlConn;

    std::string const _db;
    int const _chunkId;

    /// Set of tables and vector of subchunk ids used by ChunkResourceRequest. Do not change/reset.
   std::shared_ptr<wbase::Task::DbTblsAndSubchunks> _dbTblsAndSubchunks;

   std::string const _userName;
   std::string const _queryString;

    util::MultiError _multiError;
};

}  // namespace lsst::qserv::wdb

#endif  // LSST_QSERV_WDB_EXPLAIN_H
