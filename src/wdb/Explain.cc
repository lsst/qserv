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

// Class header
#include "wdb/Explain.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "sql/SqlErrorObject.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/SqlConnMgr.h"
#include "wdb/ChunkResource.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wdb.Explain");

}  // namespace

namespace lsst::qserv::wdb {

MYSQL_RES* Explain::_primeResult(string const& query) {
    bool queryOk = _mysqlConn->queryUnbuffered(query);
    if (!queryOk) {
        sql::SqlErrorObject errObj;
        errObj.setErrNo(_mysqlConn->getErrno());
        errObj.addErrMsg("primeResult error " + _mysqlConn->getError());
        throw errObj;
    }
    return _mysqlConn->getResult();
}

bool Explain::runQuery(std::string& errMsg) {
    // Queries that span multiple tasks should not be high priority for the SqlConMgr as it risks deadlock.
    bool scanQuery = false;  // explain is not a scan query.
    wcontrol::SqlConnLock sqlConnLock(*_sqlConnMgr, scanQuery, nullptr);

    bool connOk = _initConnection();
    if (!connOk) {
        errMsg += "initConnection failed";
        util::Error err(util::Error::WORKER_SQL_CONNECT, 0, errMsg);
        _resultElementMsg->multiError.insert(err);
        return false;  // failure
    }

    // Run the query and send the results back.
    return _dispatchChannel(errMsg);
}

bool Explain::_initConnection() {
    mysql::MySqlConfig localMySqlConfig(_mySqlConfig);
    localMySqlConfig.username = _userName;  // Override with czar-passed username.
    if (_mysqlConn != nullptr) {
        LOGS(_log, LOG_LVL_ERROR,
             cName(__func__) << " _mysqlConn not nullptr _mysqlConn=" << _mysqlConn.get());
    }
    _mysqlConn.reset(new mysql::MySqlConnection(localMySqlConfig));

    if (not _mysqlConn->connect()) {
        LOGS(_log, LOG_LVL_ERROR, "Unable to connect to MySQL: " << localMySqlConfig);
        util::Error error(util::Error::WORKER_SQL_CONNECT, util::Error::NONE,
                          "Unable to connect to MySQL; " + localMySqlConfig.toString());
        _resultElementMsg->multiError.insert(error);
        return false;
    }
    return true;
}

class ChunkResourceRequestExplain {
public:
    using Ptr = std::shared_ptr<ChunkResourceRequestExplain>;

    ChunkResourceRequestExplain(shared_ptr<ChunkResourceMgr> const& mgr, string const& db, int chunkId,
                                int subchunkId, DbTableSet const& subchunkTables)
            : _mgr(mgr),
              _db(db),
              _chunkId(chunkId),
              _subchunkId(subchunkId),
              _subchunkTables(subchunkTables) {}

    // Since each Task has only one subchunk, fragment number isn't needed.
    ChunkResource getResourceFragment() {
        // Negative indicates no subchunks.
        if (_subchunkId < 0) {
            /// Why acquire anything if there are no subchunks in the fragment?
            /// Future: Need to be certain this never happens before removing.
            return _mgr->acquire(_db, _chunkId, _subchunkTables);
        }
        IntVector subchunkIdVect;
        subchunkIdVect.push_back(_subchunkId);
        return _mgr->acquire(_db, _chunkId, _subchunkTables, subchunkIdVect);
    }

private:
    shared_ptr<ChunkResourceMgr> const _mgr;
    std::string const _db;
    int const _chunkId;
    int const _subchunkId;
    DbTableSet _subchunkTables;
};

bool Explain::_dispatchChannel(string& errMsg) {
    bool erred = false;
    bool needToFreeRes = false;  // set to true once there are results to be freed.
    // Collect the result in _transmitData. When a reasonable amount of data has been collected,
    // or there are no more rows to collect, pass _transmitData to _sendChannel.
    ChunkResourceRequestExplain::Ptr req;
    ChunkResource::Ptr cr;
    try {
        /// This is fine for a user requested explain, but for an automated czar request, it's expensive.
        req.reset(new ChunkResourceRequestExplain(_chunkResourceMgr, _db, _chunkId, _subchunkId,
                                                  _subchunkTables));
        cr.reset(new ChunkResource(req->getResourceFragment()));
        string const& query = "ANALYZE FORMAT=JSON " + _queryString;
        MYSQL_RES* res = _primeResult(query);  // This runs the SQL query, throws SqlErrorObj on failure.
        needToFreeRes = true;

        _resultElementMsg->explainResult = getJsonRes(query, res);
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " jsRes: " << _resultElementMsg->explainResult.dump());
    } catch (sql::SqlErrorObject const& e) {
        errMsg = e.errMsg() + " " + errMsg;
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " dispatchChannel " << errMsg);
        util::Error worker_err(util::Error::WORKER_SQL, e.errNo(), errMsg);
        _resultElementMsg->multiError.insert(worker_err);
        erred = true;
    }

    // IMPORTANT, do not leave this function before this check has been made.
    if (needToFreeRes) {
        needToFreeRes = false;
        // All rows have been read out or there was an error. In
        // either case resources need to be freed.
        _mysqlConn->freeResult();
    }
    return !erred;
}

std::string Explain::printRes(string const& query, MYSQL_RES* res) const {
    // This get nicer looking output, but having a json result is desirable for the czar to eventually parse
    // and use.
    if (res == nullptr) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " res is nullptr");
        return cName(__func__) + " res is nullptr";
    }
    MYSQL_ROW row;
    stringstream os;
    os << cName(__func__) << " query=" << query << " Explain result: ";
    unsigned int num_fields = mysql_num_fields(res);
    MYSQL_FIELD* fields = mysql_fetch_fields(res);
    while ((row = mysql_fetch_row(res))) {
        for (unsigned int i = 0; i < num_fields; i++) {
            string fieldName = fields[i].name;
            string fieldValue = (row[i] ? row[i] : "NULL");
            os << "{ " << fieldName << "=" << fieldValue << " }";
        }
    }
    return os.str();
}

nlohmann::json Explain::getJsonRes(std::string const& query, MYSQL_RES* res) const {
    // This gets json output, but the result is loaded with '\"' and '\n', which is hard to read.
    nlohmann::json jRes;
    if (res == nullptr) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " res is nullptr");
        jRes["error"] = "res is nullptr";
        return jRes;
    }
    MYSQL_ROW row;
    unsigned int num_fields = mysql_num_fields(res);
    MYSQL_FIELD* fields = mysql_fetch_fields(res);
    while ((row = mysql_fetch_row(res))) {
        nlohmann::json jRow;
        for (unsigned int i = 0; i < num_fields; i++) {
            string fieldName = fields[i].name;
            string fieldValue = (row[i] ? row[i] : "NULL");
            jRow[fieldName] = fieldValue;
        }
        jRes.push_back(jRow);
    }
    return jRes;
}

}  // namespace lsst::qserv::wdb
