// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
 /**
  * @file
  *
  * @brief QueryRunner instances perform single-shot query execution with the
  * result reflected in the db state or returned via a SendChannel. Works with
  * new XrdSsi API.
  *
  * @author Daniel L. Wang, SLAC; John Gates, SLAC
  */


// System headers
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>

// Third-party headers
#include <mysql/mysql.h>

// Class header
#include "wdb/QueryRunner.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
#include "global/debugUtil.h"
#include "global/UnsupportedError.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "mysql/SchemaFactory.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "sql/Schema.h"
#include "sql/SqlErrorObject.h"
#include "util/common.h"
#include "util/MultiError.h"
#include "util/StringHash.h"
#include "util/threadSafe.h"
#include "wbase/Base.h"
#include "wbase/SendChannel.h"
#include "wdb/ChunkResource.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wdb.QueryRunner");
}

namespace lsst {
namespace qserv {
namespace wdb {

QueryRunner::Ptr QueryRunner::newQueryRunner(wbase::Task::Ptr const& task,
                                             ChunkResourceMgr::Ptr const& chunkResourceMgr,
                                             mysql::MySqlConfig const& mySqlConfig) {
    Ptr qr{new QueryRunner{task, chunkResourceMgr, mySqlConfig}}; // Private constructor.
    // Let the Task know this is its QueryRunner.
    bool cancelled = qr->_task->setTaskQueryRunner(qr);
    if (cancelled) {
        qr->_cancelled.store(true);
        // runQuery will return quickly if the Task has been cancelled.
    }
    return qr;
}

/// New instances need to be made with QueryRunner to ensure registration with the task
/// and correct setup of enable_shared_from_this.
QueryRunner::QueryRunner(wbase::Task::Ptr const& task,
                         ChunkResourceMgr::Ptr const& chunkResourceMgr,
                         mysql::MySqlConfig const& mySqlConfig)
    : _task(task), _chunkResourceMgr(chunkResourceMgr), _mySqlConfig(mySqlConfig) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    assert(_task->msg);
}

/// Initialize the db connection
bool QueryRunner::_initConnection() {
    mysql::MySqlConfig localMySqlConfig(_mySqlConfig);
    localMySqlConfig.username = _task->user; // Override with czar-passed username.
    _mysqlConn.reset(new mysql::MySqlConnection(localMySqlConfig));

    if (not _mysqlConn->connect()) {
        LOGS(_log, LOG_LVL_ERROR, "Unable to connect to MySQL: " << localMySqlConfig);
        util::Error error(-1, "Unable to connect to MySQL; " + localMySqlConfig.toString());
        _multiError.push_back(error);
        return false;
    }
    return true;
}

/// Override _dbName with _msg->db() if available.
void QueryRunner::_setDb() {
    if (_task->msg->has_db()) {
        _dbName = _task->msg->db();
        LOGS(_log, LOG_LVL_WARN, "QueryRunner overriding dbName with " << _dbName);
    }
}

bool QueryRunner::runQuery() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryRunner::runQuery() " << _task->getIdStr());
    // Make certain our Task knows that this object is no longer in use when this function exits.
    class Release {
    public:
        Release(wbase::Task::Ptr t, wbase::TaskQueryRunner *tqr) : _t{t}, _tqr{tqr} {}
        ~Release() { _t->freeTaskQueryRunner(_tqr); }
    private:
        wbase::Task::Ptr _t;
        wbase::TaskQueryRunner *_tqr;
    };
    Release release(_task, this);

    if (_task->getCancelled()) {
        LOGS(_log, LOG_LVL_DEBUG, "runQuery, task was cancelled before it started. taskHash=" << _task->hash);
        return false;
    }

    _setDb();
    LOGS(_log, LOG_LVL_DEBUG, "Exec in flight for Db=" << _dbName);
    bool connOk = _initConnection();
    if (!connOk) { return false; }

    if (_task->msg->has_protocol()) {
        switch(_task->msg->protocol()) {
        case 2:
            return _dispatchChannel(); // Run the query and send the results back.
        case 1:
            throw UnsupportedError("QueryRunner: Expected protocol > 1 in TaskMsg");
        default:
            throw UnsupportedError("QueryRunner: Invalid protocol in TaskMsg");
        }
    } else {
        throw UnsupportedError("QueryRunner: Expected protocol > 1 in TaskMsg");
    }
    LOGS(_log, LOG_LVL_DEBUG, "QueryRunner::runQuery() END " << _task->getIdStr());
    return false;
}

MYSQL_RES* QueryRunner::_primeResult(std::string const& query) {
        bool queryOk = _mysqlConn->queryUnbuffered(query);
        if (!queryOk) {
            util::Error error(_mysqlConn->getErrno(), _mysqlConn->getError());
            _multiError.push_back(error);
            return nullptr;
        }
        return _mysqlConn->getResult();
}

void QueryRunner::_initMsgs() {
    _protoHeader = std::make_shared<proto::ProtoHeader>();
    _initMsg();
}

void QueryRunner::_initMsg() {
    _result = std::make_shared<proto::Result>();
    _result->mutable_rowschema();
    _result->set_continues(0);
    if (_task->msg->has_session()) {
        _result->set_session(_task->msg->session());
    }
}

void QueryRunner::_fillSchema(MYSQL_RES* result) {
    // Build schema obj from result
    auto s = mysql::SchemaFactory::newFromResult(result);
    // Fill _result's schema from Schema obj
    for(auto i=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        proto::ColumnSchema* cs = _result->mutable_rowschema()->add_columnschema();
        cs->set_name(i->name);
        if (i->hasDefault) {
            cs->set_hasdefault(true);
            cs->set_defaultvalue(i->defaultValue);
            LOGS(_log, LOG_LVL_DEBUG, i->name << " has default.");
        } else {
            cs->set_hasdefault(false);
            cs->clear_defaultvalue();
        }
        cs->set_sqltype(i->colType.sqlType);
        cs->set_mysqltype(i->colType.mysqlType);
    }
}

/// Fill one row in the Result msg from one row in MYSQL_RES*
/// If the message has gotten larger than the desired message size,
/// it will be transmitted with a flag set indicating the result
/// continues in later messages.
bool QueryRunner::_fillRows(MYSQL_RES* result, int numFields) {
    MYSQL_ROW row;
    size_t size = 0;
    while ((row = mysql_fetch_row(result))) {
        auto lengths = mysql_fetch_lengths(result);
        proto::RowBundle* rawRow =_result->add_row();
        for(int i=0; i < numFields; ++i) {
            if (row[i]) {
                rawRow->add_column(row[i], lengths[i]);
                rawRow->add_isnull(false);
            } else {
                rawRow->add_column();
                rawRow->add_isnull(true);
            }
        }
        size += rawRow->ByteSize();

        // Each element needs to be mysql-sanitized
        if (size > proto::ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT) {
            if (size > proto::ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
                LOGS_ERROR("Message single row too large to send using protobuffer");
                return false;
            }
            LOGS(_log, LOG_LVL_DEBUG, "Large message size=" << size << ", splitting message");
            _transmit(false);
            size = 0;
            _initMsg();
        }
    }
    return true;
}

/// Transmit result data with its header.
/// If 'last' is true, this is the last message in the result set
/// and flags are set accordingly.
void QueryRunner::_transmit(bool last) {
    LOGS(_log, LOG_LVL_DEBUG, "_transmit last=" << last << " " << _task->getIdStr());
    std::string resultString;
    _result->set_continues(!last);
    if (!_multiError.empty()) {
        std::string chunkId = std::to_string(_task->msg->chunkid());
        std::string msg = "Error(s) in result for chunk #" + chunkId + ": " + _multiError.toOneLineString();
        _result->set_errormsg(msg);
        LOGS(_log, LOG_LVL_ERROR, msg);
    }
    _result->SerializeToString(&resultString);
    _transmitHeader(resultString);
    LOGS(_log, LOG_LVL_DEBUG, "_transmit last=" << last << " " << _task->getIdStr()
         << " resultString=" << util::prettyCharList(resultString, 5));
    if (!_cancelled) {
        _task->sendChannel->sendStream(resultString.data(), resultString.size(), last);
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "_transmit cancelled");
    }
}

/// Transmit the protoHeader
void QueryRunner::_transmitHeader(std::string& msg) {
    LOGS(_log, LOG_LVL_DEBUG, "_transmitHeader");
    // Set header
    _protoHeader->set_protocol(2); // protocol 2: row-by-row message
    _protoHeader->set_size(msg.size());
    _protoHeader->set_md5(util::StringHash::getMd5(msg.data(), msg.size()));
    _protoHeader->set_wname(getHostname());
    std::string protoHeaderString;
    _protoHeader->SerializeToString(&protoHeaderString);

    // Flush to channel.
    // Make sure protoheader size can be encoded in a byte.
    assert(protoHeaderString.size() < 255);
    auto msgBuf = proto::ProtoHeaderWrap::wrap(protoHeaderString);
    if (!_cancelled) {
        _task->sendChannel->sendStream(msgBuf.data(), msgBuf.size(), false);
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "_transmitHeader cancelled");
    }
}

class ChunkResourceRequest {
public:
    ChunkResourceRequest(std::shared_ptr<ChunkResourceMgr> const& mgr,
                         proto::TaskMsg const& msg)
        // Use old-school member initializers because gcc 4.8.5
        // miscompiles the code when using brace initializers (DM-4704).
        : _mgr(mgr), _msg(msg) {}

    ChunkResource getResourceFragment(int i) {
        proto::TaskMsg_Fragment const& fragment(_msg.fragment(i));
        if (!fragment.has_subchunks()) {
            StringVector tables;
            for (auto const& scanTbl : _msg.scantable()) {
                tables.push_back(scanTbl.db() + "." + scanTbl.table());
            }
            assert(_msg.has_db());
            return _mgr->acquire(_msg.db(), _msg.chunkid(), tables);
        }

        std::string db;
        proto::TaskMsg_Subchunk const& sc = fragment.subchunks();
        StringVector tables(sc.table().begin(),
                            sc.table().end());
        IntVector subchunks(sc.id().begin(), sc.id().end());
        if (sc.has_database()) { db = sc.database(); }
        else { db = _msg.db(); }
        return _mgr->acquire(db, _msg.chunkid(), tables, subchunks);

    }
private:
    std::shared_ptr<ChunkResourceMgr> _mgr;
    proto::TaskMsg const& _msg;
};

bool QueryRunner::_dispatchChannel() {
    proto::TaskMsg& m = *_task->msg;
    _initMsgs();
    bool firstResult = true;
    bool erred = false;
    int numFields = -1;
    if (m.fragment_size() < 1) {
        throw Bug("QueryRunner: No fragments to execute in TaskMsg");
    }
    ChunkResourceRequest req(_chunkResourceMgr, m);

    try {
        for(int i=0; i < m.fragment_size(); ++i) {
            if (_cancelled) {
                break;
            }
            proto::TaskMsg_Fragment const& fragment(m.fragment(i));
            ChunkResource cr(req.getResourceFragment(i));
            // Use query fragment as-is, funnel results.
            for(int qi=0, qe=fragment.query_size(); qi != qe; ++qi) {
                MYSQL_RES* res = _primeResult(fragment.query(qi));
                if (!res) {
                    erred = true;
                    continue;
                }
                if (firstResult) {
                    _fillSchema(res);
                    firstResult = false;
                    numFields = mysql_num_fields(res);
                } // TODO: may want to confirm (cheaply) that
                // successive queries have the same result schema.
                // TODO fritzm: revisit this error strategy
                // (see pull-request for DM-216)
                // Now get rows...
                if (!_fillRows(res, numFields)) {
                    erred = true;
                }
                _mysqlConn->freeResult();
            } // Each query in a fragment
        } // Each fragment in a msg.
    } catch(sql::SqlErrorObject const& e) {
        util::Error worker_err(e.errNo(), e.errMsg());
        _multiError.push_back(worker_err);
    }
    if (!_cancelled) {
        // Send results.
        _transmit(true);
    } else {
        erred = true;
        // Send poison error.
        _multiError.push_back(util::Error(-1, "Poisoned."));
        // Do we need to do any cleanup?
    }
    return !erred;
}

void QueryRunner::cancel() {
    LOGS(_log, LOG_LVL_WARN, "Trying QueryRunner::cancel() call, experimental");
    _cancelled.store(true);
    if (!_mysqlConn.get()) {
        LOGS(_log, LOG_LVL_WARN, "QueryRunner::cancel() no MysqlConn");
        return;
    }
    int status = _mysqlConn->cancel();
    switch (status) {
      case -1:
          LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() NOP");
          break;
      case 0:
          LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() success");
          break;
      case 1:
          LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() Error connecting to kill query.");
          break;
      case 2:
          LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() Error processing kill query.");
          break;
      default:
          LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() unknown error");
          break;
    }
}

QueryRunner::~QueryRunner() {
}

}}} // namespace lsst::qserv::wdb

// Future idea: Query cache
// Pseudocode: Record query in query cache table
/*
  result = runQuery(db.get(),
  "INSERT INTO qcache.Queries "
  "(queryTime, query, db, path) "
  "VALUES (NOW(), ?, "
  "'" + dbName + "'"
  ", "
  "'" + _task->resultPath + "'"
  ")",
  script);
  if (result.size() != 0) {
  _errorNo = EIO;
  _errorDesc += result;
  return false;
  }
*/
