// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  * @brief QueryAction instances perform single-shot query execution with the
  * result reflected in the db state or returned via a SendChannel. Works with
  * new XrdSsi API.
  *
  * @author Daniel L. Wang, SLAC; John Gates, SLAC
  */


// System headers
#include <algorithm>
#include <iostream>
#include <memory>

// Third-party headers
#include <mysql/mysql.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/Bug.h"
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
#include "wconfig/Config.h"
#include "wdb/ChunkResource.h"
#include "wdb/QueryAction.h"

namespace lsst {
namespace qserv {
namespace wdb {
/// QueryAction Implementation class
class QueryAction::Impl {
public:
    Impl(QueryActionArg const& a);
    ~Impl() {
        if(_task) { // Detach poisoner
            _task->setPoison(std::shared_ptr<util::VoidCallable<void> >());
        }
    }

    bool act(); ///< Perform the task
    void poison(); ///< Stop the task if it is already running, or prevent it
                   ///< from starting.
    class Poisoner;

private:

    /// Initialize the db connection
    bool _initConnection() {
        mysql::MySqlConfig sc(wconfig::getConfig().getSqlConfig());
        sc.username = _user.c_str(); // Override with czar-passed username.
        _mysqlConn.reset(new mysql::MySqlConnection(sc));

        if(!_mysqlConn->connect()) {
            LOGF(_log, LOG_LVL_ERROR, "Cfg error! connect MySQL as %1% using %2%"
                 % wconfig::getConfig().getString("mysqlSocket") % _user);
            util::Error error(-1, "Unable to connect to MySQL as " + _user);
            _multiError.push_back(error);
            return false;
        }
        return true;
    }

    /// Override _dbName with _msg->db() if available.
    void _setDb() {
        if(_msg->has_db()) {
            _dbName = _msg->db();
            LOGF(_log, LOG_LVL_WARN, "QueryAction overriding dbName with %1%" % _dbName);
        }
    }

    /// Dispatch with output sent through a SendChannel
    bool _dispatchChannel();
    /// Obtain a result handle for a query.
    MYSQL_RES* _primeResult(std::string const& query);

    bool _fillRows(MYSQL_RES* result, int numFields);
    void _fillSchema(MYSQL_RES* result);
    void _initMsgs();
    void _initMsg();
    void _transmit(bool last);
    void _transmitHeader(std::string& msg);

    LOG_LOGGER _log;
    wbase::Task::Ptr _task;
    std::shared_ptr<ChunkResourceMgr> _chunkResourceMgr;
    std::string _dbName;
    std::shared_ptr<proto::TaskMsg> _msg;
    util::Flag<bool> _poisoned;
    std::shared_ptr<wbase::SendChannel> _sendChannel;
    std::unique_ptr<mysql::MySqlConnection> _mysqlConn;
    std::string _user;

    util::MultiError _multiError; // Error log

    std::shared_ptr<proto::ProtoHeader> _protoHeader;
    // _resultCurrent points to _resultA's buffer or _resultB's buffer as needed.
    std::shared_ptr<proto::Result> _result;
};

class QueryAction::Impl::Poisoner : public util::VoidCallable<void> {
public:
    Poisoner(std::shared_ptr<Impl> i) : _i(i) {}
    void operator()() {
        std::shared_ptr<Impl> iSharedPtr(_i.lock());
        if (iSharedPtr) {
            iSharedPtr->poison();
        }
    }
    // Poisoners are potentially long-lived, so weak_ptr is better,
    // otherwise we would unnecessarily hold resources corresponding
    // to work that has been completed (and we wouldn't be able to do
    // poisoning for these resources anyway).
    std::weak_ptr<Impl> _i;
};

////////////////////////////////////////////////////////////////////////
// QueryAction::Impl implementation
////////////////////////////////////////////////////////////////////////
QueryAction::Impl::Impl(QueryActionArg const& a)
    : _log(a.log),
      _task(a.task),
      _chunkResourceMgr(a.mgr),
      _dbName(a.task->dbName),
      _msg(a.task->msg),
      _poisoned(false),
      _sendChannel(a.task->sendChannel),
      _user(a.task->user) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    assert(_msg);
}

bool QueryAction::Impl::act() {
    char msg[] = "Exec in flight for Db = %1%";
    LOGF(_log, LOG_LVL_INFO, msg % _task->dbName);
    _setDb();
    bool connOk = _initConnection();
    if(!connOk) { return false; }

    if(_msg->has_protocol()) {
        switch(_msg->protocol()) {
        case 1:
            throw UnsupportedError("QueryAction: Expected protocol > 1 in TaskMsg");
        case 2:
            return _dispatchChannel();
        default:
            throw UnsupportedError("QueryAction: Invalid protocol in TaskMsg");
        }
    } else {
        throw UnsupportedError("QueryAction: Expected protocol > 1 in TaskMsg");
    }
}

MYSQL_RES* QueryAction::Impl::_primeResult(std::string const& query) {
        bool queryOk = _mysqlConn->queryUnbuffered(query);
        if(!queryOk) {
            util::Error error(_mysqlConn->getErrno(), _mysqlConn->getError());
            _multiError.push_back(error);
            return NULL;
        }
        return _mysqlConn->getResult();
}

void QueryAction::Impl::_initMsgs() {
    _protoHeader = std::make_shared<proto::ProtoHeader>();
    _initMsg();
}

void QueryAction::Impl::_initMsg() {
    _result = std::make_shared<proto::Result>();
    _result->mutable_rowschema();
    _result->set_continues(0);
    if(_msg->has_session()) {
        _result->set_session(_msg->session());
    }
}

void QueryAction::Impl::_fillSchema(MYSQL_RES* result) {
    // Build schema obj from result
    auto s = mysql::SchemaFactory::newFromResult(result);
    // Fill _result's schema from Schema obj
    for(auto i=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        proto::ColumnSchema* cs = _result->mutable_rowschema()->add_columnschema();
        cs->set_name(i->name);
        if(i->hasDefault) {
            cs->set_hasdefault(true);
            cs->set_defaultvalue(i->defaultValue);
            LOGF(_log, LOG_LVL_INFO, "%1% has default." % i->name);
        } else {
            cs->set_hasdefault(false);
            cs->clear_defaultvalue();
        }
        cs->set_sqltype(i->colType.sqlType);
        cs->set_mysqltype(i->colType.mysqlType);
    }
}

/** Fill one row in the Result msg from one row in MYSQL_RES*
 * If the message has gotten larger than the desired message size,
 * it will be transmitted with a flag set indicating the result
 * continues in later messages.
 */
bool QueryAction::Impl::_fillRows(MYSQL_RES* result, int numFields) {
    MYSQL_ROW row;
    size_t size = 0;
    while ((row = mysql_fetch_row(result))) {
        auto lengths = mysql_fetch_lengths(result);
        proto::RowBundle* rawRow =_result->add_row();
        for(int i=0; i < numFields; ++i) {
            if(row[i]) {
                rawRow->add_column(row[i], lengths[i]);
                rawRow->add_isnull(false);
            } else {
                rawRow->add_column();
                rawRow->add_isnull(true);
            }
        }
        size += rawRow->ByteSize();
#if 0 // Enable for tracing result values while debugging.
        std::cout << "row: ";
        std::copy(row, row+numFields,
                  std::ostream_iterator<char*>(std::cout, ","));
        std::cout << "\n";
#endif
        // Each element needs to be mysql-sanitized
        if (size > proto::ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT) {
            if (size > proto::ProtoHeaderWrap::PROTOBUFFER_HARD_LIMIT) {
                LOGF_ERROR("Message single row too large to send using protobuffer");
                return false;
            }
            LOGF_INFO("Large message size=%1%, splitting message" % size);
            _transmit(false);
            size = 0;
            _initMsg();
        }
    }
    return true;
}

/** Transmit result data with its header.
 * If 'last' is true, this is the last message in the result set
 * and flags are set accordingly.
 */
void QueryAction::Impl::_transmit(bool last) {
    LOGF_DEBUG("_transmit last=%1%" % last);
    std::string resultString;
    _result->set_continues(!last);
    if (!_multiError.empty()) {

        std::string chunkId = std::to_string((*_msg).chunkid());
        std::string msg = "Error(s) in result for chunk #" + chunkId + ": " + _multiError.toOneLineString();
        _result->set_errormsg(msg);
        LOGF(_log, LOG_LVL_ERROR, msg);
    }
    _result->SerializeToString(&resultString);
    _transmitHeader(resultString);
    LOGF_INFO("_transmit last=%1% resultString=%2%" % last % util::prettyCharList(resultString, 5));
    _sendChannel->sendStream(resultString.data(), resultString.size(), last);
}

/** Transmit the protoHeader
 */
void QueryAction::Impl::_transmitHeader(std::string& msg) {
    LOGF_DEBUG("_transmitHeader");
    // Set header
    _protoHeader->set_protocol(2); // protocol 2: row-by-row message
    _protoHeader->set_size(msg.size());
    _protoHeader->set_md5(util::StringHash::getMd5(msg.data(), msg.size()));
    std::string protoHeaderString;
    _protoHeader->SerializeToString(&protoHeaderString);

    // Flush to channel.
    // Make sure protoheader size can be encoded in a byte.
    assert(protoHeaderString.size() < 255);
    auto msgBuf = proto::ProtoHeaderWrap::wrap(protoHeaderString);
    _sendChannel->sendStream(msgBuf.data(), msgBuf.size(), false);
}

class ChunkResourceRequest {
public:
    ChunkResourceRequest(std::shared_ptr<ChunkResourceMgr> mgr,
                         proto::TaskMsg const& msg)
        : _mgr(mgr), _msg(msg) {}

    ChunkResource getResourceFragment(int i) {
        wbase::Task::Fragment const& f(_msg.fragment(i));
        if(!f.has_subchunks()) {
            StringVector tables(_msg.scantables().begin(),
                                _msg.scantables().end());
            assert(_msg.has_db());
            return _mgr->acquire(_msg.db(), _msg.chunkid(), tables);
        }

        std::string db;
        proto::TaskMsg_Subchunk const& sc = f.subchunks();
        StringVector tables(sc.table().begin(),
                            sc.table().end());
        IntVector subchunks(sc.id().begin(), sc.id().end());
        if(sc.has_database()) { db = sc.database(); }
        else { db = _msg.db(); }
        return _mgr->acquire(db, _msg.chunkid(), tables, subchunks);

    }
private:
    std::shared_ptr<ChunkResourceMgr> _mgr;
    proto::TaskMsg const& _msg;
};

bool QueryAction::Impl::_dispatchChannel() {
    proto::TaskMsg& m = *_task->msg;
    _initMsgs();
    bool firstResult = true;
    bool erred = false;
    int numFields = -1;
    if(m.fragment_size() < 1) {
        throw Bug("QueryAction: No fragments to execute in TaskMsg");
    }
    ChunkResourceRequest req(_chunkResourceMgr, m);

    try {
        for(int i=0; i < m.fragment_size(); ++i) {
            if (_poisoned.get()) {
                break;
            }
            wbase::Task::Fragment const& f(m.fragment(i));
            ChunkResource cr(req.getResourceFragment(i));
            // Use query fragment as-is, funnel results.
            for(int qi=0, qe=f.query_size(); qi != qe; ++qi) {
                MYSQL_RES* res = _primeResult(f.query(qi));
                if(!res) {
                    erred = true;
                    continue;
                }
                if(firstResult) {
                    _fillSchema(res);
                    firstResult = false;
                    numFields = mysql_num_fields(res);
                } // TODO: may want to confirm (cheaply) that
                // successive queries have the same result schema.
                // TODO fritzm: revisit this error strategy
                // (see pull-request for DM-216)
                // Now get rows...
                if(!_fillRows(res, numFields)) {
                    erred = true;
                }
                _mysqlConn->freeResult();
            } // Each query in a fragment
        } // Each fragment in a msg.
    } catch(sql::SqlErrorObject const& e) {
        util::Error worker_err(e.errNo(), e.errMsg());
        _multiError.push_back(worker_err);
    }
    if(!_poisoned.get()) {
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

void QueryAction::Impl::poison() {
    LOGF(_log, LOG_LVL_WARN, "Trying QueryAction::Impl::poison() call, experimental");
    _poisoned.set(true);
    if(!_mysqlConn.get()) {
    LOGF(_log, LOG_LVL_WARN, "QueryAction::Impl::poison() no MysqlConn");
        return;
    }
    int status = _mysqlConn->cancel();
    switch (status) {
      case -1:
          LOGF(_log, LOG_LVL_ERROR, "poison() NOP");
          break;
      case 0:
          LOGF(_log, LOG_LVL_ERROR, "poison() success");
          break;
      case 1:
          LOGF(_log, LOG_LVL_ERROR, "poison() Error connecting to kill query.");
          break;
      case 2:
          LOGF(_log, LOG_LVL_ERROR, "poison() Error processing kill query.");
          break;
      default:
          LOGF(_log, LOG_LVL_ERROR, "poison() unknown error");
          break;
    }
}

////////////////////////////////////////////////////////////////////////
// QueryAction implementation
////////////////////////////////////////////////////////////////////////
QueryAction::QueryAction(QueryActionArg& a)
    : _impl(new Impl(a)) {
    auto p = std::make_shared<Impl::Poisoner>(_impl);
    // Attach a poisoner that will use us.
    a.task->setPoison(p);
}

QueryAction::~QueryAction() {
}

bool QueryAction::operator()() {
    return _impl->act();
}

void QueryAction::poison() {
    _impl->poison();
}
}}} // lsst::qserv::wdb

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
