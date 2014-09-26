// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
  * @author Daniel L. Wang, SLAC
  */

#include "wdb/QueryAction.h"

// System headers
#include <iostream>

// Third-party headers
#include <mysql/mysql.h>

// Qserv headers
#include "global/Bug.h"
#include "global/UnsupportedError.h"
#include "proto/worker.pb.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "mysql/SchemaFactory.h"
#include "sql/SqlErrorObject.h"
#include "sql/Schema.h"
#include "wbase/SendChannel.h"
#include "util/StringHash.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wdb/ChunkResource.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace wdb {
/// QueryAction Implementation class
class QueryAction::Impl {
public:
    Impl(QueryActionArg const& a);
    ~Impl() {
        if(_task) { // Detach poisoner
            _task->setPoison(boost::shared_ptr<util::VoidCallable<void> >());
        }
        mysql_thread_end();
    }

    bool act(); //< Perform the task
    void poison(); //< Stop the task if it is already running, or prevent it
                   //< from starting.
private:
    class Poisoner;

    /// Initialize the db connection
    bool _initConnection() {
        mysql::MySqlConfig sc(wconfig::getConfig().getSqlConfig());
        sc.username = _user.c_str(); // Override with czar-passed username.
        _mysqlConn.reset(new mysql::MySqlConnection(sc, true));

        if(!_mysqlConn->connect()) {
            _log->info((Pformat("Cfg error! connect MySQL as %1% using %2%")
                        % wconfig::getConfig().getString("mysqlSocket") % _user).str());
            _addErrorMsg(-1, "Unable to connect to MySQL as " + _user);
            return false;
        }
        return true;
    }

    /// Override _dbName with _msg->db() if available.
    void _setDb() {
        if(_msg->has_db()) {
            _dbName = _msg->db();
            _log->warn("QueryAction overriding dbName with " + _dbName);
        }
    }

    /// Dispatch with output sent through a SendChannel
    bool _dispatchChannel();
    /// Obtain a result handle for a query.
    MYSQL_RES* _primeResult(std::string const& query);
    void _addErrorMsg(int errorCode, std::string const& errorMsg);
    bool _fillRows(MYSQL_RES* result, int numFields);
    void _fillSchema(MYSQL_RES* result);
    void _initMsgs();
    void _transmitResult();

    boost::shared_ptr<wlog::WLogger> _log;
    wbase::Task::Ptr _task;
    boost::shared_ptr<ChunkResourceMgr> _chunkResourceMgr;
    std::string _dbName;
    boost::shared_ptr<proto::TaskMsg> _msg;
    bool _poisoned;
    boost::shared_ptr<wbase::SendChannel> _sendChannel;
    std::auto_ptr<mysql::MySqlConnection> _mysqlConn;
    std::string _user;

    typedef std::pair<int, std::string> IntString;
    typedef std::vector<IntString> IntStringVector;
    IntStringVector _errors; //< Error log

    boost::shared_ptr<proto::ProtoHeader> _protoHeader;
    boost::shared_ptr<proto::Result> _result;
};

class QueryAction::Impl::Poisoner : public util::VoidCallable<void> {
public:
    Poisoner(Impl& i) : _i(i) {}
    void operator()() {
        _i.poison();
    }
    Impl& _i;
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
    // Attach a poisoner that will use us.
    _task->setPoison(boost::shared_ptr<Poisoner>(new Poisoner(*this)));
}

bool QueryAction::Impl::act() {
    char msg[] = "Exec in flight for Db = %1%, dump = %2%";
    _log->info((Pformat(msg) % _task->dbName % _task->resultPath).str());
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
            return NULL;
        }
        return _mysqlConn->getResult();
}

void QueryAction::Impl::_addErrorMsg(int code, std::string const& msg) {
    _errors.push_back(IntString(code, msg));
}

void QueryAction::Impl::_initMsgs() {
    _protoHeader.reset(new proto::ProtoHeader);
    _result.reset(new proto::Result);
    if(_msg->has_session()) {
        _result->set_session(_msg->session());
    }
}

void QueryAction::Impl::_fillSchema(MYSQL_RES* result) {
    // Build schema obj from result
    sql::Schema s = mysql::SchemaFactory::newFromResult(result);
    // Fill _result's schema from Schema obj
    sql::ColSchemaVector::const_iterator i, e;
    for(i=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        proto::ColumnSchema* cs = _result->mutable_rowschema()->add_columnschema();
        cs->set_name(i->name);
        if(i->hasDefault) {
            cs->set_hasdefault(true);
            cs->set_defaultvalue(i->defaultValue);
            std::ostringstream os;
            os << i->name << " has default.";
            _log->info(os.str());
        } else {
            cs->set_hasdefault(false);
            cs->clear_defaultvalue();
        }
        cs->set_sqltype(i->colType.sqlType);
        cs->set_mysqltype(i->colType.mysqlType);
    }
}

/// Fill one row in the Result msg from one row in MYSQL_RES*
bool QueryAction::Impl::_fillRows(MYSQL_RES* result, int numFields) {
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        proto::RowBundle* rawRow =_result->add_row();
        for(int i=0; i < numFields; ++i) {
            if(row[i]) {
                rawRow->add_column(row[i]);
                rawRow->add_isnull(false);
            } else {
                rawRow->add_column();
                rawRow->add_isnull(true);
            }
        }
#if 0 // Enable for tracing result values while debugging.
        std::cout << "row: ";
        std::copy(row, row+numFields,
                  std::ostream_iterator<char*>(std::cout, ","));
        std::cout << "\n";
#endif
        // Each element needs to be mysql-sanitized

    }
    return true;
}

/// Send results through SendChannel stream
void QueryAction::Impl::_transmitResult() {
    // FIXME: send errors too!
    // Serialize result first, because we need the size and md5 for the header
    std::string resultString;
    _result->set_nextsize(0);
    _result->SerializeToString(&resultString);

    // Set header
    _protoHeader->set_protocol(2); // protocol 2: row-by-row message
    _protoHeader->set_size(resultString.size());
    _protoHeader->set_md5(util::StringHash::getMd5(resultString.data(),
                                                   resultString.size()));
    std::string protoHeaderString;
    _protoHeader->SerializeToString(&protoHeaderString);

    // Flush to channel.
    // Make sure protoheadeder size can be encoded in a byte.
    assert(protoHeaderString.size() < 255);
    unsigned char phSize = static_cast<unsigned char>(protoHeaderString.size());
    _sendChannel->sendStream(reinterpret_cast<char const*>(&phSize), 1, false);
    _sendChannel->sendStream(protoHeaderString.data(),
                             protoHeaderString.size(), false);
    _sendChannel->sendStream(resultString.data(),
                             resultString.size(), true);
}

class ChunkResourceRequest {
public:
    ChunkResourceRequest(boost::shared_ptr<ChunkResourceMgr> mgr,
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
    boost::shared_ptr<ChunkResourceMgr> _mgr;
    proto::TaskMsg const& _msg;
};

bool QueryAction::Impl::_dispatchChannel() {
    proto::TaskMsg& m = *_task->msg;
    _initMsgs();
    bool firstResult = true;
    int numFields = -1;
    if(m.fragment_size() < 1) {
        throw Bug("QueryAction: No fragments to execute in TaskMsg");
    }
    ChunkResourceRequest req(_chunkResourceMgr, m);

    for(int i=0; i < m.fragment_size(); ++i) {
        wbase::Task::Fragment const& f(m.fragment(i));
        ChunkResource cr(req.getResourceFragment(i));
        // Use query fragment as-is, funnel results.
        for(int qi=0, qe=f.query_size(); qi != qe; ++qi) {
            MYSQL_RES* res = _primeResult(f.query(qi));
            if(!res) {
                // FIXME: report a sensible error that can be handled and returned to the user.
                throw std::runtime_error("Couldn't get result");
                return false;
            }
            if(firstResult) {
                _fillSchema(res);
                firstResult = false;
                numFields = mysql_num_fields(res);
            } // TODO: may want to confirm (cheaply) that
            // successive queries have the same result schema.
            // Now get rows...
            if(!_fillRows(res, numFields)) {
                break;
            }
            _mysqlConn->freeResult();
        } // Each query in a fragment
    } // Each fragment in a msg.
    // Send results.
    _transmitResult();
    return true;
}

void QueryAction::Impl::poison() {
    // TODO: Figure out how to cancel a MySQL C-API call in-flight
    _log->error("Ignoring QueryAction::Impl::poision() call, unimplemented");
}

////////////////////////////////////////////////////////////////////////
// QueryAction implementation
////////////////////////////////////////////////////////////////////////
QueryAction::QueryAction(QueryActionArg const& a)
    : _impl(new Impl(a)) {

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
