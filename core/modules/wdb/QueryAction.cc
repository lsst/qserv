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
// #include <fcntl.h>
#include <iostream>
// #include <sys/stat.h>

// // Third-party headers
// #include <boost/regex.hpp>
#include <mysql/mysql.h>

// // Local headers
// #include "global/constants.h"
// #include "mysql/mysql.h"
#include "proto/worker.pb.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "mysql/SchemaFactory.h"
#include "sql/SqlErrorObject.h"
#include "sql/Schema.h"
// #include "sql/SqlFragmenter.h"
#include "wbase/SendChannel.h"
#include "util/StringHash.h"
#include "wconfig/Config.h"
// #include "wdb/QueryPhyResult.h"
// #include "wdb/QuerySql.h"
// #include "wdb/QuerySql_Batch.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace wdb {
class QueryAction::Impl {
public:
    Impl(QueryActionArg const& a);
    ~Impl() {
        if(_task) { // Detach poisoner
            _task->setPoison(boost::shared_ptr<util::VoidCallable<void> >());
        }
        mysql_thread_end();
    }

    bool act();
    void poison();
private:
    class Poisoner;
    inline bool _initConnection() {
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
    inline void _checkDb() {
        if(_msg->has_db()) {
            _dbName = _msg->db();
            _log->warn("QueryAction overriding dbName with " + _dbName);
        }
    }
    inline void _prepareId() {
        // FIXME: This is historical. Is it needed?
#if 0
        _scriptId = t->dbName.substr(0, 6);
        _log->info((Pformat("TIMING,%1%ScriptStart,%2%")
                    % _scriptId % ::time(NULL)).str());
#endif
    }
    inline int selectChunkId() {
        if(_msg->has_chunkid()) {
            return _msg->chunkid();
        }
        return 1234567890;
    }
    bool _dispatchChannel();
    MYSQL_RES* _primeResult(std::string const& query);
    void _addErrorMsg(int errorCode, std::string const& errorMsg);
    bool _fillRows(MYSQL_RES* result, int numFields);
    void _fillSchema(MYSQL_RES* result);
    void _initMsgs();
    void _transmitResult();

    boost::shared_ptr<wlog::WLogger> _log;
    // sql::SqlErrorObject _errObj;
    wbase::Task::Ptr _task;
    std::string _dbName;
    boost::shared_ptr<proto::TaskMsg> _msg;
    bool _poisoned;
    boost::shared_ptr<wbase::SendChannel> _sendChannel;
    std::auto_ptr<mysql::MySqlConnection> _mysqlConn;
    std::string _user;

    typedef std::pair<int, std::string> IntString;
    typedef std::vector<IntString> IntStringVector;
    IntStringVector _errors;

    boost::shared_ptr<proto::ProtoHeader> _protoHeader;
    boost::shared_ptr<proto::Result> _result;
    // boost::shared_ptr<wdb::QueryPhyResult> _pResult;
    // wcontrol::Task::Ptr _task;
    // std::string _scriptId;
    // boost::shared_ptr<boost::mutex> _poisonedMutex;
    // StringDeque _poisoned;
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
    _checkDb();
    bool connOk = _initConnection();
    if(!connOk) { return false; }
    _checkDb();

//    int chunkId = _selectChunkId();
    // TODO: Prepare subchunks as necessary

    if(_msg->has_protocol()) {
        switch(_msg->protocol()) {
        case 1:
            throw std::runtime_error("Expected protocol > 1 in TaskMsg");
        case 2:
            return _dispatchChannel();
        default:
            throw std::runtime_error("Invalid protocol in TaskMsg");
        }
    } else {
        throw std::runtime_error("Expected protocol > 1 in TaskMsg");
    }
    // Release subchunks
}

MYSQL_RES* QueryAction::Impl::_primeResult(std::string const& query) {
        bool queryOk = _mysqlConn->queryUnbuffered(query);
        if(!queryOk) {
            return NULL;
        }
        return _mysqlConn->getResult();
#if 0
            MYSQL_RES* result = mysql_use_result(&cursor);
        MYSQL_RES* result = mysql_use_result(&cursor);
        // call after mysql_store_result
        //uint64_t rowcount = mysql_affected_rows(&cursor);
        if(result) { // rows?
            Schema s = SchemaFactory::newFromResult(result);
            std::cout << "Schema is "
                      << formCreateStatement("hello", s) << "\n";

            std::cout << "will stream results.\n";
        } else  { // mysql_store_result() returned nothing
            if(mysql_field_count(&cursor) > 0) {
                // mysql_store_result() should have returned data
                std::cout <<  "Error getting records: "
                     << mysql_error(&cursor) << std::endl;
            }
        }
#endif
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
    sql::Schema s = mysql::SchemaFactory::newFromResult(result);
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

bool QueryAction::Impl::_dispatchChannel() {
    proto::TaskMsg& m = *_task->msg;
    _initMsgs();
    bool firstResult = true;
    int numFields = -1;
    if(m.fragment_size() < 1) {
        throw std::runtime_error("No fragments to execute in TaskMsg");
    }
    for(int i=0; i < m.fragment_size(); ++i) {
        wbase::Task::Fragment const& f(m.fragment(i));
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
    // Nothing to do here? (doublecheck before merge)
}

bool QueryAction::operator()() {
    return _impl->act();
}

void QueryAction::poison() {
    _impl->poison();
}
}}} // lsst::qserv::wdb
#if 0
namespace {
bool
runBatch(boost::shared_ptr<lsst::qserv::wlog::WLogger> log,
         lsst::qserv::sql::SqlConnection& sqlConn,
         lsst::qserv::sql::SqlErrorObject& errObj,
         std::string const& scriptId,
         lsst::qserv::wdb::QuerySql::Batch& batch,
         lsst::qserv::wbase::CheckFlag* checkAbort) {
    log->info((Pformat("TIMING,%1%%2%Start,%3%")
                 % scriptId % batch.name % ::time(NULL)).str().c_str());
    bool batchAborted = false;
    while(!batch.isDone()) {
        std::string piece = batch.current();
        if(!sqlConn.runQuery(piece.data(), piece.size(), errObj) ) {
            // On error, the partial error is as good as the global.
            if(errObj.isSet() ) {
                log->error((Pformat(">>%1%<<---Error with piece %2% complete (size=%3%).")
                            % errObj.errMsg()
                            % batch.pos % batch.sequence.size()).str().c_str());
                batchAborted = true;
                break;
            } else if(checkAbort && (*checkAbort)()) {
                log->error((Pformat("Aborting query by request (%1% complete).")
                            % batch.pos).str().c_str());
                errObj.addErrorMsg("Query poisoned by client request");
                batchAborted = true;
                break;
            }
        }
        batch.next();
    }
    log->info((Pformat("TIMING,%1%%2%Finish,%3%")
               % scriptId % batch.name % ::time(NULL)).str().c_str());
    if(batchAborted) {
        errObj.addErrMsg("(during " + batch.name
                         + ")\nQueryFragment: " + batch.current());
        log->info((Pformat("Broken! ,%1%%2%---%3%")
                   % scriptId % batch.name % errObj.errMsg()).str().c_str());
        return false;
    }
    return true;
}

// Newer, flexibly-batched system.
bool
runScriptPieces(boost::shared_ptr<lsst::qserv::wlog::WLogger> log,
                lsst::qserv::sql::SqlConnection& sqlConn,
                lsst::qserv::sql::SqlErrorObject& errObj,
                std::string const& scriptId,
                lsst::qserv::wdb::QuerySql const& qSql,
                lsst::qserv::wbase::CheckFlag* checkAbort) {
    lsst::qserv::wdb::QuerySql::Batch build("QueryBuildSub", qSql.buildList);
    lsst::qserv::wdb::QuerySql::Batch exec("QueryExec", qSql.executeList);
    lsst::qserv::wdb::QuerySql::Batch clean("QueryDestroySub", qSql.cleanupList);
    bool sequenceOk = false;
    if(runBatch(log, sqlConn, errObj, scriptId, build, checkAbort)) {
        if(!runBatch(log, sqlConn, errObj, scriptId, exec, checkAbort)) {
            log->error((Pformat("Fail QueryExec phase for %1%: %2%")
                        % scriptId % errObj.errMsg()).str().c_str());
        } else {
            sequenceOk = true;
        }
    }
    // Always destroy subchunks, no aborting (use NULL checkflag)
    return sequenceOk && runBatch(log, sqlConn, errObj, scriptId, clean, NULL);
}

std::string commasToSpaces(std::string const& s) {
    std::string r(s);
    // Convert commas to spaces
    for(int i=0, e=r.size(); i < e; ++i) {
        char& c = r[i];
        if(c == ',')
            c = ' ';
    }
    return r;
}

template <typename F>
void
forEachSubChunk(std::string const& script, F& func) {
    std::string firstLine = script.substr(0, script.find('\n'));
    int subChunkCount = 0;

    boost::regex re("\\d+");
    for (boost::sregex_iterator i = boost::make_regex_iterator(firstLine, re);
         i != boost::sregex_iterator(); ++i) {

        std::string subChunk = (*i).str(0);
        func(subChunk);
        ++subChunkCount;
    }
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace wdb {

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunner
////////////////////////////////////////////////////////////////////////
QueryRunner::QueryRunner(QueryRunnerArg const& a)
    : _log(a.log),
      _user(a.task->user),
      _pResult(new QueryPhyResult()),
      _task(a.task),
      _poisonedMutex(new boost::mutex()) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!a.overrideDump.empty()) {
        _task->resultPath = a.overrideDump;
    }
}

QueryRunner::~QueryRunner() {
    mysql_thread_end();
}

bool
QueryRunner::operator()() {
    // 6/11/2013: Note that query runners are not recycled right now.
    // A Foreman::Runner thread constructs one and executes it once.
    return _act();
}

void
QueryRunner::poison(std::string const& hash) {
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    _poisoned.push_back(hash);
}
////////////////////////////////////////////////////////////////////////
// private:
////////////////////////////////////////////////////////////////////////
bool
QueryRunner::_checkPoisoned() {
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    StringDeque::const_iterator i = find(_poisoned.begin(),
                                         _poisoned.end(), _task->hash);
    return i != _poisoned.end();
}

void
QueryRunner::_setNewQuery(QueryRunnerArg const& a) {
    //_e should be tied to the MySqlFs instance and constant(?)
    _user = a.task->user;
    _task = a.task;
    _errObj.reset();
    if(!a.overrideDump.empty()) {
        _task->resultPath = a.overrideDump;
    }
}

bool
QueryRunner::actOnce() {
    return _act();
}

bool
QueryRunner::_act() {
    char msg[] = "Exec in flight for Db = %1%, dump = %2%";
    _log->info((Pformat(msg) % _task->dbName % _task->resultPath).str());

    // Do not print query-- could be multi-megabytes.
    std::string dbDump = (Pformat("Db = %1%, dump = %2%")
                          % _task->dbName % _task->resultPath).str();
    _log->info((Pformat("(fileobj:%1%) %2%")
            % (void*)(this) % dbDump).str());

    // Result files shouldn't get reused right now
    // since we trash them after they are read once
#if 0
    if (dumpFileExists(_meta.resultPath)) {
        _log->info((Pformat("Reusing pre-existing dump = %1% (chk=%2%)")
                % _task->resultPath % _task->chunkId).str());
        // The system should probably catch this earlier.
        getTracker().notify(_task->hash, wcontrol::ResultError(0,""));
        return true;
    }
#endif
    if (!_runTask(_task)) {
        _log->info((Pformat("(FinishFail:%1%) %2% hash=%3%")
                % (void*)(this) % dbDump % _task->hash).str());
        getTracker().notify(_task->hash,
                            wcontrol::ResultError(-1,"Script exec failure "
                                                  + _getErrorString()));
        return false;
    }
    _log->info((Pformat("(FinishOK:%1%) %2%")
            % (void*)(this) % dbDump).str());
    getTracker().notify(_task->hash, wcontrol::ResultError(0,""));
    return true;
}

bool
QueryRunner::_poisonCleanup() {
    StringDeque::iterator i;
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    i = find(_poisoned.begin(), _poisoned.end(), _task->hash);
    if(i == _poisoned.end()) {
        return false;
    }
    _poisoned.erase(i);
    return true;
}

std::string
QueryRunner::_getErrorString() const {
    return (Pformat("%1%: %2%") % _errObj.errNo() % _errObj.errMsg()).str();
}

// Record query in query cache table
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
bool
QueryRunner::_runTask(wcontrol::Task::Ptr t) {
    mysql::MySqlConfig sc(wconfig::getConfig().getSqlConfig());
    sc.username = _user.c_str(); // Override with czar-passed username.
    sql::SqlConnection _sqlConn(sc, true);
    bool success = true;
    _scriptId = t->dbName.substr(0, 6);
    _log->info((Pformat("TIMING,%1%ScriptStart,%2%")
                 % _scriptId % ::time(NULL)).str());
    // For now, coalesce all fragments.
    std::string resultTable;
    _pResult->reset();
    assert(t.get());
    assert(t->msg.get());
    proto::TaskMsg& m(*t->msg);
    if(!_sqlConn.connectToDb(_errObj)) {
        _log->info((Pformat("Cfg error! connect MySQL as %1% using %2%")
                    % wconfig::getConfig().getString("mysqlSocket") % _user).str());
        return _errObj.addErrMsg("Unable to connect to MySQL as " + _user);
    }
    int chunkId = 1234567890;
    if(m.has_chunkid()) { chunkId = m.chunkid(); }
    std::string defaultDb = "test";
    if(m.has_db()) { defaultDb = m.db(); }
    for(int i=0; i < m.fragment_size(); ++i) {
        wcontrol::Task::Fragment const& f(m.fragment(i));

        if(f.has_resulttable()) { resultTable = f.resulttable(); }
        assert(!resultTable.empty());

        // Use SqlFragmenter to break up query portion into fragments.
        // If protocol gives us a query sequence, we won't need to
        // split fragments.
        bool first = t->needsCreate && (i==0);
        boost::shared_ptr<wdb::QuerySql> qSql(new QuerySql(defaultDb, chunkId,
                                                           f,
                                                           first,
                                                           resultTable));

        success = _runFragment(_sqlConn, *qSql);
        if(!success) return false;
        _pResult->addResultTable(resultTable);
    }
    _log->info("about to dump table " + resultTable);
    if(success) {
        if(_task->sendChannel) {
            if(!_pResult->dumpToChannel(*_log, _user,
                                        _task->sendChannel, _errObj)) {
                return false;
            }
        } else if(!_pResult->performMysqldump(*_log,
                                              _user,
                                              _task->resultPath,
                                              _errObj)) {
            return false;
        }
    }
    if (!_sqlConn.dropTable(_pResult->getCommaResultTables(), _errObj, false)) {
        return false;
    }
    return true;
}

bool
QueryRunner::_runFragment(sql::SqlConnection& sqlConn,
                          wdb::QuerySql const& qSql) {
    boost::shared_ptr<wbase::CheckFlag> check(_makeAbort());

    if(!_prepareAndSelectResultDb(sqlConn)) {
        return false;
    }
    if(_checkPoisoned()) { // Check for poison
        _poisonCleanup(); // Clean it up.
        return false;
    }
    if( !runScriptPieces(_log, sqlConn, _errObj, _scriptId,
                         qSql, check.get()) ) {
        return false;
    }
    _log->info((Pformat("TIMING,%1%ScriptFinish,%2%")
                % _scriptId % ::time(NULL)).str().c_str());
    return true;
}

bool
QueryRunner::_prepareAndSelectResultDb(sql::SqlConnection& sqlConn,
                                       std::string const& resultDb) {
    std::string result;
    std::string dbName(resultDb);

    if(dbName.empty()) {
        dbName = wconfig::getConfig().getString("scratchDb");
    } else if(sqlConn.dropDb(dbName, _errObj, false)) {
        _log->info((Pformat("Cfg error! couldn't drop resultdb. %1%.")
                % result).str().c_str());
        return false;
    }
    if(!sqlConn.createDb(dbName, _errObj, false)) {
        _log->info((Pformat("Cfg error! couldn't create resultdb. %1%.")
                % result).str().c_str());
        return false;
    }
    if (!sqlConn.selectDb(dbName, _errObj)) {
        _log->info((Pformat("Cfg error! couldn't select resultdb. %1%.")
                % result).str().c_str());
        return _errObj.addErrMsg("Unable to select database " + dbName);
    }
    _pResult->setDb(dbName);
    return true;
}

bool
QueryRunner::_prepareScratchDb(sql::SqlConnection& sqlConn) {
    std::string dbName = wconfig::getConfig().getString("scratchDb");

    if ( !sqlConn.createDb(dbName, _errObj, false) ) {
        _log->info((Pformat("Cfg error! couldn't create scratch db. %1%.")
                % _errObj.errMsg()).str().c_str());
        return false;
    }
    if ( !sqlConn.selectDb(dbName, _errObj) ) {
        _log->info((Pformat("Cfg error! couldn't select scratch db. %1%.")
                % _errObj.errMsg()).str().c_str());
        return _errObj.addErrMsg("Unable to select database " + dbName);
    }
    return true;
}

std::string
QueryRunner::_getDumpTableList(std::string const& script) {
    // Find resultTable prefix
    char const prefix[] = "-- RESULTTABLES:";
    int prefixLen = sizeof(prefix);
    std::string::size_type prefixOffset = script.find(prefix);
    if(prefixOffset == std::string::npos) { // no table indicator?
        return std::string();
    }
    prefixOffset += prefixLen - 1; // prefixLen includes null-termination.
    std::string tables = script.substr(prefixOffset,
                                       script.find('\n', prefixOffset)
                                       - prefixOffset);
    return tables;
}

boost::shared_ptr<ArgFunc>
QueryRunner::getResetFunc() {
    class ResetFunc : public ArgFunc {
    public:
        ResetFunc(QueryRunner* r) : runner(r) {}
        void operator()(QueryRunnerArg const& a) {
            runner->_setNewQuery(a);
        }
        QueryRunner* runner;
    };
    ArgFunc* af = new ResetFunc(this);
    return boost::shared_ptr<ArgFunc>(af);
}

boost::shared_ptr<wbase::CheckFlag>
QueryRunner::_makeAbort() {
    class Check : public wbase::CheckFlag {
    public:
        Check(QueryRunner& qr) : runner(qr) {}
        virtual ~Check() {}
        bool operator()() { return runner._checkPoisoned(); }
        QueryRunner& runner;
    };
    wbase::CheckFlag* cf = new Check(*this);
    return boost::shared_ptr<wbase::CheckFlag>(cf);
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
int
dumpFileOpen(std::string const& dbName) {
    return ::open(dbName.c_str(), O_RDONLY);
}

bool
dumpFileExists(std::string const& dumpFilename) {
    struct stat statbuf;
    return ::stat(dumpFilename.c_str(), &statbuf) == 0 &&
        S_ISREG(statbuf.st_mode) && (statbuf.st_mode & S_IRUSR) == S_IRUSR;
}

}}} // namespace lsst::qserv::wdb
#endif
