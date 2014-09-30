// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2014 LSST Corporation.
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
  * @brief QueryRunner instances perform actual query execution on SQL
  * databases using SqlConnection objects to interact with dbms
  * instances.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "wdb/QueryRunner.h"

// System headers
#include <cassert>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

// Third-party headers
#include <boost/regex.hpp>

// Local headers
#include "global/constants.h"
#include "mysql/mysql.h"
#include "proto/worker.pb.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"
#include "sql/SqlFragmenter.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wdb/QueryPhyResult.h"
#include "wdb/QuerySql.h"
#include "wdb/QuerySql_Batch.h"


namespace {
bool
runBatch(LOG_LOGGER const& log,
         lsst::qserv::sql::SqlConnection& sqlConn,
         lsst::qserv::sql::SqlErrorObject& errObj,
         std::string const& scriptId,
         lsst::qserv::wdb::QuerySql::Batch& batch,
         lsst::qserv::wbase::CheckFlag* checkAbort) {
    LOGF(log, LOG_LVL_INFO, "TIMING,%1%%2%Start,%3%" % scriptId % batch.name
            % ::time(NULL));
    bool batchAborted = false;
    while(!batch.isDone()) {
        std::string piece = batch.current();
        if(!sqlConn.runQuery(piece.data(), piece.size(), errObj) ) {
            // On error, the partial error is as good as the global.
            if(errObj.isSet() ) {
                LOGF(log, LOG_LVL_ERROR,
                        ">>%1%<<---Error with piece %2% complete (size=%3%)."
                        % errObj.errMsg() % batch.pos % batch.sequence.size());
                batchAborted = true;
                break;
            } else if(checkAbort && (*checkAbort)()) {
                LOGF(log, LOG_LVL_ERROR, "Aborting query by request (%1% complete)."
                        % batch.pos);
                errObj.addErrMsg("Query poisoned by client request");
                batchAborted = true;
                break;
            }
        }
        batch.next();
    }
    LOGF(log, LOG_LVL_INFO, "TIMING,%1%%2%Finish,%3%" % scriptId % batch.name
            % ::time(NULL));
    if(batchAborted) {
        errObj.addErrMsg("(during " + batch.name
                         + ")\nQueryFragment: " + batch.current());
        LOGF(log, LOG_LVL_INFO, "Broken! ,%1%%2%---%3%"
                   % scriptId % batch.name % errObj.errMsg());
        return false;
    }
    return true;
}

// Newer, flexibly-batched system.
bool
runScriptPieces(LOG_LOGGER const& log,
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
            LOGF(log, LOG_LVL_ERROR, "Fail QueryExec phase for %1%: %2%" %
                    scriptId % errObj.errMsg());
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

    LOGF(_log, LOG_LVL_INFO, "Exec in flight for Db = %1%, dump = %2%" %
            _task->dbName % _task->resultPath);

    // Do not print query-- could be multi-megabytes.
    std::string dbDump = "Db = " + _task->dbName + ", dump = " + _task->resultPath;
    LOGF(_log, LOG_LVL_INFO, "(fileobj:%1%) %2%" % (void*)(this) % dbDump);

    // Result files shouldn't get reused right now
    // since we trash them after they are read once
#if 0
    if (dumpFileExists(_meta.resultPath)) {
        LOGF(_log, LOG_LVL_INFO, "Reusing pre-existing dump = %1% (chk=%2%)"
                % _task->resultPath % _task->chunkId);
        // The system should probably catch this earlier.
        getTracker().notify(_task->hash, wcontrol::ResultError(0,""));
        return true;
    }
#endif
    if (!_runTask(_task)) {
        LOGF(_log, LOG_LVL_INFO, "(FinishFail:%1%) %2% hash=%3%"
                % (void*)(this) % dbDump % _task->hash);
        getTracker().notify(_task->hash,
                            wcontrol::ResultError(-1,"Script exec failure "
                                                  + _getErrorString()));
        return false;
    }
    LOGF(_log, LOG_LVL_INFO, "(FinishOK:%1%) %2%" % (void*)(this) % dbDump);
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
QueryRunner::_runTask(wbase::Task::Ptr t) {
    mysql::MySqlConfig sc(wconfig::getConfig().getSqlConfig());
    sc.username = _user.c_str(); // Override with czar-passed username.
    sql::SqlConnection _sqlConn(sc, true);
    bool success = true;
    _scriptId = t->dbName.substr(0, 6);
    LOGF(_log, LOG_LVL_INFO, "TIMING,%1%ScriptStart,%2%"
                 % _scriptId % ::time(NULL));
    // For now, coalesce all fragments.
    std::string resultTable;
    _pResult->reset();
    assert(t.get());
    assert(t->msg.get());
    proto::TaskMsg& m(*t->msg);
    if(!_sqlConn.connectToDb(_errObj)) {
        LOGF(_log, LOG_LVL_INFO, "Cfg error! connect MySQL as %1% using %2%"
                    % wconfig::getConfig().getString("mysqlSocket") % _user);
        return _errObj.addErrMsg("Unable to connect to MySQL as " + _user);
    }
    int chunkId = 1234567890;
    if(m.has_chunkid()) { chunkId = m.chunkid(); }
    std::string defaultDb = "test";
    if(m.has_db()) { defaultDb = m.db(); }
    for(int i=0; i < m.fragment_size(); ++i) {
        wbase::Task::Fragment const& f(m.fragment(i));

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
    LOGF(_log, LOG_LVL_INFO, "about to dump table %1%" % resultTable);
    if(success) {
        if(_task->sendChannel) {
            if(!_pResult->dumpToChannel(_log, _user,
                                        _task->sendChannel, _errObj)) {
                return false;
            }
        } else if(!_pResult->performMysqldump(_log,
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
    LOGF(_log, LOG_LVL_INFO, "TIMING,%1%ScriptFinish,%2%"
                % _scriptId % ::time(NULL));
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
        LOGF(_log, LOG_LVL_INFO, "Cfg error! couldn't drop resultdb. %1%." % result);
        return false;
    }
    if(!sqlConn.createDb(dbName, _errObj, false)) {
        LOGF(_log, LOG_LVL_INFO, "Cfg error! couldn't create resultdb. %1%." % result);
        return false;
    }
    if (!sqlConn.selectDb(dbName, _errObj)) {
        LOGF(_log, LOG_LVL_INFO, "Cfg error! couldn't select resultdb. %1%." % result);
        return _errObj.addErrMsg("Unable to select database " + dbName);
    }
    _pResult->setDb(dbName);
    return true;
}

bool
QueryRunner::_prepareScratchDb(sql::SqlConnection& sqlConn) {
    std::string dbName = wconfig::getConfig().getString("scratchDb");

    if ( !sqlConn.createDb(dbName, _errObj, false) ) {
        LOGF(_log, LOG_LVL_INFO, "Cfg error! couldn't create scratch db. %1%."
                % _errObj.errMsg());
        return false;
    }
    if ( !sqlConn.selectDb(dbName, _errObj) ) {
        LOGF(_log, LOG_LVL_INFO, "Cfg error! couldn't select scratch db. %1%."
                % _errObj.errMsg());
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
