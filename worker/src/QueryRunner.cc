/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#include <iostream>
#include <fcntl.h>

#include "mysql/mysql.h"
#include <boost/regex.hpp>
#include "XrdSys/XrdSysError.hh"
#include "lsst/qserv/SqlErrorObject.hh"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/QueryPhyResult.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/worker/SqlFragmenter.h"
using lsst::qserv::SqlErrorObject;
using lsst::qserv::SqlConfig;
using lsst::qserv::SqlConnection;

namespace qWorker = lsst::qserv::worker;

namespace {

bool
runQueryInPieces(boost::shared_ptr<qWorker::Logger> log, 
                 SqlConnection& sqlConn,
                 SqlErrorObject& errObj,
                 std::string const& query,
                 qWorker::CheckFlag* checkAbort) {
    // Run a larger query in pieces split by semicolon/newlines.
    // This tries to avoid the max_allowed_packet
    // (MySQL client/server protocol) problem.
    // MySQL default max_allowed_packet=1MB
    qWorker::SqlFragmenter sf(query);
    while(!sf.isDone()) {
        qWorker::SqlFragmenter::Piece p = sf.getNextPiece();
        if ( !sqlConn.runQuery(p.first, p.second, errObj) ) {
            // On error, the partial error is as good as the global.
            if( errObj.isSet() ) {
                unsigned s=p.second;
                (*log)((Pformat(">>%1%<<---Error with piece %2% complete (size=%3%).") 
                        % errObj.errMsg() % sf.getCount() % s).str().c_str());
                return false;
            } else if(checkAbort && (*checkAbort)()) {
                if(sf.isDone()) {
                    (*log)("Query finished, though client requested abort.");
                } else {
                    (*log)((Pformat("Aborting query by request (%1% complete).") 
                            % sf.getCount()).str().c_str());
                    return errObj.addErrMsg("Query poisoned by client request");
                }
            }
        }
    }
    // Can't use _eDest (we are in file-scope)
    //std::cout << Pformat("Executed query in %1% pieces.") % pieceCount;
    
    // Getting here means that none of the pieces failed.
    return true;
}

bool
runScriptPiece(boost::shared_ptr<qWorker::Logger> log,
               SqlConnection& sqlConn,
               SqlErrorObject& errObj,
               std::string const& scriptId, 
               std::string const& pieceName,
               std::string const& piece, 
               qWorker::CheckFlag* checkAbort) {
    (*log)((Pformat("TIMING,%1%%2%Start,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    //(*log)(("Hi. my piece is++"+piece+"++").c_str());
	
    bool result = runQueryInPieces(log, sqlConn, errObj, piece, checkAbort);
    (*log)((Pformat("TIMING,%1%%2%Finish,%3%")
           % scriptId % pieceName % ::time(NULL)).str().c_str());
    if ( ! result ) {
        errObj.addErrMsg("(during " + pieceName + ")\nQueryFragment: " + piece);
        (*log)((Pformat("Broken! ,%1%%2%---%3%")
               % scriptId % pieceName % errObj.errMsg()).str().c_str());
        return false;
    }
    return true;
}

bool
runScriptPieces(boost::shared_ptr<qWorker::Logger> log,
                SqlConnection& sqlConn,
                SqlErrorObject& errObj,
                std::string const& scriptId, 
                std::string const& build, 
                std::string const& run, 
                std::string const& cleanup,
                qWorker::CheckFlag* checkAbort) {
    if ( runScriptPiece(log, sqlConn, errObj, scriptId, "QueryBuildSub", 
                        build, checkAbort) ) {
        if ( ! runScriptPiece(log, sqlConn, errObj, scriptId, "QueryExec", 
                              run, checkAbort) ) {
            (*log)((Pformat("Fail QueryExec phase for %1%: %2%") 
                   % scriptId % errObj.errMsg()).str().c_str());
        }
    }
    // Always destroy subchunks, no aborting.
    return runScriptPiece(log, sqlConn, errObj, scriptId, 
                          "QueryDestroySub", cleanup, 0);
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
void forEachSubChunk(std::string const& script, F& func) {
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

// FIXME: should be metadata or constant somewhere else.
const char SUB_CHUNK_COLUMN[] = "subChunkId";

template <typename T>
class ScScriptBuilder {
public:
    ScScriptBuilder(std::string const& db, std::string const& table, 
                    std::string const& scColumn, 
                    int chunkId_) : chunkId(chunkId_) {
        buildTemplate.assign((Pformat(qWorker::CREATE_SUBCHUNK_SCRIPT)
                              % db % table % scColumn
                              % chunkId % "%1%").str()); 
        cleanTemplate.assign((Pformat(qWorker::CLEANUP_SUBCHUNK_SCRIPT)
                              % db % table 
                              % chunkId % "%1%").str()); 

    }
    void operator()(T const& subc) {
        build << (Pformat(buildTemplate) % subc).str() 
              << "\n";
        clean << (Pformat(cleanTemplate) % subc).str()
              << "\n";
    }
    void reset(int chunkId_) {
        chunkId = chunkId_;
        build.str(std::string());
        clean.str(std::string());
    }
    std::string buildTemplate;
    std::string cleanTemplate;
    int chunkId;
    std::stringstream build;
    std::stringstream clean;
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunner
////////////////////////////////////////////////////////////////////////
qWorker::QueryRunner::QueryRunner(boost::shared_ptr<qWorker::Logger> log, 
                                  qWorker::Task::Ptr task, 
                                  std::string overrideDump) 
    : _log(log), _pResult(new QueryPhyResult()),
      _user(task->user.c_str()), _task(task), 
      _poisonedMutex(new boost::mutex()) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!overrideDump.empty()) {
        _task->resultPath = overrideDump;   
    }
}

qWorker::QueryRunner::QueryRunner(QueryRunnerArg const& a) 
    : _log(a.log), _pResult(new QueryPhyResult()),
      _user(a.task->user), _task(a.task),
      _poisonedMutex(new boost::mutex()) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!a.overrideDump.empty()) {
        _task->resultPath = a.overrideDump;   
    }
}

qWorker::QueryRunner::~QueryRunner() {
    mysql_thread_end();
}

bool qWorker::QueryRunner::operator()() {
    bool haveWork = true;
    Manager& mgr = getMgr();
    boost::shared_ptr<ArgFunc> afPtr(getResetFunc());
    mgr.addRunner(this);
    (*_log)((Pformat("(Queued: %1%, running: %2%)")
             % mgr.getQueueLength() % mgr.getRunnerCount()).str().c_str());
    while(haveWork) {
        if(_checkPoisoned()) {
            _poisonCleanup();
        } else {
            _act(); 
            // Might be wise to clean up poison for the current hash anyway.
        }
        (*_log)((Pformat("(Looking for work... Queued: %1%, running: %2%)")
                % mgr.getQueueLength() 
                % mgr.getRunnerCount()).str().c_str());
        bool reused = mgr.recycleRunner(afPtr.get(), _task->msg->chunkid());
        if(!reused) {
            mgr.dropRunner(this);
            haveWork = false;
        }
    } // finished with work.

    return true;
}

#if 0
bool qWorker::QueryRunner::operate2()() {
    
    bool haveWork = true;
    Manager& mgr = getMgr();
    boost::shared_ptr<ArgFunc> afPtr(getResetFunc());
    mgr.addRunner(this);
    (*_log)((Pformat("(Queued: %1%, running: %2%)")
            % mgr.getQueueLength() % mgr.getRunnerCount()).str().c_str());
    while(haveWork) {
        if(_checkPoisoned()) {
            _poisonCleanup();
        } else {
            _act(); 
            // Might be wise to clean up poison for the current hash anyway.
        }
        (*_log)((Pformat("(Looking for work... Queued: %1%, running: %2%)")
                % mgr.getQueueLength() 
                % mgr.getRunnerCount()).str().c_str());
        assert(_task.get()); 
        assert(_task->msg.get());
        bool reused = mgr.recycleRunner(afPtr.get(), _task->msg->chunkid());
        if(!reused) {
            mgr.dropRunner(this);
            haveWork = false;
        }
    } // finished with work.
    return true;
}
#endif

void qWorker::QueryRunner::poison(std::string const& hash) {
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    _poisoned.push_back(hash);
}
////////////////////////////////////////////////////////////////////////
// private:
////////////////////////////////////////////////////////////////////////
bool qWorker::QueryRunner::_checkPoisoned() {
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    StringDeque::const_iterator i = find(_poisoned.begin(), 
                                         _poisoned.end(), _task->hash);
    return i != _poisoned.end();
}

void qWorker::QueryRunner::_setNewQuery(QueryRunnerArg const& a) {
    //_e should be tied to the MySqlFs instance and constant(?)
    _user = a.task->user;
    _task = a.task;
    _errObj.reset();
    if(!a.overrideDump.empty()) {
        _task->resultPath = a.overrideDump;   
    }
}

bool qWorker::QueryRunner::actOnce() {
    return _act();

}
bool qWorker::QueryRunner::_act() {
    char msg[] = "Exec in flight for Db = %1%, dump = %2%";
    (*_log)((Pformat(msg) % _task->dbName % _task->resultPath).str().c_str());

    // Do not print query-- could be multi-megabytes.
    std::string dbDump = (Pformat("Db = %1%, dump = %2%")
                          % _task->dbName % _task->resultPath).str();
    (*_log)((Pformat("(fileobj:%1%) %2%")
            % (void*)(this) % dbDump).str().c_str());

    // Result files shouldn't get reused right now 
    // since we trash them after they are read once
#if 0 
    if (qWorker::dumpFileExists(_meta.resultPath)) {
        (*_log)((Pformat("Reusing pre-existing dump = %1% (chk=%2%)")
                % _task->resultPath % _task->chunkId).str().c_str());
        // The system should probably catch this earlier.
        getTracker().notify(_task->hash, ResultError(0,""));
        return true;
    }
#endif	
    if (!_runTask(_task)) {
        (*_log)((Pformat("(FinishFail:%1%) %2% hash=%3%")
                % (void*)(this) % dbDump % _task->hash).str().c_str());
        getTracker().notify(_task->hash,
                            ResultError(-1,"Script exec failure" 
                                        + _getErrorString()));
        return false;
    }
    (*_log)((Pformat("(FinishOK:%1%) %2%")
            % (void*)(this) % dbDump).str().c_str());    
    getTracker().notify(_task->hash, ResultError(0,""));
    return true;
}

bool qWorker::QueryRunner::_poisonCleanup() {
    StringDeque::iterator i;
    boost::lock_guard<boost::mutex> lock(*_poisonedMutex);
    i = find(_poisoned.begin(), _poisoned.end(), _task->hash);
    if(i == _poisoned.end()) {
        return false;
    }
    _poisoned.erase(i);
    return true;
}

std::string qWorker::QueryRunner::_getErrorString() const {
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
bool qWorker::QueryRunner::_runTask(qWorker::Task::Ptr t) {
    SqlConfig sc;
    sc.hostname = "";
    sc.username = _user.c_str();
    sc.password = "";
    sc.dbName = "";
    sc.port = 0;
    sc.socket = getConfig().getString("mysqlSocket").c_str();
    
    SqlConnection _sqlConn(sc);

    bool success = true;
    _scriptId = t->dbName.substr(0, 6);
    (*_log)((Pformat("TIMING,%1%ScriptStart,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    // For now, coalesce all fragments.
    std::string resultTable;
    _pResult->reset();
    assert(t.get());
    assert(t->msg.get());
    TaskMsg& m(*t->msg);
    if(!_sqlConn.connectToDb(_errObj)) {
        (*_log)((Pformat("Cfg error! connect MySQL as %1% using %2%") 
                % getConfig().getString("mysqlSocket") % _user).str().c_str());
        return _errObj.addErrMsg("Unable to connect to MySQL as " + _user);
    }
    for(int i=0; i < m.fragment_size(); ++i) {
        Task::Fragment const& f(m.fragment(i));
        ScScriptBuilder<int> scb("LSST", "Object", // FIXME: get from message
                                 SUB_CHUNK_COLUMN, t->msg->chunkid());
        std::stringstream ss;

        for(int j=0; j < f.subchunk_size(); ++j) {
            scb(f.subchunk(j));
        }
        if(f.has_resulttable()) { resultTable = f.resulttable(); }
        assert(!resultTable.empty());

        if(t->needsCreate) {
            if(!_pResult->hasResultTable(resultTable)) 
                ss << "CREATE TABLE " << resultTable << " ";
            else ss << "INSERT INTO " << resultTable << " ";
        }
        ss << f.query();
        success = _runFragment(_sqlConn, ss.str(), 
                               scb.build.str(), scb.clean.str(), 
                               resultTable);
        if(!success) return false;
        _pResult->addResultTable(resultTable);
    }
    if(success) {
        if(!_pResult->performMysqldump(*_log,_user,_task->resultPath,_errObj)) {
            return false;
        }
    }
    if (!_sqlConn.dropTable(_pResult->getCommaResultTables(), _errObj, false)) {
        return false;
    }
    return true;
}

// FIXME rework this function!!! Jacek
bool qWorker::QueryRunner::_runFragment(SqlConnection& sqlConn,
                                        std::string const& scr,
                                        std::string const& buildSc,
                                        std::string const& cleanSc,
                                        std::string const& resultTable) {
    boost::shared_ptr<CheckFlag> check(_makeAbort());
    
    if(!_prepareAndSelectResultDb(sqlConn)) {
        return false;
    }
    if(_checkPoisoned()) { // Check for poison
        _poisonCleanup(); // Clean it up.
        return false; 
    }
    if ( !runScriptPieces(_log, sqlConn, _errObj, _scriptId, buildSc, 
                          scr, cleanSc, check.get()) ) {
        return false;
    }
    (*_log)((Pformat("TIMING,%1%ScriptFinish,%2%")
             % _scriptId % ::time(NULL)).str().c_str());
    return true;
}

void qWorker::QueryRunner::_buildSubchunkScripts(std::string const& script,
                                                 std::string& build, 
                                                 std::string& cleanup) {
    ScScriptBuilder<std::string> scb("LSST", "Object", SUB_CHUNK_COLUMN,
                                     _task->msg->chunkid());
    (*_log)((Pformat("TIMING,%1%QueryFormatStart,%2%")
            % _scriptId % ::time(NULL)).str().c_str());
    
    forEachSubChunk(script, scb);
    build.assign(scb.build.str());
    cleanup.assign(scb.clean.str());

    (*_log)((Pformat("TIMING,%1%QueryFormatFinish,%2%")
            % _scriptId % ::time(NULL)).str().c_str());
}

bool 
qWorker::QueryRunner::_prepareAndSelectResultDb(SqlConnection& sqlConn, 
                                                std::string const& resultDb) {
    std::string result;
    std::string dbName(resultDb);

    if(sqlConn.dropDb(dbName, _errObj, false)) {
        (*_log)((Pformat("Cfg error! couldn't drop resultdb. %1%.")
                % result).str().c_str());
        return false;
    }
    if(!sqlConn.createDb(dbName, _errObj)) {
        (*_log)((Pformat("Cfg error! couldn't create resultdb. %1%.") 
                % result).str().c_str());
        return false;
    }
    if (!sqlConn.selectDb(dbName, _errObj)) {
        (*_log)((Pformat("Cfg error! couldn't select resultdb. %1%.") 
                % result).str().c_str());
        return _errObj.addErrMsg("Unable to select database " + dbName);
    }
    _pResult->setDb(dbName);
    return true;
}

bool qWorker::QueryRunner::_prepareScratchDb(SqlConnection& sqlConn) {
    std::string dbName = getConfig().getString("scratchDb");

    if ( !sqlConn.createDb(dbName, _errObj, false) ) {
        (*_log)((Pformat("Cfg error! couldn't create scratch db. %1%.") 
                % _errObj.errMsg()).str().c_str());
        return false;
    }
    if ( !sqlConn.selectDb(dbName, _errObj) ) {
        (*_log)((Pformat("Cfg error! couldn't select scratch db. %1%.") 
                % _errObj.errMsg()).str().c_str());
        return _errObj.addErrMsg("Unable to select database " + dbName);
    }
    return true;
}

std::string qWorker::QueryRunner::_getDumpTableList(std::string const& script) {
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

boost::shared_ptr<qWorker::ArgFunc> qWorker::QueryRunner::getResetFunc() {
    class ResetFunc : public ArgFunc {
    public:
        ResetFunc(QueryRunner* r) : runner(r) {}
        void operator()(QueryRunnerArg const& a) {
            runner->_setNewQuery(a);
        }
        QueryRunner* runner;
    };
    ArgFunc* af = new ResetFunc(this); 
    return boost::shared_ptr<qWorker::ArgFunc>(af);
}

boost::shared_ptr<qWorker::CheckFlag> qWorker::QueryRunner::_makeAbort() {
    class Check : public CheckFlag {
    public:
        Check(QueryRunner& qr) : runner(qr) {}
        virtual ~Check() {}
        bool operator()() { return runner._checkPoisoned(); }
        QueryRunner& runner;
    };
    CheckFlag* cf = new Check(*this);
    return boost::shared_ptr<CheckFlag>(cf);
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////
int qWorker::dumpFileOpen(std::string const& dbName) {
    return open(dbName.c_str(), O_RDONLY);
}

bool qWorker::dumpFileExists(std::string const& dumpFilename) {
    struct stat statbuf;
    return ::stat(dumpFilename.c_str(), &statbuf) == 0 &&
        S_ISREG(statbuf.st_mode) && (statbuf.st_mode & S_IRUSR) == S_IRUSR;
}
