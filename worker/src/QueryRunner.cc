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
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/QueryPhyResult.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/worker/SqlFragmenter.h"

namespace qWorker = lsst::qserv::worker;

namespace {

class DbHandle {
public:
    DbHandle(void) : _db((MYSQL*)malloc(sizeof(MYSQL))) { 
        if(_db) mysql_init(_db);	
    };
    ~DbHandle(void) {
        if (_db) {
            mysql_close(_db);
            free(_db);
            _db = 0;
        }
    };
    MYSQL* get(void) const { return _db; };
private:
    MYSQL* _db;
};

std::string runQuery(MYSQL* db, char const*  query, int qSize,
                     std::string arg=std::string()) {
    if(arg.size() != 0) {
        // TODO -- bind arg
    }
    if(mysql_real_query(db, query, qSize) != 0) {
	MYSQL_RES* result = mysql_store_result(db);
	if(result) mysql_free_result(result);
	
        return std::string("Unable to execute query: ") + mysql_error(db);
        //    + "\nQuery = " + std::string(query, qSize);
    }
    int status = 0;
    do {
        MYSQL_RES* result = mysql_store_result(db);
        if (result) {
            // TODO -- Do something with it?
            mysql_free_result(result);
        }
        else if (mysql_field_count(db) != 0) {
            return std::string("Unable to store result for query: ") 
                + std::string(query, qSize);
        }
        status = mysql_next_result(db);
        if (status > 0) {
            return std::string("Error retrieving results for query: ") +
                mysql_error(db) + "\nQuery = " + std::string(query, qSize);
        }
    } while (status == 0);
    return std::string();
}

std::string runQuery(MYSQL* db, std::string const query, 
                     std::string arg=std::string()) {
    return runQuery(db, query.data(), query.size(), arg);
}

std::string runQueryInPieces(boost::shared_ptr<qWorker::Logger> log, MYSQL* db, 
                             std::string const& query,
                             qWorker::CheckFlag* checkAbort) {
    // Run a larger query in pieces split by semicolon/newlines.
    // This tries to avoid the max_allowed_packet
    // (MySQL client/server protocol) problem.
    // MySQL default max_allowed_packet=1MB
    std::string subResult;
    qWorker::SqlFragmenter sf(query);
    while(!sf.isDone()) {
        qWorker::SqlFragmenter::Piece p = sf.getNextPiece();
        subResult = runQuery(db, p.first, p.second);
        // On error, the partial error is as good as the global.
        if(!subResult.empty()) {
            unsigned s=p.second;
            (*log)((Pformat(">>%1%<<---Error with piece %2% complete (size=%3%).") % subResult % sf.getCount() % s).str().c_str());
            return subResult;
        } else if(checkAbort && (*checkAbort)()) {
            if(sf.isDone()) {
                (*log)("Query finished, though client requested abort.");
            } else {
                (*log)((Pformat("Aborting query by request (%1% complete).") 
                       % sf.getCount()).str().c_str());
                return std::string("Query poisoned by client request.");
            }
        }
    }
    // Can't use _eDest (we are in file-scope)
    //std::cout << Pformat("Executed query in %1% pieces.") % pieceCount;
    
    // Getting here means that none of the pieces failed.
    return std::string();
}

std::string runScriptPiece(boost::shared_ptr<qWorker::Logger> log,
                           MYSQL*const db,
                           std::string const& scriptId, 
                           std::string const& pieceName,
                           std::string const& piece, 
                           qWorker::CheckFlag* checkAbort) {
    std::string result;
    (*log)((Pformat("TIMING,%1%%2%Start,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    //(*log)(("Hi. my piece is++"+piece+"++").c_str());
	
    result = runQueryInPieces(log, db, piece, checkAbort);
    (*log)((Pformat("TIMING,%1%%2%Finish,%3%")
           % scriptId % pieceName % ::time(NULL)).str().c_str());
    if(!result.empty()) {
        result += "(during " + pieceName + ")\nQueryFragment: " + piece;
        (*log)((Pformat("Broken! ,%1%%2%---%3%")
               % scriptId % pieceName % result).str().c_str());
    }
    return result;
}	   

std::string runScriptPieces(boost::shared_ptr<qWorker::Logger> log,
                            MYSQL*const db,
                            std::string const& scriptId, 
                            std::string const& build, 
                            std::string const& run, 
                            std::string const& cleanup,
                            qWorker::CheckFlag* checkAbort) {
    std::string result;
    std::string eResult;

    result = runScriptPiece(log, db, scriptId, "QueryBuildSub", build, 
                            checkAbort);
    if(result.empty()) {
        eResult = runScriptPiece(log, db, scriptId, "QueryExec", run,
                                 checkAbort);
        if(eResult.empty()) {
        } else {
            (*log)((Pformat("Fail QueryExec phase for %1%: %2%") 
                   % scriptId % result).str().c_str());
            result += eResult;
        }
    }
    // Always destroy subchunks, no aborting.
    result += runScriptPiece(log, db, scriptId, "QueryDestroySub", cleanup, 0);
    return result;
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
    _errorDesc.clear();
    _errorNo = 0;
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

void 
qWorker::QueryRunner::_appendError(int errorNo, std::string const& desc) {
    if(_errorNo != 0) _errorNo = errorNo;
    _errorDesc += desc;
}

bool qWorker::QueryRunner::_connectDbServer(MYSQL* db) {
    if(mysql_real_connect(db, 
                          0,  // host
                          _user.c_str(), // user
                          0, 0, 0, //passwd, db, port
                          getConfig().getString("mysqlSocket").c_str(), 
                          CLIENT_MULTI_STATEMENTS) == 0) {
        _appendError(EIO, "Unable to connect to MySQL as " + _user);
        (*_log)((Pformat("Cfg error! connect Mysql as %1% using %2%") 
                % getConfig().getString("mysqlSocket") % _user).str().c_str());
        return false;
    }
    return true;
}

bool qWorker::QueryRunner::_dropDb(MYSQL* db, std::string const& name) {
    std::string result = runQuery(db, "DROP DATABASE IF EXISTS " + name);
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    return true;
}

bool qWorker::QueryRunner::_dropTables(MYSQL* db, 
                                       std::string const& commaTables) {
    std::string result = runQuery(db, "DROP TABLE IF EXISTS " + commaTables);
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    return true;
}

std::string qWorker::QueryRunner::_getErrorString() const {
    return (Pformat("%1%: %2%") % _errorNo % _errorDesc).str();
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
    DbHandle db;
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
    if(!_connectDbServer(db.get())) {
        return false;
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
        success = _runFragment(db.get(), ss.str(), 
                               scb.build.str(), scb.clean.str(), 
                               resultTable);
        if(!success) return false;
        _pResult->addResultTable(resultTable);
    }
    if(success) {
        if(!_pResult->performMysqldump(*_log, _user, _task->resultPath,
                                       _errorNo, _errorDesc)) {
            _appendError(EIO, "mysqldump failure");
            return false;
        }
    }
    _dropTables(db.get(), _pResult->getCommaResultTables());
    return true;
}

bool qWorker::QueryRunner::_runFragment(MYSQL* dbMy,
                                        std::string const& scr,
                                        std::string const& buildSc,
                                        std::string const& cleanSc,
                                        std::string const& resultTable) {
    boost::shared_ptr<CheckFlag> check(_makeAbort());
    
    if(!_prepareAndSelectResultDb(dbMy)) {
        return false;
    }
    if(_checkPoisoned()) { // Check for poison
        _poisonCleanup(); // Clean it up.
        return false; 
    }
    std::string result = runScriptPieces(_log, dbMy, _scriptId, buildSc, 
                                         scr, cleanSc, check.get());
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    } 

    (*_log)((Pformat("TIMING,%1%ScriptFinish,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    return _errorDesc.empty();

    return false;
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
qWorker::QueryRunner::_prepareAndSelectResultDb(MYSQL* db, 
                                                std::string const& resultDb) {
    std::string result;
    std::string dbName(resultDb);
    
    if(dbName.empty()) {
         dbName = getConfig().getString("scratchDb");
    } else if(!_dropDb(db, dbName)) {
        (*_log)((Pformat("Cfg error! couldn't drop resultdb. %1%.")
                % result).str().c_str());
        return false;
    }

    result = runQuery(db, "CREATE DATABASE IF NOT EXISTS " + dbName);
    if (!result.empty()) {
        _errorNo = EIO;
        _errorDesc += result;
        (*_log)((Pformat("Cfg error! couldn't create resultdb. %1%.") 
                % result).str().c_str());
        return false;
    }
    if (mysql_select_db(db, dbName.c_str()) != 0) {
        _errorNo = EIO;
        _errorDesc += "Unable to select database " + dbName;
        (*_log)((Pformat("Cfg error! couldn't select resultdb. %1%.") 
                % result).str().c_str());
        return false;
    }
    _pResult->setDb(dbName);
    return true;
}

bool qWorker::QueryRunner::_prepareScratchDb(MYSQL* db) {
    std::string dbName = getConfig().getString("scratchDb");

    std::string result = runQuery(db, "CREATE DATABASE IF NOT EXISTS " 
                                  + dbName);
    if (!result.empty()) {
        _errorNo = EIO;
        _errorDesc += result;
        (*_log)((Pformat("Cfg error! couldn't create scratch db. %1%.") 
                % result).str().c_str());
        return false;
    }
    if (mysql_select_db(db, dbName.c_str()) != 0) {
        _errorNo = EIO;
        _errorDesc += "Unable to select database " + dbName;
        (*_log)((Pformat("Cfg error! couldn't select scratch db. %1%.") 
                % result).str().c_str());
        return false;
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

