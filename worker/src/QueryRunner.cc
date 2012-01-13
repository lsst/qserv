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
 
#include <fcntl.h>
#include <iostream>
#include "mysql/mysql.h"
#include <boost/regex.hpp>
#include "XrdSys/XrdSysError.hh"
#include "lsst/qserv/worker/QueryRunner.h"
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
    
} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunner
////////////////////////////////////////////////////////////////////////
qWorker::QueryRunner::QueryRunner(boost::shared_ptr<qWorker::Logger> log, 
                                  std::string const& user,
                                  qWorker::Task::Ptr task, 
                                  std::string overrideDump) 
    : _log(log), _user(user.c_str()), _task(task), 
      _poisonedMutex(new boost::mutex()) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!overrideDump.empty()) {
        _task->resultPath = overrideDump;   
    }
}

qWorker::QueryRunner::QueryRunner(QueryRunnerArg const& a) 
    : _log(a.log), _user(a.user), _task(a.task),
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
    _user = a.user;
    _task = a.task;
    _errorDesc.clear();
    _errorNo = 0;
    if(!a.overrideDump.empty()) {
        _task->resultPath = a.overrideDump;   
    }
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

void qWorker::QueryRunner::_mkdirP(std::string const& filePath) {
    // Quick and dirty mkdir -p functionality.  No error checking.
    std::string::size_type pos = 0;
    struct stat statbuf;
    while ((pos = filePath.find('/', pos + 1)) != std::string::npos) {
        std::string dir(filePath, 0, pos);
        if (::stat(dir.c_str(), &statbuf) == -1) {
            if (errno == ENOENT) {
                mkdir(dir.c_str(), 0777);
            }
        }
    }
}

bool qWorker::QueryRunner::_performMysqldump(std::string const& dbName, 
                                             std::string const& dumpFile,
                                             std::string const& tables) {
    
    // Dump a database to a dumpfile.
    
    // Make sure the path exists
    _mkdirP(dumpFile);

    std::string cmd = getConfig().getString("mysqlDump") + 
        (Pformat(
            " --compact --add-locks --create-options --skip-lock-tables"
	    " --socket=%1%"
            " -u %2%"
            " --result-file=%3% %4% %5%")
         % getConfig().getString("mysqlSocket") 
         % _user
         % dumpFile % dbName % tables).str();
    (*_log)((Pformat("dump cmdline: %1%") % cmd).str().c_str());

    (*_log)((Pformat("TIMING,%1%QueryDumpStart,%2%")
            % _scriptId % ::time(NULL)).str().c_str());
    int cmdResult = system(cmd.c_str());

    (*_log)((Pformat("TIMING,%1%QueryDumpFinish,%2%")
            % _scriptId % ::time(NULL)).str().c_str());

    if (cmdResult != 0) {
        _errorNo = errno;
        _errorDesc += "Unable to dump database " + dbName 
            + " to " + dumpFile;
        return false;
    }
    return true;
}

bool qWorker::QueryRunner::_runScriptCore(MYSQL* db, std::string const& script,
                                          std::string const& dbName,
                                          std::string const& tableList) {
    std::string buildScript;
    std::string cleanupScript;
    std::string realDbName(dbName);

    if(!tableList.empty()) {
        realDbName = getConfig().getString("scratchDb");
    }
    boost::shared_ptr<CheckFlag> check(_makeAbort());
    _buildSubchunkScripts(script, buildScript, cleanupScript);
    std::string result = runScriptPieces(_log, db, _scriptId, buildScript, 
                                         script, cleanupScript, check.get());
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    else if(!_performMysqldump(realDbName, _task->resultPath, tableList)) {
        _appendError(EIO, "mysqldump failure");
        return false;
    }
    return true;
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
    _scriptId = t->dbName.substr(0, 6);
    (*_log)((Pformat("TIMING,%1%ScriptStart,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    // For now, coalesce all fragments.
    std::stringstream ss;
    IntVector sc;
    assert(t.get());
    assert(t->msg.get());
    TaskMsg& m(*t->msg);
    for(int i=0; i < m.fragment_size(); ++i) {
        Task::Fragment const& f(m.fragment(i));
        ss << f.query();
        for(int j=0; j < f.subchunk_size(); ++j) {
            sc.push_back(f.subchunk(j));
        }
    }
    return _runFragment(ss.str(), sc, t->dbName);

}
bool qWorker::QueryRunner::_runFragment(std::string const& fscr,
                                        IntVector const& sc,
                                        std::string const& dbName) {
    
}

bool qWorker::QueryRunner::_runScript(
    std::string const& script, std::string const& dbName) {
    
    DbHandle db;
    std::string result;
    std::string tables; 
    bool scriptSuccess = false;

    _scriptId = dbName.substr(0, 6);
    (*_log)((Pformat("TIMING,%1%ScriptStart,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    if(!_connectDbServer(db.get())) {
        return false;
    }
    tables = _getDumpTableList(script);
    // (*_log)((Pformat("Dump tables for %1%: %2%") 
    //         % _scriptId % tables).str().c_str());
    if(tables.empty()) {
        if(!_prepareAndSelectResultDb(db.get(), dbName)) {
            return false;
        }
    } else if(!_prepareScratchDb(db.get())) {
        return false;
    }
    if(_checkPoisoned()) { // Check for poison
        _poisonCleanup(); // Clean it up.
        return false; 
    }
    scriptSuccess = _runScriptCore(db.get(), script, dbName, 
                                   commasToSpaces(tables));

    if(!tables.empty()) {
        _dropTables(db.get(), tables);
    } else {
        _dropDb(db.get(), dbName);
    }

    (*_log)((Pformat("TIMING,%1%ScriptFinish,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    return _errorDesc.empty();
}

void qWorker::QueryRunner::_buildSubchunkScripts(std::string const& script,
                                                 std::string& build, 
                                                 std::string& cleanup) {
    std::string firstLine = script.substr(0, script.find('\n'));
    int subChunkCount = 0;
    (*_log)((Pformat("TIMING,%1%QueryFormatStart,%2%")
            % _scriptId % ::time(NULL)).str().c_str());
    
#ifdef DO_NOT_USE_BOOST
    Regex re("[0-9][0-9]*");
    for(Regex::Iterator i = re.newIterator(firstLine);
	i != Regex::Iterator::end(); ++i) {
#else
    boost::regex re("\\d+");
    for (boost::sregex_iterator i = boost::make_regex_iterator(firstLine, re);
         i != boost::sregex_iterator(); ++i) {
#endif
        std::string subChunk = (*i).str(0);
        build +=
            (Pformat(CREATE_SUBCHUNK_SCRIPT)
             % _task->msg->chunkid() % subChunk).str() + "\n";
        cleanup +=
            (Pformat(CLEANUP_SUBCHUNK_SCRIPT)
             % _task->msg->chunkid() % subChunk).str() + "\n";
	++subChunkCount;
#ifdef DO_NOT_USE_BOOST // workaround emacs indent rules.
    }
#else
    }
#endif
    (*_log)((Pformat("TIMING,%1%QueryFormatFinish,%2%")
            % _scriptId % ::time(NULL)).str().c_str());
}

bool 
qWorker::QueryRunner::_prepareAndSelectResultDb(MYSQL* db, 
                                                std::string const& dbName) {
    std::string result;
    if(!_dropDb(db, dbName)) {
        (*_log)((Pformat("Cfg error! couldn't drop resultdb. %1%.")
                % result).str().c_str());
        return false;
    }

    result = runQuery(db, "CREATE DATABASE " + dbName);
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

