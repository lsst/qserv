#include <fcntl.h>
#include <iostream>
#include "mysql/mysql.h"
#include <boost/regex.hpp>
#include "XrdSys/XrdSysError.hh"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Base.h"

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
	
        return std::string("Unable to execute query: ") + mysql_error(db) +
            "\nQuery = " + std::string(query, qSize);
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


std::string runQueryInPieces(MYSQL* db, std::string const& query,
			     std::string arg=std::string()) {
    // Run a larger query in pieces split by semicolon/newlines.
    // This tries to avoid the max_allowed_packet
    // (MySQL client/server protocol) problem.
    // MySQL default max_allowed_packet=1MB
    std::string queryPiece;
    std::string subResult;
    std::string delimiter = ";\n";
    std::string::size_type pieceBegin=0;
    std::string::size_type pieceEnd=0;
    std::string::size_type qEnd=query.length();
    std::string::size_type sizeTarget=25; // Is this too small?
    std::string::size_type searchTarget;
    unsigned pieceCount = 0;

    while(pieceEnd != qEnd) { 
	searchTarget = pieceBegin + sizeTarget;
	if(searchTarget < qEnd) {  // Is it worth splitting?
	    pieceEnd = query.rfind(delimiter, searchTarget);
	
	    // Did we find a split-point?
	    if((pieceEnd > pieceBegin) && (pieceEnd != std::string::npos)) {
		pieceEnd += delimiter.size();
	    } else {
		// Look forward instead of backward.
		pieceEnd = query.find(delimiter, pieceBegin + sizeTarget);
		if(pieceEnd != std::string::npos) { // Found?
		    pieceEnd += delimiter.size(); 
		} else { // Not found bkwd/fwd. Use end.
		    pieceEnd = qEnd; 
		}
	    }
	} else { // Remaining is small. Don't split further.
	    pieceEnd = qEnd; 
	}

	//queryPiece = "";
	// Backoff whitepace or null.
	int pos = pieceEnd;
	char c = query[pos];
	char const* queryPiece;
	int pieceSize = 0;
	while((c == '\0') || (c == '\n') 
	      || (c == ' ') || (c == '\t')) { c = query[--pos];}
	if (pos > (int)pieceBegin) {
	    queryPiece = query.data() + pieceBegin;
	    pieceSize = pos - (int)pieceBegin;
	    //queryPiece.assign(query, pieceBegin, pos-(int)pieceBegin);
	}
	// Catch empty strings.
	if(pieceSize && queryPiece[0] != '\0') {
	    //std::cout << "PIECE--" << queryPiece << "--" << std::endl;
	    subResult = runQuery(db, queryPiece, pieceSize, arg);
	  }
	// On error, the partial error is as good as the global.
	if(!subResult.empty()) {
	    unsigned s=pieceEnd-pieceBegin;
	    return (Pformat("%1%---Error with piece %2% complete (size=%3%).") 
		    % subResult % pieceCount % s).str();
	}
	++pieceCount;
	//std::cout << Pformat("Piece %1% complete.") % pieceCount;

	pieceBegin = pieceEnd;
    }
    // Can't use _eDest (we are in file-scope)
    //std::cout << Pformat("Executed query in %1% pieces.") % pieceCount;
    
    // Getting here means that none of the pieces failed.
    return std::string();
}

std::string runScriptPiece(XrdSysError& e,
			    MYSQL*const db,
			    std::string const& scriptId, 
			    std::string const& pieceName,
			    std::string const& piece) {
    std::string result;
    e.Say((Pformat("TIMING,%1%%2%Start,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    //e.Say(("Hi. my piece is++"+piece+"++").c_str());
	
    result = runQueryInPieces(db, piece);
    e.Say((Pformat("TIMING,%1%%2%Finish,%3%")
                 % scriptId % pieceName % ::time(NULL)).str().c_str());
    if(!result.empty()) {
	result += "(during " + pieceName + ")\nQueryFragment: " + piece;
	e.Say((Pformat("Broken! ,%1%%2%---%3%")
		     % scriptId % pieceName % result).str().c_str());

    }
    return result;
}		   

std::string runScriptPieces(XrdSysError& e,
			    MYSQL*const db,
			    std::string const& scriptId, 
			    std::string const& build, 
			    std::string const& run, 
			    std::string const& cleanup) {
    std::string result;    
    std::string eResult;

    result = runScriptPiece(e, db, scriptId, "QueryBuildSub", build);
    if(result.empty()) {
	eResult = runScriptPiece(e, db, scriptId, "QueryExec", run);
	if(eResult.empty()) {
	} else {
	    e.Say((Pformat("Fail QueryExec phase for %1%: %2%") 
		   % scriptId % result).str().c_str());
	    result += eResult;
	}
	// Always destroy subchunks.
	result += runScriptPiece(e, db, scriptId, "QueryDestroySub", cleanup);
    } 
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
// lsst::qserv::worker::ExecEnv
////////////////////////////////////////////////////////////////////////
qWorker::ExecEnv& qWorker::getExecEnv() {
    static ExecEnv e;
    static boost::mutex m;

    boost::lock_guard<boost::mutex> lock(m);
    if(!e._isReady) {
	e._setup();
    } 
    return e;
}

char const* qWorker::ExecEnv::_getEnvDefault(char const* varName, 
                                             char const* defVal) {
    char const* s = ::getenv(varName);
    if(s != (char const*)0) { return s; 
    } else { 
        return defVal; 
    }
}

void qWorker::ExecEnv::_setup() {
    // Try to capture socket filename from environment
    char* sock = ::getenv("QSW_DBSOCK");
    if(sock != (char*)0) { _socketFilename = sock; }
    else { _socketFilename = "/var/lib/mysql/mysql.sock"; }

    // Capture alternative mysqldump
    char* path = ::getenv("QSW_MYSQLDUMP");
    if(path != (char*)0) { _mysqldumpPath = path; }
    else { _mysqldumpPath = "/usr/bin/mysqldump"; }

    // Capture alternative shared scratch db
    _scratchDb = _getEnvDefault("QSW_SCRATCHDB", "qservScratch");
}

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunnerManager
////////////////////////////////////////////////////////////////////////
qWorker::QueryRunnerArg const& qWorker::QueryRunnerManager::getQueueHead() const {
    assert(!_queue.empty());
    return _queue.front();
}

void qWorker::QueryRunnerManager::add(qWorker::QueryRunnerArg const& a) {
    ++_jobTotal;
    _queue.push_back(a);
}

void qWorker::QueryRunnerManager::popQueueHead() {
    assert(!_queue.empty());
    _queue.pop_front();
}

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunner
////////////////////////////////////////////////////////////////////////
qWorker::QueryRunner::QueryRunner(XrdSysError& e, 
				  std::string const& user,
				  qWorker::ScriptMeta const& s, 
				  std::string overrideDump) 
    : _env(getExecEnv()), _e(e), _user(user.c_str()), _meta(s) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!overrideDump.empty()) {
	_meta.resultPath = overrideDump;   
    }
}

qWorker::QueryRunner::QueryRunner(QueryRunnerArg const& a) 
    : _env(getExecEnv()), _e(a.e), _user(a.user), _meta(a.s) {
    int rc = mysql_thread_init();
    assert(rc == 0);
    if(!a.overrideDump.empty()) {
	_meta.resultPath = a.overrideDump;   
    }
}

qWorker::QueryRunner::~QueryRunner() {
    mysql_thread_end();
}

bool qWorker::QueryRunner::operator()() {
    bool haveWork = true;
    Manager& mgr = getMgr();
    {
	boost::lock_guard<boost::mutex> m(mgr.getMutex());
	mgr.addRunner();
	_e.Say((Pformat("(Queued: %1%, running: %2%)")
		% mgr.getQueueLength() % mgr.getRunnerCount()).str().c_str());
	
    }
    while(haveWork) {
	_act();
	{
	    boost::lock_guard<boost::mutex> m(mgr.getMutex());
	    _e.Say((Pformat("(Looking for work... Queued: %1%, running: %2%)")
		    % mgr.getQueueLength() 
		    % mgr.getRunnerCount()).str().c_str());

	    if((!mgr.isOverloaded()) && (mgr.getQueueLength() > 0)) {
		_setNewQuery(mgr.getQueueHead()); // Switch to new query
		mgr.popQueueHead();
	    } else {
		mgr.dropRunner();
		haveWork = false;
	    }
	}
	
    } // finished with work.

    return true;
}

////////////////////////////////////////////////////////////////////////
// private:
////////////////////////////////////////////////////////////////////////
void qWorker::QueryRunner::_setNewQuery(QueryRunnerArg const& a) {
    // _env should be identical since it's static (for sure?).
    //_e should be tied to the MySqlFs instance and constant(?)
    _user = a.user;
    _meta = a.s;
    _errorDesc.clear();
    _errorNo = 0;
    if(!a.overrideDump.empty()) {
	_meta.resultPath = a.overrideDump;   
    }
}

bool qWorker::QueryRunner::_act() {
    char msg[] = "Exec in flight for Db = %1%, dump = %2%";
    _e.Say((Pformat(msg) % _meta.dbName % _meta.resultPath).str().c_str());

    // Do not print query-- could be multi-megabytes.
    std::string dbDump = (Pformat("Db = %1%, dump = %2%")
			  % _meta.dbName % _meta.resultPath).str();
    _e.Say((Pformat("(fileobj:%1%) %2%")
	    % (void*)(this) % dbDump).str().c_str());

    if (qWorker::dumpFileExists(_meta.resultPath)) {
	_e.Say((Pformat("Reusing pre-existing dump = %1%")
		% _meta.resultPath).str().c_str());
	// The system should probably catch this earlier.
	getTracker().notify(_meta.hash, ResultError(0,""));
	return true;
    }
	
    if (!_runScript(_meta.script, _meta.dbName)) {
	_e.Say((Pformat("(FinishFail:%1%) %2% hash=%3%")
		% (void*)(this) % dbDump % _meta.hash).str().c_str());
	getTracker().notify(_meta.hash,
			    ResultError(-1,"Script exec failure" 
					+ _getErrorString()));
	return false;
    } 

    _e.Say((Pformat("(FinishOK:%1%) %2%")
	    % (void*)(this) % dbDump).str().c_str());
    getTracker().notify(_meta.hash, ResultError(0,""));
    return true;
}

void qWorker::QueryRunner::_appendError(int errorNo, std::string const& desc) {
    if(_errorNo != 0) _errorNo = errorNo;
    _errorDesc += desc;
}

bool qWorker::QueryRunner::_connectDbServer(MYSQL* db) {
    if(mysql_real_connect(db, 
			  0,  // host
			  _user.c_str(), // user
			  0, 0, 0, //passwd, db, port
			  _env.getSocketFilename().c_str(), // socket
			  CLIENT_MULTI_STATEMENTS) == 0) {
        _appendError(EIO, "Unable to connect to MySQL as " + _user);
	_e.Say((Pformat("Cfg error! connect Mysql as %1% using %2%") 
		% _env.getSocketFilename() % _user).str().c_str());
        return false;
    }
    return true;
}

bool qWorker::QueryRunner::_dropDb(MYSQL* db, std::string const& name) {
    std::string result = runQuery(db, 
                                  "DROP DATABASE IF EXISTS " + name);
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    return true;
}

bool qWorker::QueryRunner::_dropTables(MYSQL* db, 
                                       std::string const& commaTables) {
    std::string result = runQuery(db, 
                                  "DROP TABLE IF EXISTS " + commaTables);
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    return true;
}

std::string qWorker::QueryRunner::_getErrorString() const {
    return (Pformat("%1%: %2%") % _errorNo % _errorDesc).str();
}
bool qWorker::QueryRunner::_isExecutable(std::string const& execFile) {
     return 0 == ::access(execFile.c_str(), X_OK);
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

    if(!_isExecutable(_env.getMysqldumpPath())) {
	// Shell exec will crash a boost test case badly in this case.
	return false; // Can't do dump w/o an executable.
    }
    std::string cmd = _env.getMysqldumpPath() + (Pformat(
            " --compact --add-locks --create-options --skip-lock-tables"
	    " --socket=%1%"
            " --result-file=%2% %3% %4%")
            % _env.getSocketFilename() % dumpFile % dbName % tables).str();
    _e.Say((Pformat("dump cmdline: %1%") % cmd).str().c_str());

    _e.Say((Pformat("TIMING,%1%QueryDumpStart,%2%")
	    % _scriptId % ::time(NULL)).str().c_str());
    int cmdResult = system(cmd.c_str());

    _e.Say((Pformat("TIMING,%1%QueryDumpFinish,%2%")
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
        realDbName = _env.getScratchDb();
    }

    _buildSubchunkScripts(script, buildScript, cleanupScript);
    std::string result = runScriptPieces(_e, db, _scriptId, buildScript, 
			     script, cleanupScript);
    if(!result.empty()) { 
        _appendError(EIO, result); 
        return false;
    }
    else if(!_performMysqldump(realDbName, _meta.resultPath, tableList)) {
        _appendError(EIO, "mysqldump failure");
	return false;
    }
    
    // Record query in query cache table
    /*
    result = runQuery(db.get(),
                      "INSERT INTO qcache.Queries "
                      "(queryTime, query, db, path) "
                      "VALUES (NOW(), ?, "
                          "'" + dbName + "'"
                          ", "
                          "'" + _meta.resultPath + "'"
                          ")",
                      script);
    if (result.size() != 0) {
        _errorNo = EIO;
        _errorDesc += result;
        return false;
    }
    */
    return true;
}

bool qWorker::QueryRunner::_runScript(
    std::string const& script, std::string const& dbName) {
    
    DbHandle db;
    std::string result;
    std::string tables; 
    bool scriptSuccess = false;

    _scriptId = dbName.substr(0, 6);
    _e.Say((Pformat("TIMING,%1%ScriptStart,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    if(!_connectDbServer(db.get())) {
        return false;
    }
    tables = _getDumpTableList(script);
    // _e.Say((Pformat("Dump tables for %1%: %2%") 
    //         % _scriptId % tables).str().c_str());
    if(tables.empty()) {
        if(!_prepareAndSelectResultDb(db.get(), dbName)) {
            return false;
        }
    } else if(!_prepareScratchDb(db.get())) {
        return false;
    }
    
    scriptSuccess = _runScriptCore(db.get(), script, dbName, commasToSpaces(tables));

    if(!tables.empty()) {
        _dropTables(db.get(), tables);
    } else {
        _dropDb(db.get(), dbName);
    }

    _e.Say((Pformat("TIMING,%1%ScriptFinish,%2%")
                 % _scriptId % ::time(NULL)).str().c_str());
    return _errorDesc.empty();
}

void qWorker::QueryRunner::_buildSubchunkScripts(std::string const& script,
						 std::string& build, 
						 std::string& cleanup) {
    std::string firstLine = script.substr(0, script.find('\n'));
    int subChunkCount = 0;
    _e.Say((Pformat("TIMING,%1%QueryFormatStart,%2%")
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
             % _meta.chunkId % subChunk).str() + "\n";
        cleanup +=
            (Pformat(CLEANUP_SUBCHUNK_SCRIPT)
             % _meta.chunkId % subChunk).str() + "\n";
	++subChunkCount;
#ifdef DO_NOT_USE_BOOST // workaround emacs indent rules.
    }
#else
    }
#endif
    _e.Say((Pformat("TIMING,%1%QueryFormatFinish,%2%")
	    % _scriptId % ::time(NULL)).str().c_str());
}

bool qWorker::QueryRunner::_prepareAndSelectResultDb(MYSQL* db, 
						     std::string const& dbName) {
    std::string result;
    if(!_dropDb(db, dbName)) {
        _e.Say((Pformat("Cfg error! couldn't drop resultdb. %1%.")
                % result).str().c_str());
        return false;
    }

    result = runQuery(db, "CREATE DATABASE " + dbName);
    if (!result.empty()) {
	_errorNo = EIO;
	_errorDesc += result;
	_e.Say((Pformat("Cfg error! couldn't create resultdb. %1%.") 
		% result).str().c_str());
        return false;
    }
    if (mysql_select_db(db, dbName.c_str()) != 0) {
	_errorNo = EIO;
	_errorDesc += "Unable to select database " + dbName;
	_e.Say((Pformat("Cfg error! couldn't select resultdb. %1%.") 
		% result).str().c_str());
        return false;
    }
    return true;
}


bool qWorker::QueryRunner::_prepareScratchDb(MYSQL* db) {
    std::string dbName = _env.getScratchDb();

    std::string result = runQuery(db, "CREATE DATABASE IF NOT EXISTS " 
                                  + dbName);
    if (!result.empty()) {
	_errorNo = EIO;
	_errorDesc += result;
	_e.Say((Pformat("Cfg error! couldn't create scratch db. %1%.") 
		% result).str().c_str());
        return false;
    }
    if (mysql_select_db(db, dbName.c_str()) != 0) {
	_errorNo = EIO;
	_errorDesc += "Unable to select database " + dbName;
	_e.Say((Pformat("Cfg error! couldn't select scratch db. %1%.") 
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

