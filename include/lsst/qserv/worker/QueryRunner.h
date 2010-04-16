#ifndef LSST_QSERV_WORKER_QUERYRUNNER_H
#define LSST_QSERV_WORKER_QUERYRUNNER_H

// C++
#include <deque>

// boost
#include "boost/thread.hpp" // for mutex support
// xrootd
#include "XrdSfs/XrdSfsInterface.hh"

// package
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/ResultTracker.h"


namespace lsst {
namespace qserv {
namespace worker {


////////////////////////////////////////////////////////////////////////
class ExecEnv {
public:
    std::string const& getSocketFilename() const { return _socketFilename; }
    std::string const& getMysqldumpPath() const { return _mysqldumpPath; }
private:
    ExecEnv() : _isReady(false){}
    void _setup();
    
    bool _isReady;
    // trim this list.
    std::string _socketFilename;
    std::string _mysqldumpPath;

    friend ExecEnv& getExecEnv();
};

ExecEnv& getExecEnv();
////////////////////////////////////////////////////////////////////////
class QueryRunnerArg {
public:
    QueryRunnerArg(XrdOucErrInfo& ei_, XrdSysError& e_, 
		   std::string const& user_, ScriptMeta const& s_,
		   std::string overrideDump_=std::string()) 
	: ei(ei_), e(e_), user(user_), s(s_), overrideDump(overrideDump_) { }

    XrdOucErrInfo& ei;
    XrdSysError& e;
    std::string user;
    ScriptMeta s;
    std::string overrideDump;
};

////////////////////////////////////////////////////////////////////////
class QueryRunnerManager {
public:
 QueryRunnerManager() : _limit(8), _jobTotal(0) {}
    ~QueryRunnerManager() {}

    // const
    bool hasSpace() const { return _running < _limit; }
    QueryRunnerArg const& getQueueHead() const;
    int getQueueLength() const { return _queue.size();}
    int getRunnerCount() const { return _running;}

    // non-const
    void add(QueryRunnerArg const& a);
    void setSpaceLimit(int limit) { _limit = limit; }
    void popQueueHead();
    void addRunner() { ++_running; }
    void dropRunner() { --_running; }

    // Mutex
    boost::mutex& getMutex() { return _mutex; }

private:
    typedef std::deque<QueryRunnerArg> QueryQueue;
    QueryQueue _queue;
    int _jobTotal;
    int _running;
    int _limit;
    boost::mutex _mutex;    
};

////////////////////////////////////////////////////////////////////////
class QueryRunner {
public:
    typedef ResultTracker<std::string, ErrorPair> Tracker;
    typedef QueryRunnerManager Manager;
    QueryRunner(XrdOucErrInfo& ei, XrdSysError& e, 
		std::string const& user, ScriptMeta const& s,
		std::string overrideDump=std::string());
    QueryRunner(QueryRunnerArg const& a);
    bool operator()();

    // Static: 
    static Tracker& getTracker() { static Tracker t; return t;}
    static Manager& getMgr() { static Manager m; return m;}

private:
    bool _act();
    void _mkdirP(std::string const& filePath);
    bool _runScript(std::string const& script, std::string const& dbName);
    bool _performMysqldump(std::string const& dbName, 
			   std::string const& dumpFile);
    bool _isExecutable(std::string const& execName);
    void _setNewQuery(QueryRunnerArg const& a);

    ExecEnv& _env;
    XrdOucErrInfo& _errinfo;
    XrdSysError& _e;
    std::string _user;
    ScriptMeta _meta;
    std::string _scriptId;

};

 int dumpFileOpen(std::string const& dbName);
 bool dumpFileExists(std::string const& dumpFilename);


}}}
#endif // LSST_QSERV_WORKER_QUERYRUNNER_H
