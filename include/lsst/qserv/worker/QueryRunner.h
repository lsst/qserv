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

// Forward
class QueryRunner;

////////////////////////////////////////////////////////////////////////
class QueryRunnerArg {
public:
    QueryRunnerArg(XrdSysError* e_, 
		   std::string const& user_, ScriptMeta const& s_,
		   std::string overrideDump_=std::string()) 
	: e(e_), user(user_), s(s_), overrideDump(overrideDump_) { }

    XrdSysError* e; // must be assignable
    std::string user;
    ScriptMeta s;
    std::string overrideDump;
};

class ArgFunc {
public:
    virtual ~ArgFunc() {}
    virtual void operator()(QueryRunnerArg const& )=0;
};

////////////////////////////////////////////////////////////////////////
class QueryRunnerManager {
public:
    QueryRunnerManager() : _limit(8), _jobTotal(0) {}
    ~QueryRunnerManager() {}

    // const
    bool hasSpace() const { return _runners.size() < _limit; }
    bool isOverloaded() const { return _runners.size() > _limit; }
    int getQueueLength() const { return _args.size();}
    int getRunnerCount() const { return _runners.size();}

    // non-const
    void runOrEnqueue(QueryRunnerArg const& a);
    void setSpaceLimit(int limit) { _limit = limit; }
    bool squashByHash(std::string const& hash);
    void addRunner(QueryRunner* q); 
    void dropRunner(QueryRunner* q);
    bool recycleRunner(ArgFunc* r);

    // Mutex
    boost::mutex& getMutex() { return _mutex; }

private:
    typedef std::deque<QueryRunnerArg> ArgQueue;
    typedef std::deque<QueryRunner*> QueryQueue;

    QueryRunnerArg const& _getQueueHead() const;
    void _popQueueHead();
    bool _cancelQueued(std::string const& hash);
    bool _cancelRunning(std::string const& hash);
    void _enqueue(QueryRunnerArg const& a);

    ArgQueue _args;
    QueryQueue _runners;
    int _jobTotal;
    
    int _limit;
    boost::mutex _mutex;    
};

////////////////////////////////////////////////////////////////////////
class QueryRunner {
public:
    typedef ResultTracker<std::string, ResultError> Tracker;
    typedef QueryRunnerManager Manager;
    QueryRunner(XrdSysError& e, 
		std::string const& user, ScriptMeta const& s,
		std::string overrideDump=std::string());
    explicit QueryRunner(QueryRunnerArg const& a);
    ~QueryRunner();
    bool operator()();
    std::string const& getHash() const { return _meta.hash; }
    void poison(std::string const& hash);

    // Static: 
    static Tracker& getTracker() { static Tracker t; return t;}
    static Manager& getMgr() { static Manager m; return m;}

private:
    typedef std::deque<std::string> StringDeque;
    bool _act();
    void _appendError(int errorNo, std::string const& desc);
    bool _connectDbServer(MYSQL* db);
    bool _dropDb(MYSQL* db, std::string const& name);
    bool _dropTables(MYSQL* db, std::string const& tables);
    std::string _getDumpTableList(std::string const& script);
    void _mkdirP(std::string const& filePath);
    bool _runScript(std::string const& script, std::string const& dbName);
    bool _runScriptCore(MYSQL* db, std::string const& script,
                        std::string const& dbName,
                        std::string const& tableList);

    void _buildSubchunkScripts(std::string const& script,
			       std::string& build, std::string& cleanup);
    bool _prepareAndSelectResultDb(MYSQL* db, 
				   std::string const& dbName);
    bool _prepareScratchDb(MYSQL* db);
    bool _performMysqldump(std::string const& dbName, 
			   std::string const& dumpFile,
			   std::string const& tables);
    bool _isExecutable(std::string const& execName);
    void _setNewQuery(QueryRunnerArg const& a);
    std::string _getErrorString() const;
    boost::shared_ptr<ArgFunc> getResetFunc();
    bool _checkPoisoned();
    boost::shared_ptr<CheckFlag> _makeAbort();
    bool _poisonCleanup();

    XrdSysError& _e;
    std::string _user;
    ScriptMeta _meta;
    std::string _scriptId;
    int _errorNo;
    std::string _errorDesc;
    boost::mutex _poisonedMutex;
    StringDeque _poisoned;
};

 int dumpFileOpen(std::string const& dbName);
 bool dumpFileExists(std::string const& dumpFilename);


}}}
#endif // LSST_QSERV_WORKER_QUERYRUNNER_H
