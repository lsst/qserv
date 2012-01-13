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

#include "mysql/mysql.h"

// package
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/ResultTracker.h"
#include "lsst/qserv/worker/QueryRunnerManager.h"

namespace lsst {
namespace qserv {
namespace worker {

////////////////////////////////////////////////////////////////////////
class QueryRunner {
public:
    typedef ResultTracker<std::string, ResultError> Tracker;
    typedef QueryRunnerManager Manager;
    QueryRunner(boost::shared_ptr<Logger> log, 
                std::string const& user, Task::Ptr task,
                std::string overrideDump=std::string());
    explicit QueryRunner(QueryRunnerArg const& a);
    ~QueryRunner();
    bool operator()();
    std::string const& getHash() const { return _task->hash; }
    void poison(std::string const& hash);

    // Static: 
    static Tracker& getTracker() { static Tracker t; return t;}
    static Manager& getMgr() { static Manager m; return m;}

private:
    typedef std::deque<std::string> StringDeque;
    typedef std::vector<int> IntVector;
    typedef IntVector::iterator IntVectorIter;
    typedef boost::shared_ptr<IntVector> IntVectorPtr;

    bool _act();
    void _appendError(int errorNo, std::string const& desc);
    bool _connectDbServer(MYSQL* db);
    bool _dropDb(MYSQL* db, std::string const& name);
    bool _dropTables(MYSQL* db, std::string const& tables);
    std::string _getDumpTableList(std::string const& script);
    void _mkdirP(std::string const& filePath);
    bool _runScript(std::string const& script, std::string const& dbName);
    bool _runTask(Task::Ptr t);
    bool _runFragment(std::string const& fscr,
                      IntVector const& sc,
                      std::string const& dbName);

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

    boost::shared_ptr<Logger> _log;
    std::string _user;
    Task::Ptr _task;
    std::string _scriptId;
    int _errorNo;
    std::string _errorDesc;
    boost::shared_ptr<boost::mutex> _poisonedMutex;
    StringDeque _poisoned;
};

 int dumpFileOpen(std::string const& dbName);
 bool dumpFileExists(std::string const& dumpFilename);

}}}
#endif // LSST_QSERV_WORKER_QUERYRUNNER_H
