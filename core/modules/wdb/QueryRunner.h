// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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

#ifndef LSST_QSERV_WDB_QUERYRUNNER_H
#define LSST_QSERV_WDB_QUERYRUNNER_H
 /**
  * @file
  *
  * @brief QueryRunner instances perform actual query execution on SQL
  * databases using SqlConnection objects to interact with dbms
  * instances.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>

// Third-party headers
#include "boost/thread.hpp" // for mutex support

// Local headers
#include "mysql/mysql.h"
#include "sql/SqlErrorObject.h"
#include "wbase/Base.h"
#include "wbase/Task.h"
#include "wcontrol/ResultTracker.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace sql {
    class SqlConnection;
}
namespace wdb {
    class QuerySql;
    class QueryPhyResult;
}
namespace wlog {
    class WLogger;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wdb {

////////////////////////////////////////////////////////////////////////
struct QueryRunnerArg {
public:
    QueryRunnerArg() {}

    QueryRunnerArg(boost::shared_ptr<wlog::WLogger> log_,
                   wbase::Task::Ptr task_,
                   std::string overrideDump_=std::string())
        : log(log_), task(task_), overrideDump(overrideDump_) { }
    boost::shared_ptr<wlog::WLogger> log;
    wbase::Task::Ptr task;
    std::string overrideDump;
};
class ArgFunc {
public:
    virtual ~ArgFunc() {}
    virtual void operator()(QueryRunnerArg const& )=0;
};
////////////////////////////////////////////////////////////////////////
class QueryRunner {
// The interface of QueryRunner is too cluttered with implementation detail,
// whiich means that all users of it are exposed to its ugly details. We will
// write a new one to leverage the new xrootd interface and new result
// transfer. ResultTracker may be eliminated.
public:
    typedef wcontrol::ResultTracker<std::string, wcontrol::ResultError> Tracker;
    QueryRunner(QueryRunnerArg const& a);
    ~QueryRunner();
    bool operator()(); // exec and loop as long as there are queries
                       // to run.
    bool actOnce();

    std::string const& getHash() const { return _task->hash; }
    void poison(std::string const& hash);

    // Static:
    static Tracker& getTracker() { static Tracker t; return t;}

private:
    typedef std::deque<std::string> StringDeque;

    typedef std::vector<int> IntVector;
    typedef IntVector::iterator IntVectorIter;
    typedef boost::shared_ptr<IntVector> IntVectorPtr;

    bool _act();
    std::string _getDumpTableList(std::string const& script);
    bool _runTask(wbase::Task::Ptr t);
    bool _runFragment(sql::SqlConnection& sqlConn,
                      wdb::QuerySql const& qSql);
    void _buildSubchunkScripts(std::string const& script,
                               std::string& build, std::string& cleanup);
    bool _prepareAndSelectResultDb(sql::SqlConnection& sqlConn,
                                   std::string const& dbName=std::string());
    bool _prepareScratchDb(sql::SqlConnection& sqlConn);
    bool _performMysqldump(std::string const& dbName,
                           std::string const& dumpFile,
                           std::string const& tables);
    bool _isExecutable(std::string const& execName);
    void _setNewQuery(QueryRunnerArg const& a);
    std::string _getErrorString() const;
    boost::shared_ptr<ArgFunc> getResetFunc();
    bool _checkPoisoned();
    boost::shared_ptr<wbase::CheckFlag> _makeAbort();
    bool _poisonCleanup();

    // Fields
    boost::shared_ptr<wlog::WLogger> _log;
    sql::SqlErrorObject _errObj;
    std::string _user;
    boost::shared_ptr<wdb::QueryPhyResult> _pResult;
    wbase::Task::Ptr _task;
    std::string _scriptId;
    boost::shared_ptr<boost::mutex> _poisonedMutex;
    StringDeque _poisoned;
};

int dumpFileOpen(std::string const& dbName);
bool dumpFileExists(std::string const& dumpFilename);

}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_QUERYRUNNER_H
