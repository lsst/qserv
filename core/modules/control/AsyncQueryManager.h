// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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

#ifndef LSST_QSERV_CONTROL_ASYNCQUERYMANAGER_H
#define LSST_QSERV_CONTROL_ASYNCQUERYMANAGER_H
/**
  * @file AsyncQueryManager.h
  *
  * @brief AsyncQueryManager is the class that orchestrates the C++ layer
  * execution of a query. While most of its work is delegated, it is the one
  * that maintains thread pools and dispatch/join of chunk queries.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>
#include <map>

// Third-party headers
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

// Local headers
#include "control/DynamicWorkQueue.h"
#include "xrdc/xrdfile.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace control {
    class TransactionSpec;
}
namespace merger {
    class MergeFixup;
    class TableMerger;
    class TableMergerConfig;
}
namespace qdisp {
    class ChunkQuery;
    class MessageStore;
}
namespace qproc {
     class QuerySession;
}
namespace xrdc {
    class PacketIter;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace control {

//////////////////////////////////////////////////////////////////////
// class AsyncQueryManager
// Babysits a related set of queries.  Issues asynchronously handles
// preparation, status-checking, and post-processing (if a merger has
// been configured).
//
//////////////////////////////////////////////////////////////////////
class AsyncQueryManager {
public:
    typedef std::pair<int, xrdc::XrdTransResult> Result;
    typedef std::deque<Result> ResultDeque;
    typedef std::deque<Result>::const_iterator ResultDequeCItr;
    typedef boost::shared_ptr<AsyncQueryManager> Ptr;
    typedef std::map<std::string, std::string> StringMap;
    typedef boost::shared_ptr<xrdc::PacketIter> PacIterPtr;

    explicit AsyncQueryManager(std::map<std::string,std::string> const& cfg)
        :_lastId(1000000000),
        _isExecFaulty(false), _isSquashed(false),
        _queryCount(0),
        _shouldLimitResult(false),
        _resultLimit(1024*1024*1024), _totalSize(0)
    {
        _readConfig(cfg);
    }

    ~AsyncQueryManager() { }

    void configureMerger(merger::TableMergerConfig const& c);
    void configureMerger(merger::MergeFixup const& m,
                         std::string const& resultTable);

    boost::shared_ptr<qdisp::MessageStore> getMessageStore();

    int add(TransactionSpec const& t, std::string const& resultName);
    void join(int id);
    bool tryJoin(int id);
    void joinEverything();
    ResultDeque const& getFinalState() { return _results; }
    void finalizeQuery(int id,  xrdc::XrdTransResult r, bool aborted);
    std::string getMergeResultName() const;
    std::string const& getXrootdHostPort() const { return _xrootdHostPort; };
    std::string const& getScratchPath() const { return _scratchPath; };

    void addToReadQueue(DynamicWorkQueue::Callable * callable);
    void addToWriteQueue(DynamicWorkQueue::Callable * callable);

    qproc::QuerySession& getQuerySession() { return *_qSession; }
        
private:
    // QuerySpec: ChunkQuery object + result name
    typedef std::pair<boost::shared_ptr<qdisp::ChunkQuery>, std::string> QuerySpec;
    typedef std::map<int, QuerySpec> QueryMap;

    // Functors for applying to queries
    class printQueryMapValue; // defined in AsyncQueryManager.cc
    class squashQuery; // defined in AsyncQueryManager.cc

    int _getNextId() {
	boost::lock_guard<boost::mutex> m(_idMutex);
	return ++_lastId;
    }
    void _readConfig(std::map<std::string,std::string> const& cfg);
    void _initFacade(std::string const& cssTech, std::string const& cssConn);
    void _printState(std::ostream& os);
    void _addNewResult(int id, ssize_t dumpSize, std::string const& dumpFile,
                       std::string const& tableName);
    void _addNewResult(int id, PacIterPtr pacIter, std::string const& tableName);
    void _squashExecution();
    void _squashRemaining();

    boost::mutex _idMutex;
    boost::mutex _queriesMutex;
    boost::mutex _resultsMutex;
    boost::mutex _totalSizeMutex;
    boost::condition_variable _queriesEmpty;

    int _lastId;
    bool _isExecFaulty;
    bool _isSquashed;
    int _squashCount;
    QueryMap _queries;
    ResultDeque _results;
    int _queryCount;
    bool _shouldLimitResult;
    ssize_t _resultLimit;
    ssize_t _totalSize;

    // For merger configuration
    std::string _resultDbSocket;
    std::string _resultDbUser;
    std::string _resultDbDb;

    std::string _xrootdHostPort;
    std::string _scratchPath;
    boost::shared_ptr<qdisp::MessageStore> _messageStore;
    boost::shared_ptr<merger::TableMerger> _merger;
    boost::shared_ptr<qproc::QuerySession> _qSession;
};
}}} // namespace lsst::qserv::control

#endif // LSST_QSERV_CONTROL_ASYNCQUERYMANAGER_H
