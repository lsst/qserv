// -*- LSST-C++ -*-

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
 
#ifndef LSST_QSERV_MASTER_ASYNCQUERYMANAGER_H
#define LSST_QSERV_MASTER_ASYNCQUERYMANAGER_H

// Standard
#include <deque>
#include <map>

// Boost
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

namespace lsst {
namespace qserv {
namespace master {

// Forward
class ChunkQuery;
class TableMerger;
class TableMergerConfig;
class XrdTransResult;
class TransactionSpec;

//////////////////////////////////////////////////////////////////////
// class AsyncQueryManager 
// Babysits a related set of queries.  Issues asynchronously handles 
// preparation, status-checking, and post-processing (if a merger has 
// been configured).
// 
//////////////////////////////////////////////////////////////////////
class AsyncQueryManager {
public:
    typedef std::pair<int, XrdTransResult> Result;
    typedef std::deque<Result> ResultDeque;
    typedef boost::shared_ptr<AsyncQueryManager> Ptr;
    typedef std::map<std::string, std::string> StringMap;
    
    explicit AsyncQueryManager(std::map<std::string,std::string> const& cfg) 
        :_lastId(1000000000), 
        _isExecFaulty(false), _isSquashed(false),
        _queryCount(0),
        _shouldLimitResult(false), 
        _resultLimit(1024*1024*1024), _totalSize(0),
        _canRead(true), _reliefFiles(0)
            { _readConfig(cfg); }
    void configureMerger(TableMergerConfig const& c);

    int add(TransactionSpec const& t, std::string const& resultName);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;
    void joinEverything();
    ResultDeque const& getFinalState() { return _results; }
    void finalizeQuery(int id,  XrdTransResult r, bool aborted); 
    std::string getMergeResultName() const;
    std::string const& getXrootdHostPort() const { return _xrootdHostPort; };

    void getReadPermission();
    void getWritePermission();
    void signalTooManyFiles();
    void pauseReadTrans();
    void resumeReadTrans();

private:
    // QuerySpec: ChunkQuery object + result name
    typedef std::pair<boost::shared_ptr<ChunkQuery>, std::string> QuerySpec;
    typedef std::map<int, QuerySpec> QueryMap;

    // Functors for applying to queries
    class printQueryMapValue; // defined in AsyncQueryManager.cc
    class squashQuery; // defined in AsyncQueryManager.cc

    int _getNextId() {
	boost::lock_guard<boost::mutex> m(_idMutex); 
	return ++_lastId;
    }
    void _readConfig(std::map<std::string,std::string> const& cfg);
    void _printState(std::ostream& os);
    void _addNewResult(ssize_t dumpSize, std::string const& dumpFile, 
                       std::string const& tableName);
    void _squashExecution();
    void _squashRemaining();

    boost::mutex _idMutex;
    boost::mutex _queriesMutex;
    boost::mutex _resultsMutex;
    boost::mutex _totalSizeMutex;
    boost::condition_variable _queriesEmpty;
    boost::mutex _canReadMutex;
    boost::condition_variable _canReadCondition;

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
    bool _canRead;
    int _reliefFiles;

    std::string _xrootdHostPort;
    boost::shared_ptr<TableMerger> _merger;
};

}}} // lsst::qserv::master namespace

#endif // LSST_QSERV_MASTER_ASYNCQUERYMANAGER_H
