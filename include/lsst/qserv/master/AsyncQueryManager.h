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
    
    explicit AsyncQueryManager() 
        :_lastId(1000000000), _isExecFaulty(false), _queryCount(0) {}
    void configureMerger(TableMergerConfig const& c);

    int add(TransactionSpec const& t, std::string const& resultName);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;
    void joinEverything();
    ResultDeque const& getFinalState() { return _results; }
    void finalizeQuery(int id,  XrdTransResult r, bool aborted); 
    std::string getMergeResultName() const;
    
private:
    typedef std::pair<boost::shared_ptr<ChunkQuery>, std::string> QuerySpec;
    typedef std::map<int, QuerySpec> QueryMap;

    // Functors for applying to queries
    class printQueryMapValue; // defined in AsyncQueryManager.cc
    class squashQuery; // defined in AsyncQueryManager.cc

    int _getNextId() {
	boost::lock_guard<boost::mutex> m(_idMutex); 
	return ++_lastId;
    }
    void _printState(std::ostream& os);
    void _squashExecution();

    boost::mutex _idMutex;
    boost::mutex _queriesMutex;
    boost::mutex _resultsMutex;
    boost::condition_variable _queriesEmpty;
    int _lastId;
    bool _isExecFaulty;
    int _squashCount;
    QueryMap _queries;
    ResultDeque _results;
    int _queryCount;
    boost::shared_ptr<TableMerger> _merger;
};

}}} // lsst::qserv::master namespace

#endif // LSST_QSERV_MASTER_ASYNCQUERYMANAGER_H
