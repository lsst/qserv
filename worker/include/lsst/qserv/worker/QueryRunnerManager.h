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
#ifndef LSST_QSERV_WORKER_QUERYRUNNERMANAGER_H
#define LSST_QSERV_WORKER_QUERYRUNNERMANAGER_H
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/Task.h"

namespace lsst {
namespace qserv {
namespace worker {

// Forward
class Logger;
class QueryRunner;

////////////////////////////////////////////////////////////////////////
class QueryRunnerArg {
public:
QueryRunnerArg(boost::shared_ptr<Logger> log_,
               Task::Ptr task_,
               std::string overrideDump_=std::string()) 
    : log(log_), task(task_), overrideDump(overrideDump_) { }
    boost::shared_ptr<Logger> log;
    Task::Ptr task;
    std::string overrideDump;
};

class ArgFunc {
public:
    virtual ~ArgFunc() {}
    virtual void operator()(QueryRunnerArg const& )=0;
};

////////////////////////////////////////////////////////////////////////
// QueryRunnerManager manages a set of QueryRunner threads, which
// execute queued query tasks for the fulfillment of incoming
// ChunkQueries. 
////////////////////////////////////////////////////////////////////////
class QueryRunnerManager {
public:
    QueryRunnerManager() : _limit(8), _jobTotal(0) { _init(); }
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
    bool recycleRunner(ArgFunc* r, int lastChunkId);

    // Mutex
    boost::mutex& getMutex() { return _mutex; }

private:
    typedef std::deque<QueryRunnerArg> ArgQueue;
    typedef std::deque<QueryRunner*> QueryQueue;
    class argMatch;

    void _init();
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
}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_QUERYRUNNERMANAGER_H
