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

/**
  * @file
  *
  * @brief AsyncQueryManager: Manages/dispatches individual chunk
  * queries, waits for their completions, collects results, and
  * invokes result merging. Initiates query squashing when faults are
  * detected.  "Async" refers to the use of asynchronous xrootd client
  * API, which required some state management and liberal use of
  * callbacks.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "ccontrol/AsyncQueryManager.h"

// System headers
#include <cassert>
#include <iostream>
#include <sstream>

// Third-party headers
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "boost/format.hpp"
#include "boost/make_shared.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "ccontrol/ConfigMap.h"
#include "css/Facade.h"
#include "global/stringTypes.h"
#include "log/msgCode.h"
#include "rproc/TableMerger.h"
#include "qdisp/ChunkQuery.h"
#include "qdisp/MessageStore.h"
#include "qdisp/TransactionSpec.h"
#include "qproc/QuerySession.h"
#include "util/PacketBuffer.h"
#include "util/Timer.h"
#include "util/WorkQueue.h"
#include "xrdc/XrdBufferSource.h"

// Namespace modifiers
using boost::make_shared;

namespace lsst {
namespace qserv {
namespace ccontrol {


// Local Helpers --------------------------------------------------
namespace {

// TODO(smm): These should be created elsewhere, and the thread counts
//            should come from a configuration file.
static DynamicWorkQueue globalReadQueue(25, 5, 50, 0);
static DynamicWorkQueue globalWriteQueue(50, 2, 60, 0);

// Doctors the query path to specify the async path.
// Modifies the string in-place.
void doctorQueryPath(std::string& path) {
    std::string::size_type pos;
    std::string before("/query/");
    std::string after("/query2/");

    pos = path.find(before);
    if(pos != std::string::npos) {
        path.replace(pos, before.size(), after);
    } // Otherwise, don't doctor.
}

}

////////////////////////////////////////////////////////////
// AsyncQueryManager nested classes
////////////////////////////////////////////////////////////

class AsyncQueryManager::printQueryMapValue {
public:
    printQueryMapValue(std::ostream& os_) : os(os_) {}
    void operator()(QueryMap::value_type const& qv) {
        os << "Query with id=" << qv.first;
        os << ": ";
        if(qv.second.first) {
            os << qv.second.first->getDesc();
        } else {
            os << "(NULL)";
        }
        os << ", " << qv.second.second << std::endl;
    }
    std::ostream& os;
};

class AsyncQueryManager::squashQuery {
public:
    squashQuery(boost::mutex& mutex_, QueryMap& queries_)
        :mutex(mutex_), queries(queries_) {}
    void operator()(QueryMap::value_type const& qv) {
        boost::shared_ptr<qdisp::ChunkQuery> cq = qv.second.first;
        if(!cq.get()) return;
        {
            boost::unique_lock<boost::mutex> lock(mutex);
            QueryMap::iterator i = queries.find(qv.first);
            if(i != queries.end()) {
                cq = i->second.first; // Get the shared version.
                if(!cq.get()) {
                    //qv.second.first.reset(); // Erase ours too.
                    return;
                }
                //idInProgress = i->first;
            }
        }
        // Query may have been completed, and its memory freed,
        // but still exist briefly before it is deleted from the map.
        util::Timer t;
        t.start();
        cq->requestSquash();
        t.stop();
        LOGF_INFO("qSquash %1%" % t);
    }
    boost::mutex& mutex;
    QueryMap& queries;
};

////////////////////////////////////////////////////////////
// AsyncQueryManager
////////////////////////////////////////////////////////////
int AsyncQueryManager::add(qdisp::TransactionSpec const& t,
                           std::string const& resultName) {
    LOGF_DEBUG("EXECUTING AsyncQueryManager::add(TransactionSpec, %1%)"
               % resultName);
    int id = t.chunkId;
    // Use chunkId as id, and assume that it will be unique for the
    // AsyncQueryManager instance.
    if(id == -1) {
        id = _getNextId();
    }
    if(t.isNull() || _isExecFaulty) {
        // If empty spec or fault already detected, refuse to run.
        return -1;
    }
    qdisp::TransactionSpec ts(t);

    doctorQueryPath(ts.path);
    QuerySpec qs(boost::make_shared<qdisp::ChunkQuery>(ts, id, this),
                 resultName);
    {
        boost::lock_guard<boost::mutex> lock(_queriesMutex);
        _queries[id] = qs;
        ++_queryCount;
    }
    std::string msg = std::string("Query Added: url=") + ts.path + ", savePath=" + ts.savePath;
    getMessageStore()->addMessage(id, log::MSG_MGR_ADD, msg);
    LOGF_INFO("Added query id=%1% url=%2% with save %3%"
              % id % ts.path % ts.savePath);
    qs.first->run();
    return id;
}

void AsyncQueryManager::finalizeQuery(int id,
                                      xrdc::XrdTransResult r,
                                      bool aborted) {
    std::stringstream ss;
    util::Timer t1;
    t1.start();
    /// Finalize a query.
    /// Note that all parameters should be copies and not const references.
    /// We delete the ChunkQuery (the caller) here, so a ref would be invalid.
    std::string dumpFile;
    std::string tableName;
    int dumpSize;
    LOGF_DEBUG("finalizing. read=%1% and status is %2%"
               % r.read % (aborted ? "ABORTED" : "okay"));
    LOGF_DEBUG("%1% Finalizing query (%2%)" % ((void*)this) % id);
    if((!aborted) && (r.open >= 0) && (r.queryWrite >= 0)
       && (r.read >= 0)) {
        util::Timer t2;
        t2.start();
        boost::shared_ptr<util::PacketBuffer> pacBuffer;
        { // Lock scope for reading
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            QuerySpec& s = _queries[id];
            pacBuffer.reset(new util::PacketBuffer(s.first->getResultBuffer().release()));
            dumpFile = s.first->getSavePath();
            dumpSize = s.first->getSaveSize();
            tableName = s.second;
            //assert(r.localWrite == dumpSize); // not valid when using iter
            s.first.reset(); // clear out ChunkQuery.
        }
        // Lock-free merge

        _addNewResult(id, pacBuffer, tableName);
        // Erase right before notifying.
        t2.stop();
        ss << id << " QmFinalizeMerge " << t2 << std::endl;
        getMessageStore()->addMessage(id, log::MSG_MERGED, "Results Merged.");
    } // end if
    else {
        util::Timer t2e;
        t2e.start();
        if(!aborted) {
            _isExecFaulty = true;
            LOGF_INFO("Requesting squash %1% because open=%2% queryWrite=%3% read=%4%"
                      % id % r.open % r.queryWrite % r.read);
            _squashExecution();
            LOGF_INFO("Skipped merge (read failed for id=%1%)" % id);
        }
        t2e.stop();
        ss << id << " QmFinalizeError " << t2e << std::endl;
    }
    util::Timer t3;
    t3.start();
    {
        boost::lock_guard<boost::mutex> lock(_resultsMutex);
        _results.push_back(Result(id,r));
        if(aborted) ++_squashCount; // Borrow result mutex to protect counter.
        { // Lock again to erase.
            util::Timer t2e1;
            t2e1.start();
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            _queries.erase(id);
            if(_queries.empty()) _queriesEmpty.notify_all();
            t2e1.stop();
            ss << id << " QmFinalizeErase " << t2e1 << std::endl;
            getMessageStore()->addMessage(id, log::MSG_ERASED,
                                          "Query Resources Erased.");
        }
    }
    t3.stop();
    ss << id << " QmFinalizeResult " << t3 << std::endl;
    LOGF_DEBUG("%1% Done finalizing query (%2%)" % (void*)this % id);
    t1.stop();
    ss << id << " QmFinalize " << t1 << std::endl;
    LOGF_INFO("%1%" % ss.str());
    getMessageStore()->addMessage(id, log::MSG_FINALIZED, "Query Finalized.");
}

// FIXME: With squashing, we should be able to return the result earlier.
// So, clients will call joinResult(), to get the result, and let a reaper
// thread call joinEverything, since that ensures that this object has
// ceased activity and can recycle resources.
// This is a performance optimization.
void AsyncQueryManager::joinEverything() {
    boost::unique_lock<boost::mutex> lock(_queriesMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    if (LOG_CHECK_DEBUG()) {
        std::stringstream ss;
        _printState(ss);
        LOGF_INFO("%1%" % ss.str());
    }
    while(!_queries.empty()) {
        count = _queries.size();
        if(count != lastCount) {
            LOGF_INFO("Still %1% in flight." % count);
            count = lastCount;
            ++complainCount;
            if(complainCount > moreDetailThreshold) {
                if (LOG_CHECK_WARN()) {
                    std::stringstream ss;
                    _printState(ss);
                    LOGF_WARN("%1%" % ss.str());
                }
                complainCount = 0;
            }
        }
        _queriesEmpty.timed_wait(lock, boost::posix_time::seconds(5));
    }
    _merger->finalize();
    _merger.reset();
    LOGF_INFO("Query finish. %1% dispatched." % _queryCount);
}

void AsyncQueryManager::configureMerger(rproc::TableMergerConfig const& c) {
    _merger = boost::make_shared<rproc::TableMerger>(c);
}

void AsyncQueryManager::configureMerger(rproc::MergeFixup const& m,
                                        std::string const& resultTable) {
    // Can we configure the merger without involving settings
    // from the python layer? Historically, the Python layer was
    // needed to generate the merging SQL statements, but we are now
    // creating them without Python.
    std::string mysqlBin="obsolete";
    std::string dropMem;
    rproc::TableMergerConfig cfg(_resultDbDb,     // cfg result db
                                  resultTable,     // cfg resultname
                                  m,               // merge fixup obj
                                  _resultDbUser,   // result db credentials
                                  _resultDbSocket, // result db credentials
                                  mysqlBin,        // Obsolete
                                  dropMem          // cfg
                                  );
    _merger = boost::make_shared<rproc::TableMerger>(cfg);
}

std::string AsyncQueryManager::getMergeResultName() const {
    if(_merger.get()) {
        return _merger->getTargetTable();
    }
    return std::string();
}

void AsyncQueryManager::addToReadQueue(DynamicWorkQueue::Callable * callable) {
    globalReadQueue.add(this, callable);
}

void AsyncQueryManager::addToWriteQueue(DynamicWorkQueue::Callable * callable) {
    globalWriteQueue.add(this, callable);
}

boost::shared_ptr<qdisp::MessageStore>
AsyncQueryManager::getMessageStore() {
    if (!_messageStore) {
        // Lazy instantiation of MessageStore.
        _messageStore = boost::make_shared<qdisp::MessageStore>();
    }
    return _messageStore;
}

////////////////////////////////////////////////////////////////////////
// private: ////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////


void AsyncQueryManager::_readConfig(std::map<std::string,
                                    std::string> const& cfg) {
    ConfigMap cm(cfg);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    _xrootdHostPort = cm.get(
        "frontend.xrootd",
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    _scratchPath =  cm.get(
        "frontend.scratch_path",
        "Error, no scratch path found. Using /tmp.",
        "/tmp");
    // This should be overriden by the installer properly.
    _resultDbSocket =  cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    _resultDbUser =  cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    _resultDbDb =  cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");

    std::string cssTech = cm.get(
        "css.technology",
        "Error, css.technology not found.",
        "invalid");
    std::string cssConn = cm.get(
        "css.connection",
        "Error, css.connection not found.",
        "");
    int cssTimeout = atoi(cm.get(
        "css.timeout",
        "Error, css.timeout not found.",
        "10000").c_str());
    _initFacade(cssTech, cssConn, cssTimeout);

    std::string defaultDb = cm.get(
        "table.defaultdb",
        "Empty table.defaultdb. Using LSST",
        "LSST");
    _qSession->setDefaultDb(defaultDb);
}

void AsyncQueryManager::_initFacade(std::string const& cssTech,
                                    std::string const& cssConn,
                                    int timeout_msec) {
    if (cssTech == "zoo") {
        LOGF_INFO("Initializing zookeeper-based css, %1% %2%msec" % 
                  cssConn % timeout_msec);
        boost::shared_ptr<css::Facade> cssFPtr(
            css::FacadeFactory::createZooFacade(cssConn, timeout_msec));
        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else if (cssTech == "mem") {
        LOGF_INFO("Initializing memory-based css, with %1%" % cssConn);
        boost::shared_ptr<css::Facade> cssFPtr(
            css::FacadeFactory::createMemFacade(cssConn));
        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else {
        LOGF_ERROR("Unable to determine css technology, check config file.");
        throw ConfigError("Invalid css technology, check config file.");
    }
}

void AsyncQueryManager::_addNewResult(int id,
                                      boost::shared_ptr<util::PacketBuffer> pb,
                                      std::string const& tableName) {
    LOGF_DEBUG("EXECUTING AsyncQueryManager::_addNewResult(%1%, pacIter, %2%)"
               % id % tableName);
    bool mergeResult = _merger->merge(pb, tableName);
    ssize_t sz = pb->getTotalSize();
    {
        boost::lock_guard<boost::mutex> lock(_totalSizeMutex);
        _totalSize += sz;
    }

    if(_shouldLimitResult && (_totalSize > _resultLimit)) {
        _squashRemaining();
    }
    if(!mergeResult) {
        rproc::TableMergerError e = _merger->getError();
        getMessageStore()->addMessage(id, e.errorCode != 0 ? -abs(e.errorCode) : -1,
                                      "Failed to merge results.");
        if(e.resultTooBig()) {
            _squashRemaining();
        }
    }
}

void AsyncQueryManager::_addNewResult(int id, ssize_t dumpSize,
                                      std::string const& dumpFile,
                                      std::string const& tableName) {
    if(dumpSize < 0) {
        throw std::invalid_argument("dumpSize < 0");
    }
    {
        boost::lock_guard<boost::mutex> lock(_totalSizeMutex);
        _totalSize += dumpSize;
    }

    if(_shouldLimitResult && (_totalSize > _resultLimit)) {
        _squashRemaining();
    }

    if(dumpSize > 0) {
        bool mergeResult = _merger->merge(dumpFile, tableName);
        int res = unlink(dumpFile.c_str()); // Hurry and delete dump file.
        if(0 != res) {
            LOGF_ERROR("Error removing dumpFile %1% errno=%2%"
                       % dumpFile % errno);
        }
        if(!mergeResult) {
            rproc::TableMergerError e = _merger->getError();
            getMessageStore()->addMessage(id, e.errorCode != 0 ? -abs(e.errorCode) : -1,
                                          "Failed to merge results.");
            if(e.resultTooBig()) {
                _squashRemaining();
            }
        }
        LOGF_DEBUG("Merge of %1% into %2% %3%" % dumpFile % tableName
                   % (mergeResult ? " OK----" : " FAIL===="));
    }
}

void AsyncQueryManager::_printState(std::ostream& os) {
    typedef std::map<int, boost::shared_ptr<qdisp::ChunkQuery> > QueryMap;
    std::for_each(_queries.begin(), _queries.end(), printQueryMapValue(os));
}

void AsyncQueryManager::_squashExecution() {
    // Halt new query dispatches and cancel the ones in flight.
    // This attempts to save on resources and latency, once a query
    // fault is detected.

    if(_isSquashed) return;
    _isSquashed = true; // Mark before acquiring lock--faster.
    LOGF_DEBUG("Squash requested by %1%" % (void*)this);
    util::Timer t;
    // Squashing is dependent on network latency and remote worker
    // responsiveness, so make a copy so others don't have to wait.
    std::vector<std::pair<int, QuerySpec> > myQueries;
    {
        boost::unique_lock<boost::mutex> lock(_queriesMutex);
        t.start();
        myQueries.resize(_queries.size());
        LOGF_INFO("AsyncQM squashExec copy");
        std::copy(_queries.begin(), _queries.end(), myQueries.begin());
    }
    LOGF_INFO("AsyncQM squashQueued");
    globalWriteQueue.cancelQueued(this);
    LOGF_INFO("AsyncQM squashExec iteration");
    std::for_each(myQueries.begin(), myQueries.end(),
                  squashQuery(_queriesMutex, _queries));
    t.stop();
    LOGF_INFO("AsyncQM squashExec %1%" % t);
    _isSquashed = true; // Ensure that flag wasn't trampled.

    getMessageStore()->addMessage(-1, log::MSG_EXEC_SQUASHED,
                                  "Query Execution Squashed.");
}

void AsyncQueryManager::_squashRemaining() {
    _squashExecution();  // Not sure if this is right. FIXME
}

}}} // namespace lsst::qserv::ccontrol
