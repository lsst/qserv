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
 
#include <iostream>

#include <boost/make_shared.hpp>
#include "boost/date_time/posix_time/posix_time_types.hpp" 

#include "lsst/qserv/master/AsyncQueryManager.h"
#include "lsst/qserv/master/ChunkQuery.h"
#include "lsst/qserv/master/TableMerger.h"
#include "lsst/qserv/master/Timer.h"
#include "lsst/qserv/common/WorkQueue.h"
#include "lsst/qserv/master/PacketIter.h"

// Namespace modifiers
using boost::make_shared;
namespace qMaster = lsst::qserv::master;


// Local Helpers --------------------------------------------------
namespace { 

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

class qMaster::AsyncQueryManager::printQueryMapValue {
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

class qMaster::AsyncQueryManager::squashQuery {
public:
    squashQuery(boost::mutex& mutex_, QueryMap& queries_) 
        :mutex(mutex_), queries(queries_) {}
    void operator()(QueryMap::value_type const& qv) {
        boost::shared_ptr<ChunkQuery> cq = qv.second.first;
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
        Timer t;
        t.start();
        cq->requestSquash();
        t.stop();
        std::cout << "qSquash " << t << std::endl;
    }
    boost::mutex& mutex;
    QueryMap& queries;
};

////////////////////////////////////////////////////////////
// AsyncQueryManager
////////////////////////////////////////////////////////////
int qMaster::AsyncQueryManager::add(TransactionSpec const& t, 
                                    std::string const& resultName) {
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
    TransactionSpec ts(t);

    doctorQueryPath(ts.path);
    QuerySpec qs(boost::make_shared<ChunkQuery>(ts, id, this),
                 resultName);
    {
        boost::lock_guard<boost::mutex> lock(_queriesMutex);
        _queries[id] = qs;
        ++_queryCount;
    }
    std::cout << "Added query id=" << id << " url=" << ts.path 
              << " with save " << ts.savePath << "\n";
    qs.first->run();
    return id;
}

void qMaster::AsyncQueryManager::finalizeQuery(int id, 
                                               XrdTransResult r,
                                               bool aborted) {
    std::stringstream ss;
    Timer t1;
    t1.start();
    /// Finalize a query.
    /// Note that all parameters should be copies and not const references.
    /// We delete the ChunkQuery (the caller) here, so a ref would be invalid.
    std::string dumpFile;
    std::string tableName;
    int dumpSize;
    // std::cout << "finalizing. read=" << r.read << " and status is "
    //           << (aborted ? "ABORTED" : "okay") << std::endl;
    //std::cout << ((void*)this) << "Finalizing query (" << id << ")" << std::endl;
    if((!aborted) && (r.open >= 0) && (r.queryWrite >= 0) 
       && (r.read >= 0)) {
        Timer t2;
        t2.start();
        boost::shared_ptr<PacketIter> resIter;
        { // Lock scope for reading
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            QuerySpec& s = _queries[id];
            resIter = s.first->getResultIter();	
            dumpFile = s.first->getSavePath();
            dumpSize = s.first->getSaveSize(); 
            tableName = s.second;
            //assert(r.localWrite == dumpSize); // not valid when using iter
            s.first.reset(); // clear out chunkquery.            
        } 
        // Lock-free merge
        if(resIter) {
            _addNewResult(resIter, tableName);
        } else {
            _addNewResult(dumpSize, dumpFile, tableName);
        }
        // Erase right before notifying.
        t2.stop();
        ss << id << " QmFinalizeMerge " << t2 << std::endl;
    } // end if 
    else { 
        Timer t2e;
        t2e.start();
        if(!aborted) {
            _isExecFaulty = true;
            std::cout << "Requesting squash " << id 
                      << " because open=" << r.open
                      << " queryWrite=" << r.queryWrite 
                      << " read=" << r.read << std::endl;
            _squashExecution();
            std::cout << " Skipped merge (read failed for id=" 
                      << id << ")" << std::endl;
        } 
        t2e.stop();
        ss << id << " QmFinalizeError " << t2e << std::endl;
    }
    Timer t3;
    t3.start();
    {
        boost::lock_guard<boost::mutex> lock(_resultsMutex);
        _results.push_back(Result(id,r));
        if(aborted) ++_squashCount; // Borrow result mutex to protect counter.
        { // Lock again to erase.
            Timer t2e1;
            t2e1.start();
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            _queries.erase(id);
            if(_queries.empty()) _queriesEmpty.notify_all();
            t2e1.stop();
            ss << id << " QmFinalizeErase " << t2e1 << std::endl;
        } 
    }
    t3.stop();
    ss << id << " QmFinalizeResult " << t3 << std::endl;
    //std::cout << (void*)this << " Done finalizing query (" << id << ")" << std::endl;
    t1.stop();
    ss << id << " QmFinalize " << t1 << std::endl;
    std::cout << ss.str();
}

// FIXME: With squashing, we should be able to return the result earlier.
// So, clients will call joinResult(), to get the result, and let a reaper
// thread call joinEverything, since that ensures that this object has 
// ceased activity and can recycle resources.
// This is a performance optimization.
void qMaster::AsyncQueryManager::joinEverything() {
    boost::unique_lock<boost::mutex> lock(_queriesMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    //_printState(std::cout);
    while(!_queries.empty()) { 
        count = _queries.size();
        if(count != lastCount) {
            std::cout << "Still " << count
                      << " in flight." << std::endl;
            count = lastCount;
            ++complainCount;
            if(complainCount > moreDetailThreshold) {
                _printState(std::cout);
                complainCount = 0;
            }
        }
        _queriesEmpty.timed_wait(lock, boost::posix_time::seconds(5));
    }
    _destroyPool();
    _merger->finalize();
    _merger.reset();
    std::cout << "Query finish. " << _queryCount << " dispatched." 
              << std::endl;
}

void qMaster::AsyncQueryManager::configureMerger(TableMergerConfig const& c) {
    _merger = boost::make_shared<TableMerger>(c);
}

std::string qMaster::AsyncQueryManager::getMergeResultName() const {
    if(_merger.get()) {
        return _merger->getTargetTable();
    }
    return std::string();
}

void qMaster::AsyncQueryManager::getReadPermission() {
    boost::unique_lock<boost::mutex> lock(_canReadMutex);
    if(!_canRead) {
        while(!_canRead) {
            _canReadCondition.timed_wait(lock, 
                                         boost::posix_time::seconds(5));
            if(_reliefFiles > 0)
                break; // Allow "relief" from too many open files
        }
    }
}

void qMaster::AsyncQueryManager::getWritePermission() {
    boost::unique_lock<boost::mutex> lock(_canReadMutex);
    while(!_canRead) { // only wait up to 5 seconds before continuing.
        _canReadCondition.timed_wait(lock, 
                                     boost::posix_time::seconds(5));
    }
}

void qMaster::AsyncQueryManager::signalTooManyFiles() {
    boost::unique_lock<boost::mutex> lock(_canReadMutex);
    std::cout << "Too many files! relieving." << std::endl;
    _reliefFiles = 500;
    _canReadCondition.notify_all();
}

void qMaster::AsyncQueryManager::pauseReadTrans() {
    boost::unique_lock<boost::mutex> lock(_canReadMutex);
    _canRead = false;
}

void qMaster::AsyncQueryManager::resumeReadTrans() {
    boost::unique_lock<boost::mutex> lock(_canReadMutex);
    _canRead = true;
    _canReadCondition.notify_all();
}


////////////////////////////////////////////////////////////////////////
// private: ////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
void qMaster::AsyncQueryManager::_initPool() {
    const int readThreads = 20;
    const int writeThreads = 500; // Should grow/shrink more dynamically
    _readQueue = boost::make_shared<lsst::qserv::common::WorkQueue>(readThreads);
    _writeQueue = boost::make_shared<lsst::qserv::common::WorkQueue>(writeThreads);
}

void qMaster::AsyncQueryManager::_destroyPool() {
    _readQueue.reset();
    _writeQueue.reset();
}

void qMaster::AsyncQueryManager::_readConfig(std::map<std::string,
                                                      std::string> const& cfg) {
    StringMap::const_iterator i = cfg.find("frontend.xrootd");
    if(i != cfg.end()) {
        _xrootdHostPort = i->second;
    } else {
        std::cout << "WARNING! No xrootd spec. Using lsst-dev01:1094" 
                  << std::endl;
        _xrootdHostPort = "lsst-dev01:1094";
    }
}

void qMaster::AsyncQueryManager::_addNewResult(PacIterPtr pacIter,
                                               std::string const& tableName) {
    bool mergeResult = _merger->merge(pacIter, tableName);
    _totalSize += pacIter->getTotalSize();
    
    if(_shouldLimitResult && (_totalSize > _resultLimit)) {
        _squashRemaining();
    }
    if(!mergeResult) {
        TableMergerError e = _merger->getError();
        if(e.resultTooBig()) {
            _squashRemaining();
        }
    }            
}

void qMaster::AsyncQueryManager::_addNewResult(ssize_t dumpSize, 
                                               std::string const& dumpFile, 
                                               std::string const& tableName) {
    assert(dumpSize >= 0);
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
            std::cout << "Error removing dumpFile " << dumpFile
                      << " errno=" << errno << std::endl;
        }        
        if(!mergeResult) {
            TableMergerError e = _merger->getError();
            if(e.resultTooBig()) {
                _squashRemaining();
            }
        }
        // std::cout << "Merge of " << dumpFile << " into "
        //           << tableName 
        //           << (mergeResult ? " OK----" : " FAIL====") 
        //           << std::endl;
    }
}

void qMaster::AsyncQueryManager::_printState(std::ostream& os) {
    typedef std::map<int, boost::shared_ptr<ChunkQuery> > QueryMap;
    std::for_each(_queries.begin(), _queries.end(), printQueryMapValue(os));
}

void qMaster::AsyncQueryManager::_squashExecution() {
    // Halt new query dispatches and cancel the ones in flight.
    // This attempts to save on resources and latency, once a query
    // fault is detected.
    
    if(_isSquashed) return;  
    _isSquashed = true; // Mark before acquiring lock--faster.
    //std::cout << "Squash requested by "<<(void*)this << std::endl;
    Timer t;
    // Squashing is dependent on network latency and remote worker
    // responsiveness, so make a copy so others don't have to wait.
    std::vector<std::pair<int, QuerySpec> > myQueries;
    {
        boost::unique_lock<boost::mutex> lock(_queriesMutex);
        t.start();
        myQueries.resize(_queries.size());
        std::cout << "AsyncQM squashExec copy " <<  std::endl;
        std::copy(_queries.begin(), _queries.end(), myQueries.begin());
    }
    std::cout << "AsyncQM squashQueued" << std::endl;
    _writeQueue->cancelQueued();
    std::cout << "AsyncQM squashExec iteration " <<  std::endl;
    std::for_each(myQueries.begin(), myQueries.end(), 
                  squashQuery(_queriesMutex, _queries));
    t.stop();
    std::cout << "AsyncQM squashExec " << t << std::endl;
    _isSquashed = true; // Ensure that flag wasn't trampled.
}

void qMaster::AsyncQueryManager::_squashRemaining() {
    _squashExecution();  // Not sure if this is right. FIXME
}
