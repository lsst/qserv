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
#include "lsst/qserv/worker/QueryRunnerManager.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/worker/Config.h"
namespace qWorker = lsst::qserv::worker;

////////////////////////////////////////////////////////////////////////
// local helpers
////////////////////////////////////////////////////////////////////////
namespace {
/// matchHash: helper functor that matches queries by hash.
class matchHash { 
public:
    matchHash(std::string const& hash_) : hash(hash_) {}
    inline bool operator()(qWorker::QueryRunnerArg const& a) {
        return a.s.hash == hash;
    }
    inline bool operator()(qWorker::QueryRunner const* r) {
        return r->getHash() == hash;
    }
    std::string hash;
};

template <typename Callable>
void launchThread(Callable const& c) {
    // Oddly enough, inlining the line below instead of
    // calling this function doesn't work.
    boost::thread t(c);
}
} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunnerManager::argMatch
////////////////////////////////////////////////////////////////////////
class qWorker::QueryRunnerManager::argMatch {
public:
    argMatch(int chunkId_) : chunkId(chunkId_) {}
    bool operator()(ArgQueue::value_type const& v) {
        return chunkId == v.s.chunkId;
    }
    int chunkId;
};

////////////////////////////////////////////////////////////////////////
// lsst::qserv::worker::QueryRunnerManager
////////////////////////////////////////////////////////////////////////
void qWorker::QueryRunnerManager::_init() {
    // Check config for numthreads.
    _limit = getConfig().getInt("numThreads", _limit);     
}

qWorker::QueryRunnerArg const& 
qWorker::QueryRunnerManager::_getQueueHead() const {
    assert(!_args.empty());
    return _args.front();
}

void 
qWorker::QueryRunnerManager::runOrEnqueue(qWorker::QueryRunnerArg const& a) {
    boost::lock_guard<boost::mutex> m(_mutex);
    if(hasSpace()) {
        // Can't simply do: boost::thread t(qWorker::QueryRunner(a));
        // Must call function.
        launchThread(qWorker::QueryRunner(a)); 
    } else {
        _enqueue(a);
    }
}

bool qWorker::QueryRunnerManager::squashByHash(std::string const& hash) {
    boost::lock_guard<boost::mutex> m(_mutex);
    bool success = _cancelQueued(hash) || _cancelRunning(hash);
    // Check if squash okay?
    if(success) {
        // Notify the tracker in case someone is waiting.
        ResultError r(-2, "Squashed by request");
        QueryRunner::getTracker().notify(hash, r);
        // Remove squash notification to prevent future poisioning.
        QueryRunner::getTracker().clearNews(hash);
    }
    return success;
}

void qWorker::QueryRunnerManager::_popQueueHead() {
    assert(!_args.empty());
    _args.pop_front();
}

void qWorker::QueryRunnerManager::addRunner(QueryRunner* q) {
    // The QueryRunner object will only add itself if it is valid, and 
    // will drop itself before it becomes invalid.
    boost::lock_guard<boost::mutex> m(_mutex);
    _runners.push_back(q);    
}

void qWorker::QueryRunnerManager::dropRunner(QueryRunner* q) {
    // The QueryRunner object will only add itself if it is valid, and 
    // will drop itself before it becomes invalid.
    boost::lock_guard<boost::mutex> m(_mutex);
    QueryQueue::iterator b = _runners.begin();
    QueryQueue::iterator e = _runners.end();
    QueryQueue::iterator qi = find(b, e, q);
    assert(qi != e); // Otherwise the deque is corrupted.
    _runners.erase(qi);
}

bool qWorker::QueryRunnerManager::recycleRunner(ArgFunc* af, int lastChunkId) {
    boost::lock_guard<boost::mutex> m(_mutex);
    if((!isOverloaded()) && (!_args.empty())) {
        if(true) { // Simple version
            (*af)(_getQueueHead()); // Switch to new query
            _popQueueHead();
        } else { // Prefer same chunk, if possible. 
            // For now, don't worry about starving other chunks.
            ArgQueue::iterator i;
            i = std::find_if(_args.begin(), _args.end(), argMatch(lastChunkId));
            if(i != _args.end()) {
                (*af)(*i);
                _args.erase(i);
            } else {
                (*af)(_getQueueHead()); // Switch to new query
                _popQueueHead();            
            }
        }
        return true;
    } 
    return false;
}

////////////////////////////////////////////////////////////////////////
// private:
bool qWorker::QueryRunnerManager::_cancelQueued(std::string const& hash) {
    // Should be locked now.
    ArgQueue::iterator b = _args.begin();
    ArgQueue::iterator e = _args.end();
    ArgQueue::iterator q = find_if(b, e, matchHash(hash));
    if(q != e) {
        _args.erase(q);
        return true;
    }
    return false;
}

bool qWorker::QueryRunnerManager::_cancelRunning(std::string const& hash) {
    // Should be locked now.
    QueryQueue::iterator b = _runners.begin();
    QueryQueue::iterator e = _runners.end();
    QueryQueue::iterator q = find_if(b, e, matchHash(hash));
    if(q != e) {
        (*q)->poison(hash); // Poison the query exec, by hash.
        return true;
    }
    return false;
}

void qWorker::QueryRunnerManager::_enqueue(qWorker::QueryRunnerArg const& a) {
    ++_jobTotal;
    _args.push_back(a);
}
