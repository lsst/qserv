// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2009-2015 AURA/LSST.
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

// Class header
#include "ccontrol/thread.h"

// System headers
#include <algorithm>

// Third-party headers
#include "boost/make_shared.hpp"
#include "XrdPosix/XrdPosixCallBack.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "rproc/TableMerger.h"
#include "util/xrootd.h"
#include "xrdc/xrdfile.h"


// Namespace modifiers
using boost::make_shared;

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

} // anonymous namespace


namespace lsst {
namespace qserv {
namespace ccontrol {

//////////////////////////////////////////////////////////////////////
// TransactionCallable
//////////////////////////////////////////////////////////////////////
// for now, two simultaneous writes (queries)
Semaphore TransactionCallable::_sema(120);

void TransactionCallable::operator()() {
    using namespace lsst::qserv::xrdc;
    LOGF_INFO("%1% in flight" % _spec.path);
    _result = xrdOpenWriteReadSaveClose(_spec.path.c_str(),
                                        _spec.query.c_str(),
                                        _spec.query.length(),
                                        _spec.bufferSize,
                                        _spec.savePath.c_str());
    LOGF_INFO("%1% finished" % _spec.path);
}

//////////////////////////////////////////////////////////////////////
// Manager
//////////////////////////////////////////////////////////////////////
void Manager::setupFile(std::string const& file) {
    _file = file;
    _reader = make_shared<qdisp::TransactionSpec::Reader>(file);
}
void Manager::_joinOne() {
    int oldsize = _threads.size();
    ThreadDeque newDeq;
    if(oldsize == 0) return;

    while(1) {
        std::remove_copy_if(
                      _threads.begin(), _threads.end(),
                      back_inserter(newDeq),
                      tryJoinBoostThread<boost::shared_ptr<boost::thread> >());
        // newDeq now has threads that didn't join.
        if(newDeq.size() == (unsigned)oldsize) {
            newDeq.clear();
            boost::this_thread::sleep(boost::posix_time::milliseconds(500));
        } else {
            _threads = newDeq;
            break;
        }
    }
}

void Manager::run() {
    int inFlight = 0;
    time_t lastReap;
    time_t thisReap;
    int reapSize;
    int thisSize;
    time(&thisReap);
    if(_reader.get() == 0) { return; }
    while(1) {
        qdisp::TransactionSpec s = _reader->getSpec();
        if(s.isNull()) { break; }
        TransactionCallable t(s);
        _threads.push_back(make_shared<boost::thread>(t));
        ++inFlight;
        thisSize = _threads.size();
        if(thisSize > _highWaterThreads) {
            lastReap = thisReap;
            LOGF_INFO("Reaping, %1% dispatched." % inFlight);
            _joinOne();
            time(&thisReap);
            reapSize = _threads.size();
            LOGF_INFO("%1% Done reaping, %2% still flying, completion rate=%3%"
                      % thisReap % reapSize
                      % ((1.0+thisSize - reapSize)*1.0/(1.0+thisReap - lastReap)));
        }
        if(_threads.size() > 1000) break; // DEBUG early exit.
    }
    LOGF_INFO("Joining");
    std::for_each(_threads.begin(), _threads.end(),
                  joinBoostThread<boost::shared_ptr<boost::thread> >());
}

//////////////////////////////////////////////////////////////////////
// QueryManager
//////////////////////////////////////////////////////////////////////
/// Adds a transaction (open/write/read/close) operation to the query
/// manager, which is run with best-effort.
/// @param t specification for this transaction.
/// @param id Optional, specify the id for this query
/// Generally, the query id is selected by the query manager, but may
/// be presented by the caller.  Caller assumes responsibility for
/// ensuring id uniqueness when doing this.
int QueryManager::add(qdisp::TransactionSpec const& t, int id) {
    if(id == -1) {
        id = _getNextId();
    }
    assert(id >= 0);
    if(t.isNull()) { return -1; }
    TransactionCallable tc(t);
    {
        boost::lock_guard<boost::mutex> lock(_waitingMutex);
        _waiting.push_back(IdCallable(id, ManagedCallable(*this, id, t)));
    }
    _addThreadIfSpace();
    return id;
}

/// Record the result of a completed query transaction,
/// and retrieve another callable transaction, if one is available.
/// The returned transaction is marked as running.
/// @param id Id of completed transaction
/// @param r Transaction result
/// @return The next callable that can be executed.
QueryManager::ManagedCallable
QueryManager::completeAndFetch(int id, xrdc::XrdTransResult const& r) {
    qdisp::TransactionSpec nullSpec;
    {
        boost::lock_guard<boost::mutex> rlock(_runningMutex);
        boost::lock_guard<boost::mutex> flock(_finishedMutex);
        // Pull from _running
        _running.erase(id);
        // Insert into _finished
        _finished[id] = r;
    }
    // MUST RETURN MANAGED CALLABLE
    boost::shared_ptr<ManagedCallable> mc;
    mc = _getNextCallable();
    if(mc.get() != 0) {
        return *mc;
    } else {
        return ManagedCallable(*this, 0, nullSpec);
    }
}

boost::shared_ptr<QueryManager::ManagedCallable>
QueryManager::_getNextCallable() {
    boost::shared_ptr<ManagedCallable> mc;
    boost::lock_guard<boost::mutex> wlock(_waitingMutex);
    boost::lock_guard<boost::mutex> rlock(_runningMutex);
    int nextId = -1;
    // Try to pull from _waiting
    if(!_waiting.empty()) {
        IdCallable const& ic = _waiting.front();
        mc = make_shared<ManagedCallable>(ic.second);
        nextId = ic.first;
        assert(nextId >= 0);
        _running[nextId] = *mc;
        _waiting.pop_front();
        // If exist, return it, so caller can run it.
    }
    return mc;
}

int QueryManager::_getNextId() {
    // FIXME(eventually) should track ids in use and recycle ids like pids.
    static int x = 0;
    static boost::mutex mutex;
    boost::lock_guard<boost::mutex> lock(mutex);
    return ++x;
}

void QueryManager::_addThreadIfSpace() {
    {
        boost::lock_guard<boost::mutex> lock(_callablesMutex);
        if(_callables.size() >= (unsigned)_highWaterThreads) {
            // Don't add if there are already lots of callables in flight.
            return;
        }
    }
    _tryJoinAll();
    {
        boost::lock_guard<boost::mutex> lock(_threadsMutex);
        if(_threads.size() < (unsigned)_highWaterThreads) {
            boost::shared_ptr<boost::thread> t = _startThread();
            if(t.get() != 0) {
                _threads.push_back(t);
            }
        }
    }
}

void QueryManager::_tryJoinAll() {
    boost::lock_guard<boost::mutex> lock(_threadsMutex);
    int oldsize = _threads.size();
    ThreadDeque newDeq;
    if(oldsize == 0) return;
    std::remove_copy_if(
                      _threads.begin(), _threads.end(),
                      back_inserter(newDeq),
                      tryJoinBoostThread<boost::shared_ptr<boost::thread> >());
    // newDeq now has threads that didn't join.
    if(newDeq.size() == (unsigned)oldsize) {
        newDeq.clear();
    } else {
        _threads = newDeq;
    }
}

void QueryManager::joinEverything() {
    time_t now;
    time_t last;
    while(1) {
        LOGF_INFO("Threads left:%1%" % _threads.size());
        time(&last);
        _tryJoinAll();
        time(&now);
        LOGF_INFO("Joinloop took:%1%" % (now-last));
        if(_threads.size() > 0) {
            sleep(1);
        } else {
            break;
        }
    }
}

boost::shared_ptr<boost::thread> QueryManager::_startThread() {
    boost::shared_ptr<ManagedCallable> c(_getNextCallable());
    if(c.get() != 0) {
        return make_shared<boost::thread>(*c);
    }
    return boost::shared_ptr<boost::thread>();
}

void QueryManager::addCallable(ManagedCallable* c) {
    boost::lock_guard<boost::mutex> lock(_callablesMutex);
    _callables.insert(c);
    // FIXME: is there something else to do?
}

void QueryManager::dropCallable(ManagedCallable* c) {
    boost::lock_guard<boost::mutex> lock(_callablesMutex);
    _callables.erase(c);
    // FIXME: is there something else to do?
}

//////////////////////////////////////////////////////////////////////
// QueryManager::ManagedCallable
//////////////////////////////////////////////////////////////////////
QueryManager::ManagedCallable::ManagedCallable()
    : _qm(0), _id(0), _c(TransactionCallable(qdisp::TransactionSpec())) {
}

QueryManager::ManagedCallable::ManagedCallable(
    QueryManager& qm, int id, qdisp::TransactionSpec const& t)
    :_qm(&qm), _id(id), _c(t) {
    assert(_qm != 0);
}

QueryManager::ManagedCallable&
QueryManager::ManagedCallable::operator=(ManagedCallable const& m) {
    _qm = m._qm;
    _id = m._id;
    _c = m._c;
    return *this;
}

void QueryManager::ManagedCallable::operator()() {
    _qm->addCallable(this);
    while(!_c.getSpec().isNull()) {
        _c(); /// Do the real work.
        ManagedCallable c = _qm->completeAndFetch(_id, _c.getResult());
        _id = c._id;
        _c = c._c;
    }
    // No more work. Die.
    _qm->dropCallable(this);
}

}}} // namespace lsst::qserv::ccontrol
