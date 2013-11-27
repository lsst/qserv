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


// Standard
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
// Boost
#include <boost/make_shared.hpp>
// LSST
#include "log/Logger.h"
#include "xrdc/xrdfile.h"
#include "control/thread.h"
#include "util/xrootd.h"
#include "merger/TableMerger.h"

// Xrootd
#include "XrdPosix/XrdPosixCallBack.hh"

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

int seekMagic(int start, char* buffer, int term) {
    // Find magic sequence
    const char m[] = "####"; // MAGIC!
    for(int p = start; p < term; ++p) {
        if(((term - p) > 4) &&
           (buffer[p+0] == m[0]) && (buffer[p+1] == m[1]) &&
           (buffer[p+2] == m[2]) && (buffer[p+3] == m[3])) {
            return p;
        }
    }
    return term;
}
} // anonymous namespace


namespace lsst {
namespace qserv {
namespace control {
    
//////////////////////////////////////////////////////////////////////
// TransactionSpec
//////////////////////////////////////////////////////////////////////
TransactionSpec::Reader::Reader(std::string const& file) {
    _mmapFd = -1;
    _mmapChunk = 0;
    _rawContents = NULL;
    _setupMmap(file);
    //_readWholeFile(file);
    _pos = 0;
}

TransactionSpec::Reader::~Reader() {
    if(_rawContents != NULL) delete[] _rawContents; // cleanup
    _cleanupMmap();
}

void TransactionSpec::Reader::_readWholeFile(std::string const& file) {
    // Read the file into memory.  All of it.
    std::ifstream is;
    is.open(file.c_str(), std::ios::binary);

    // get length of file:
    is.seekg(0, std::ios::end);
    _rawLength = is.tellg();
    if(_rawLength <= 0) {
        _rawContents = NULL;
        return;
    }
    is.seekg(0, std::ios::beg);

    // allocate memory:
    _rawContents = new char[_rawLength];

    // read data as a block:
    is.read(_rawContents, _rawLength);
    is.close();
 }

void TransactionSpec::Reader::_setupMmap(std::string const& file) {
    { // get length
        std::ifstream is;
        is.open(file.c_str(), std::ios::binary);

        // get length of file:
        is.seekg(0, std::ios::end);
        _rawLength = is.tellg();
        is.close();
    }
    // 0x1000: 4K, 0x10000: 64K 0x100000:1M, 0x1000 000: 16M
    _mmapDefaultSize = 0x1000000; // 16M
    _mmapChunkSize = _mmapDefaultSize;
    _mmapMinimum = 0x40000; // 256K
    _mmapOffset = 0;
    _mmapFd = open(file.c_str(), O_RDONLY);
    _mmapChunk = NULL;
    _mmapChunk = (char*)mmap(NULL, _mmapDefaultSize, PROT_READ, MAP_PRIVATE,
                             _mmapFd, _mmapOffset);
    if(_mmapChunk == (void*)-1) {
        perror("error mmaping.");
    }
    assert(_mmapChunk != (void*)-1);
}

void TransactionSpec::Reader::_advanceMmap() {
    int distToEnd = _rawLength - _mmapOffset;
    if(distToEnd > _mmapDefaultSize) { // Non-last chunk?
        int posInChunk = _pos - _mmapOffset;
        int distToBorder = _mmapDefaultSize - posInChunk;
        if(distToBorder < _mmapMinimum) {
            munmap(_mmapChunk, _mmapDefaultSize);
            _mmapOffset += _mmapDefaultSize - _mmapMinimum;
            _mmapChunk = (char*)mmap(NULL, _mmapDefaultSize, PROT_READ,
                                     MAP_PRIVATE, _mmapFd, _mmapOffset);
            assert(_mmapChunk != (void*)-1);

            if((_rawLength - _mmapOffset) < _mmapDefaultSize) {
                // Chunk exceeds end of file.
                // Overwrite mmap chunk size
                _mmapChunkSize = _rawLength - _mmapOffset;
            }
        }
    }
}

void TransactionSpec::Reader::_cleanupMmap() {
    if(_mmapChunk != NULL) {
        munmap(_mmapChunk, _mmapChunkSize);
    }
    if(_mmapFd != -1) {
        close(_mmapFd);
    }
}

TransactionSpec TransactionSpec::Reader::getSpec() {
    int beginPath;
    int endPath;
    int beginQuery;
    int endQuery;
    //int bufEnd = _rawContents;
    int bufEnd = _mmapChunkSize;;

    const int magicLength=4;
    TransactionSpec ts;

    //beginPath = seekMagic(_pos, _rawContents, bufEnd);
    beginPath = seekMagic(_pos-_mmapOffset, _mmapChunk, bufEnd);
    if(beginPath == bufEnd) return ts;
    beginPath += magicLength; // Start after magic sequence.

    //endPath = seekMagic(beginPath, _rawContents, bufEnd);
    endPath = seekMagic(beginPath, _mmapChunk, bufEnd);
    if(endPath == bufEnd) return ts;
    beginQuery = endPath + magicLength;

    //endQuery = seekMagic(beginQuery, _rawContents, bufEnd);
    endQuery = seekMagic(beginQuery, _mmapChunk, bufEnd);
    if(endQuery == bufEnd) return ts;
    // ts.path = std::string(_rawContents + beginPath, endPath - beginPath);
    // ts.query = std::string(_rawContents + beginQuery, endQuery - beginQuery);
    ts.path = std::string(_mmapChunk + beginPath, endPath - beginPath);
    ts.query = std::string(_mmapChunk + beginQuery, endQuery - beginQuery);
    ts.savePath = "/dev/null";
    ts.bufferSize = 1024000;
    //_pos = endQuery + magicLength; // Advance past the detected marker.
    _pos = _mmapOffset + endQuery + magicLength; // Advance past the detected marker.
    _advanceMmap();
    return ts;
}

//////////////////////////////////////////////////////////////////////
// TransactionCallable
//////////////////////////////////////////////////////////////////////
// for now, two simultaneous writes (queries)
Semaphore TransactionCallable::_sema(120);

void TransactionCallable::operator()() {
    using namespace lsst::qserv::xrdc;
    LOGGER_INF << _spec.path << " in flight\n";
    _result = xrdOpenWriteReadSaveClose(_spec.path.c_str(),
                                        _spec.query.c_str(),
                                        _spec.query.length(),
                                        _spec.bufferSize,
                                        _spec.savePath.c_str());
    LOGGER_INF << _spec.path << " finished\n";
}

//////////////////////////////////////////////////////////////////////
// Manager
//////////////////////////////////////////////////////////////////////
void Manager::setupFile(std::string const& file) {
    _file = file;
    _reader = make_shared<TransactionSpec::Reader>(file);
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
        TransactionSpec s = _reader->getSpec();
        if(s.isNull()) { break; }
        TransactionCallable t(s);
        _threads.push_back(make_shared<boost::thread>(t));
        ++inFlight;
        thisSize = _threads.size();
        if(thisSize > _highWaterThreads) {
            lastReap = thisReap;
            LOGGER_INF << "Reaping, "<< inFlight << " dispatched.\n";
            _joinOne();
            time(&thisReap);
            reapSize = _threads.size();
            LOGGER_INF << thisReap << " Done reaping, " << reapSize
                      << " still flying, completion rate="
                      << (1.0+thisSize - reapSize)*1.0/(1.0+thisReap - lastReap)
                      << "\n"  ;
        }
        if(_threads.size() > 1000) break; // DEBUG early exit.
    }
    LOGGER_INF << "Joining\n";
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
int QueryManager::add(TransactionSpec const& t, int id) {
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
    TransactionSpec nullSpec;
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
        LOGGER_INF << "Threads left:" << _threads.size() << std::endl;
        time(&last);
        _tryJoinAll();
        time(&now);
        LOGGER_INF << "Joinloop took:" << now-last << std::endl;
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
    : _qm(0), _id(0), _c(TransactionCallable(TransactionSpec())) {
}

QueryManager::ManagedCallable::ManagedCallable(
    QueryManager& qm, int id, TransactionSpec const& t)
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

}}} // namespace lsst::qserv::control
