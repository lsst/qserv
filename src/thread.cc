// -*- LSST-C++ -*-

// Standard
#include <fstream>
// Boost
#include <boost/make_shared.hpp>
// LSST 
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/thread.h"

using boost::make_shared;

namespace qMaster = lsst::qserv::master;

// Local Helpers --------------------------------------------------
namespace { 
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
}

//////////////////////////////////////////////////////////////////////
// TransactionSpec
//////////////////////////////////////////////////////////////////////
qMaster::TransactionSpec::Reader::Reader(std::string const& file) {
    // Read the file into memory.  All of it.
    using namespace std;
    ifstream is;
    is.open(file.c_str(), ios::binary);

    // get length of file:
    is.seekg(0, ios::end);
    _rawLength = is.tellg();
    is.seekg(0, ios::beg);
    
    // allocate memory:
    _rawContents = new char[_rawLength];
    
    // read data as a block:
    is.read(_rawContents, _rawLength);
    is.close();
    _pos = 0;
}



qMaster::TransactionSpec qMaster::TransactionSpec::Reader::getSpec() {

    int beginPath;
    int endPath;
    int beginQuery;
    int endQuery;
    const int magicLength=4;
    qMaster::TransactionSpec ts;

    beginPath = seekMagic(_pos, _rawContents, _rawLength);
    if(beginPath == _rawLength) return ts;
    beginPath += magicLength; // Start after magic sequence.

    endPath = seekMagic(beginPath, _rawContents, _rawLength);
    if(endPath == _rawLength) return ts;
    beginQuery = endPath + magicLength;

    endQuery = seekMagic(beginQuery, _rawContents, _rawLength);
    if(endQuery == _rawLength) return ts;
    ts.path = std::string(_rawContents + beginPath, endPath - beginPath);
    ts.query = std::string(_rawContents + beginQuery, endQuery - beginQuery);
    ts.savePath = "/dev/null";
    ts.bufferSize = 8192000;
    _pos = endQuery + magicLength; // Advance past the detected marker.
    return ts; // FIXME placeholder
}
//////////////////////////////////////////////////////////////////////
// TransactionCallable
//////////////////////////////////////////////////////////////////////
// for now, two simultaneous writes (queries)
qMaster::Semaphore qMaster::TransactionCallable::_sema(120);

void qMaster::TransactionCallable::operator()() {
    using namespace lsst::qserv::master;
    std::cout << _spec.path << " in flight\n";
    _result = xrdOpenWriteReadSaveClose(_spec.path.c_str(),
					_spec.query.c_str(),
					_spec.query.length(),
					_spec.bufferSize,
					_spec.savePath.c_str());
    std::cout << _spec.path << " finished\n";

}



//////////////////////////////////////////////////////////////////////
// Manager
//////////////////////////////////////////////////////////////////////
void qMaster::Manager::setupFile(std::string const& file) {
    _file = file;
    _reader = make_shared<qMaster::TransactionSpec::Reader>(file);
}
void qMaster::Manager::_joinOne() {
    int oldsize = _threads.size();
    ThreadDeque newDeq;
    if(oldsize == 0) return;

    while(1) {
	std::remove_copy_if(_threads.begin(), _threads.end(), 
			    back_inserter(newDeq),
			    tryJoinBoostThread<boost::shared_ptr<boost::thread> >());
	// newDeq now has threads that didn't join.
	if(newDeq.size() == oldsize) {
	    newDeq.clear();
	    boost::this_thread::sleep(boost::posix_time::milliseconds(500));
	} else {
	    _threads = newDeq;
	    break;
	}
    }
}

void qMaster::Manager::run() {
    int inFlight = 0;
    time_t lastReap;
    time_t thisReap;
    int reapSize;
    int thisSize;
    time(&thisReap);
    if(_reader.get() == 0) { return; }
    while(1) {
	qMaster::TransactionSpec s = _reader->getSpec();
	if(s.isNull()) { break; }
	qMaster::TransactionCallable t(s);
	_threads.push_back(make_shared<boost::thread>(t));
	++inFlight;
	thisSize = _threads.size();
	if(thisSize > _highWaterThreads) {
	    lastReap = thisReap;
	    std::cout << "Reaping, "<< inFlight << " dispatched.\n";
	    _joinOne();
	    time(&thisReap);
	    reapSize = _threads.size();
	    std::cout << thisReap << " Done reaping, " << reapSize
		      << " still flying, completion rate=" 
		      << (1.0+thisSize - reapSize)*1.0/(1.0+thisReap - lastReap)
		      << "\n"  ;
	}
	if(_threads.size() > 1000) break; // DEBUG early exit.
    }
    std::cout << "Joining\n";
    std::for_each(_threads.begin(), _threads.end(), 
		  joinBoostThread<boost::shared_ptr<boost::thread> >());
}


/// Adds a transaction (open/write/read/close) operation to the query
/// manager, which is run with best-effort.
/// @param t specification for this transaction.
/// @param id Optional, specify the id for this query
/// Generally, the query id is selected by the query manager, but may
/// be presented by the caller.  Caller assumes responsibility for
/// ensuring id uniqueness when doing this. 
int qMaster::QueryManager::add(TransactionSpec const& t, int id) {
    if(id == -1) {
	id = _getNextId();
    }
    assert(id >= 0);
    if(t.isNull()) { return -1; }
    qMaster::TransactionCallable tc(t);
    {
	boost::unique_lock<boost::mutex> lock(_waitingMutex);
	_waiting.push_back(IdCallable(id, ManagedCallable(*this, id, t)));
    }
    _addThreadIfSpace();
}

/// Record the result of a completed query transaction, 
/// and retrieve another callable transaction, if one is available.
/// The returned transaction is marked as running.
/// @param id Id of completed transaction
/// @param r Transaction result
/// @return The next callable that can be executed.
qMaster::QueryManager::ManagedCallable qMaster::QueryManager::completeAndFetch(int id, XrdTransResult const& r) {
    TransactionSpec nullSpec;
    int nextId = -1;
    {
	boost::unique_lock<boost::mutex> rlock(_runningMutex);
	boost::unique_lock<boost::mutex> flock(_finishedMutex);
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

boost::shared_ptr<qMaster::QueryManager::ManagedCallable> qMaster::QueryManager::_getNextCallable() {
    boost::shared_ptr<ManagedCallable> mc; 
    boost::unique_lock<boost::mutex> wlock(_waitingMutex);
    boost::unique_lock<boost::mutex> rlock(_runningMutex);
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

int qMaster::QueryManager::_getNextId() {
    // FIXME(eventually) should track ids in use and recycle ids like pids.
    static int x = 0;
    static boost::mutex mutex;
    boost::unique_lock<boost::mutex> lock(mutex);
    return ++x;
}

void qMaster::QueryManager::_addThreadIfSpace() {
    {
        boost::unique_lock<boost::mutex> lock(_callablesMutex);
        if(_callables.size() >= _highWaterThreads) {
            // Don't add if there are already lots of callables in flight.
            return; 
        }
    }
    _tryJoinAll();
    {
        boost::unique_lock<boost::mutex> lock(_threadsMutex);
        if(_threads.size() < _highWaterThreads) {
            boost::shared_ptr<boost::thread> t = _startThread();
            if(t.get() != 0) {
                _threads.push_back(t);
            }
        }
    }
}

void qMaster::QueryManager::_tryJoinAll() {
    boost::unique_lock<boost::mutex> lock(_threadsMutex);
    int oldsize = _threads.size();
    ThreadDeque newDeq;
    if(oldsize == 0) return;
    std::remove_copy_if(_threads.begin(), _threads.end(), 
			back_inserter(newDeq),
			tryJoinBoostThread<boost::shared_ptr<boost::thread> >());
    // newDeq now has threads that didn't join.
    if(newDeq.size() == oldsize) {
	newDeq.clear();
    } else {
	_threads = newDeq;
    }
}
void qMaster::QueryManager::joinEverything() {
    while(1) {
	_tryJoinAll();
	if(_threads.size() > 0) {
	    sleep(1);
	} else {
	    break;
	}
    }
}

boost::shared_ptr<boost::thread> qMaster::QueryManager::_startThread() {
    boost::shared_ptr<ManagedCallable> c(_getNextCallable());
    if(c.get() != 0) {
        return make_shared<boost::thread>(*c);
    }
    return boost::shared_ptr<boost::thread>();
}


void qMaster::QueryManager::addCallable(ManagedCallable* c) {
    boost::unique_lock<boost::mutex> lock(_callablesMutex);
    _callables.insert(c);
    // FIXME: is there something else to do?
}

void qMaster::QueryManager::dropCallable(ManagedCallable* c) {
    boost::unique_lock<boost::mutex> lock(_callablesMutex);
    _callables.erase(c);
    // FIXME: is there something else to do?
}

//////////////////////////////////////////////////////////////////////
// QueryManager::ManagedCallable
//////////////////////////////////////////////////////////////////////
qMaster::QueryManager::ManagedCallable::ManagedCallable() 
    : _qm(0), _id(0), _c(TransactionCallable(TransactionSpec())) {
}

qMaster::QueryManager::ManagedCallable::ManagedCallable(
    qMaster::QueryManager& qm, int id, qMaster::TransactionSpec const& t) 
    :_qm(&qm), _id(id), _c(t) {
    assert(_qm != 0);    
}

qMaster::QueryManager::ManagedCallable& qMaster::QueryManager::ManagedCallable::operator=(ManagedCallable const& m) {
    _qm = m._qm;
    _id = m._id;
    _c = m._c;
}

void qMaster::QueryManager::ManagedCallable::operator()() {
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

