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

qMaster::QueryManager::ManagedCallable::ManagedCallable(
    qMaster::QueryManager& qm, int id, qMaster::TransactionSpec const& t) 
    :_qm(qm), _id(id), _c(t) {
    
}

void qMaster::QueryManager::ManagedCallable::operator()() {
    _qm.addThread(this);
    while(!_c.getSpec().isNull()) {
	_c();
	ManagedCallable c = _qm.completeAndFetch(_id, _c.getResult());
	_id = c._id;
	_c = c._c;
    }
    // No more work. Die.
    _qm.dropThread(this);
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


qMaster::QueryManager::ManagedCallable qMaster::QueryManager::completeAndFetch(int id, XrdTransResult const& r) {
    // FIXME, still need to implement
    {
	boost::unique_lock<boost::mutex> rlock(_runningMutex);
	boost::unique_lock<boost::mutex> flock(_finishedMutex);
	// Pull from _running
	// Insert into _finished
    }
    {
	boost::unique_lock<boost::mutex> wlock(_waitingMutex);
	boost::unique_lock<boost::mutex> rlock(_runningMutex);
	// Try to pull from _waiting
	// If exist, return it, so caller can run it.
    }
    
    // FIXME 
    // MUST RETURN MANAGED CALLABLE
}

int qMaster::QueryManager::_getNextId() {
    // FIXME
    assert(false);
    return 0;
}

void qMaster::QueryManager::_addThreadIfSpace() {
    // FIXME
}

void qMaster::QueryManager::addThread(ManagedCallable* c) {
    // FIXME
}

void qMaster::QueryManager::dropThread(ManagedCallable* c) {
    // FIXME
}
