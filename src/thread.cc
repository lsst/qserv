// -*- LSST-C++ -*-

// Standard
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
// Boost
#include <boost/make_shared.hpp>
// LSST 
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/thread.h"
#include "lsst/qserv/master/xrootd.h"
#include "lsst/qserv/master/TableMerger.h"

// Xrootd
#include "XrdPosix/XrdPosixCallBack.hh"


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


//////////////////////////////////////////////////////////////////////
// class ChunkQuery 
//////////////////////////////////////////////////////////////////////
void qMaster::ChunkQuery::Complete(int Result) {
	bool isReallyComplete = false;
	switch(_state) {
	case WRITE_OPEN: // Opened, so we can send off the query
	    {
		boost::lock_guard<boost::mutex> lock(_mutex);
		_result.open = Result;
	    }
	    if(Result < 0) { // error? 
		_result.open = Result;
		isReallyComplete = true;
		_state = COMPLETE;
	    } else {
		_state = WRITE_WRITE;
		_sendQuery(Result);
	    }
	    break;
	case READ_OPEN: // Opened, so we can read-back the results.
	    if(Result < 0) { // error? 
		_result.read = Result;
		std::cout << "Problem reading result: read=" 
			  << _result.read << std::endl;
		isReallyComplete = true;
		_state = COMPLETE;
	    } else {
		_state = READ_READ;
		_readResults(Result);
	    }
	    break;
	default:
	    isReallyComplete = true;
	    _state = CORRUPT;
	}

	if(isReallyComplete) { _notifyManager(); }
}

qMaster::ChunkQuery::ChunkQuery(qMaster::TransactionSpec const& t, int id, 
				qMaster::AsyncQueryManager* mgr) 
    : _spec(t), _manager(mgr), _id(id), XrdPosixCallBack() {
    assert(_manager != NULL);
    _result.open = 0;
    _result.queryWrite = 0;
    _result.read = 0;
    _result.localWrite = 0;
    // Patch the spec to include the magic query terminator.
    _spec.query.append(4,0); // four null bytes.

}

void qMaster::ChunkQuery::run() {
    // This lock ensures that the remaining ChunkQuery::Complete() calls
    // do not proceed until this initial step completes.
    boost::lock_guard<boost::mutex> lock(_mutex);

    _state = WRITE_OPEN;
    std::cout << "Opening " << _spec.path << "\n";
    int result = qMaster::xrdOpenAsync(_spec.path.c_str(), O_WRONLY, this);
    if(result != -EINPROGRESS) {
	// don't continue, set result with the error.
	std::cout << "Not EINPROGRESS, should not continue with " 
		  << _spec.path << "\n";
	_result.open = result;
	_state = COMPLETE;
	_notifyManager(); // manager should delete me.
    } else {
	std::cout << "Waiting for " << _spec.path << "\n";
	_hash = qMaster::hashQuery(_spec.query.c_str(), 
				   _spec.query.size());
	
    }
    // Callback(Complete) will handle the rest.
}

std::string qMaster::ChunkQuery::getDesc() const {
    std::stringstream ss;
    ss << "Query " << _id << " (" << _hash << ") " << _resultUrl
       << " " << _queryHostPort << " state=";
    switch(_state) {
    case WRITE_OPEN:
	ss << "openingWrite";
	break;
    case WRITE_WRITE:
	ss << "writing";
	break;
    case READ_OPEN:
	ss << "openingRead";
	break;
    case READ_READ:
	ss << "reading";
	break;
    case COMPLETE:
	ss << "complete";
	break;
    case CORRUPT:
	ss << "corrupted";
	break;
    default:
	break;
    }
    return ss.str();
}

void qMaster::ChunkQuery::_sendQuery(int fd) {
	bool isReallyComplete = false;
	// Now write
	int len = _spec.query.length();
	int writeCount = qMaster::xrdWrite(fd, _spec.query.c_str(), len);
	if(writeCount != len) {
	    _result.queryWrite = -errno;
	    isReallyComplete = true;
	    // To be safe, close file anyway.
	    qMaster::xrdClose(fd);
	} else {
	    _result.queryWrite = writeCount;
	    _queryHostPort = qMaster::xrdGetEndpoint(fd);
	    _resultUrl = qMaster::makeUrl(_queryHostPort.c_str(), "result", 
					  _hash);
	    qMaster::xrdClose(fd); 
	    _state = READ_OPEN;
	    std::cout  << "opening async read to " << _resultUrl << "\n";
	    int result = qMaster::xrdOpenAsync(_resultUrl.c_str(), 
					       O_RDONLY, this);
	    if(result != -EINPROGRESS) {
		_result.read = result;
		isReallyComplete = true;
	    }  // open for read in progress.
	} // Write ok
	    
	if(isReallyComplete) { 
	    _state=COMPLETE;
	    _notifyManager(); 
	}
}

void qMaster::ChunkQuery::_readResults(int fd) {
	int const fragmentSize = 4*1024*1024; // 4MB fragment size
	// Now read.
	qMaster::xrdReadToLocalFile(fd, fragmentSize, _spec.savePath.c_str(), 
			   &(_result.localWrite), &(_result.read));
	qMaster::xrdClose(fd);
	_state = COMPLETE;
	_notifyManager(); // This is a successful completion.
}
    
void qMaster::ChunkQuery::_notifyManager() {
	_manager->finalizeQuery(_id, _result);
}



//////////////////////////////////////////////////////////////////////
// TransactionSpec
//////////////////////////////////////////////////////////////////////
qMaster::TransactionSpec::Reader::Reader(std::string const& file) {
    _mmapFd = -1;
    _mmapChunk = 0;
    _rawContents = NULL;
    _setupMmap(file);
    //_readWholeFile(file);
    _pos = 0;
}
qMaster::TransactionSpec::Reader::~Reader() {
    if(_rawContents != NULL) delete[] _rawContents; // cleanup 
    _cleanupMmap();
}

void qMaster::TransactionSpec::Reader::_readWholeFile(std::string const& file) {
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

void qMaster::TransactionSpec::Reader::_setupMmap(std::string const& file) {
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

void qMaster::TransactionSpec::Reader::_advanceMmap() { 
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

void qMaster::TransactionSpec::Reader::_cleanupMmap() {
    if(_mmapChunk != NULL) {
	munmap(_mmapChunk, _mmapChunkSize);
    }
    if(_mmapFd != -1) {
	close(_mmapFd);
    }
}

qMaster::TransactionSpec qMaster::TransactionSpec::Reader::getSpec() {

    int beginPath;
    int endPath;
    int beginQuery;
    int endQuery;
    //int bufEnd = _rawContents;
    int bufEnd = _mmapChunkSize;;

    const int magicLength=4;
    qMaster::TransactionSpec ts;

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
    //    ts.path = std::string(_rawContents + beginPath, endPath - beginPath);
    //    ts.query = std::string(_rawContents + beginQuery, endQuery - beginQuery);
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

////////////////////////////////////////////////////////////
// AsyncQueryManager
////////////////////////////////////////////////////////////
int qMaster::AsyncQueryManager::add(TransactionSpec const& t, 
				    std::string const& resultName) {
    // For now, artificially limit the number of chunks in flight.
    int id = _getNextId();
    assert(id >= 0);
    if(t.isNull()) { return -1; }
    TransactionSpec ts(t);

    doctorQueryPath(ts.path);
    {
	boost::lock_guard<boost::mutex> lock(_queriesMutex);
	_queries[id] = QuerySpec(boost::make_shared<ChunkQuery>(ts, id, this),
				 resultName);
    }
    std::cout << "Added query id=" << id << " url=" << ts.path 
	      << " with save " << ts.savePath << "\n";
    _queries[id].first->run(); 
}

void qMaster::AsyncQueryManager::finalizeQuery(int id, 
					       XrdTransResult const& r) {
    QuerySpec& s = _queries[id];
    _merger->merge(s.first->getSavePath(), s.second);
    {
	boost::lock_guard<boost::mutex> lock(_queriesMutex);
	_queries.erase(id); //Warning! s is invalid now.
    }
    _results.push_back(Result(id,r));

}

void qMaster::AsyncQueryManager::joinEverything() {
    _printState(std::cout);
    while(!_queries.empty()) {
	std::cout << "Still " << _queries.size() << " in flight." << std::endl;
	_printState(std::cout);
	sleep(1); 
    }
    _merger->finalize();
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


void qMaster::AsyncQueryManager::_printState(std::ostream& os) {
    typedef std::map<int, boost::shared_ptr<ChunkQuery> > QueryMap;
    std::for_each(_queries.begin(), _queries.end(), printQueryMapValue(os));

}

////////////////////////////////////////////////////////////
// QueryManager
////////////////////////////////////////////////////////////

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
	boost::lock_guard<boost::mutex> lock(_waitingMutex);
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

boost::shared_ptr<qMaster::QueryManager::ManagedCallable> qMaster::QueryManager::_getNextCallable() {
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

int qMaster::QueryManager::_getNextId() {
    // FIXME(eventually) should track ids in use and recycle ids like pids.
    static int x = 0;
    static boost::mutex mutex;
    boost::lock_guard<boost::mutex> lock(mutex);
    return ++x;
}

void qMaster::QueryManager::_addThreadIfSpace() {
    {
        boost::lock_guard<boost::mutex> lock(_callablesMutex);
        if(_callables.size() >= _highWaterThreads) {
            // Don't add if there are already lots of callables in flight.
            return; 
        }
    }
    _tryJoinAll();
    {
        boost::lock_guard<boost::mutex> lock(_threadsMutex);
        if(_threads.size() < _highWaterThreads) {
            boost::shared_ptr<boost::thread> t = _startThread();
            if(t.get() != 0) {
                _threads.push_back(t);
            }
        }
    }
}

void qMaster::QueryManager::_tryJoinAll() {
    boost::lock_guard<boost::mutex> lock(_threadsMutex);
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
    time_t now;
    time_t last;
    while(1) {
	std::cout << "Threads left:" << _threads.size() << std::endl;
	time(&last);
	_tryJoinAll();
	time(&now);
	std::cout << "Joinloop took:" << now-last << std::endl;
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
    boost::lock_guard<boost::mutex> lock(_callablesMutex);
    _callables.insert(c);
    // FIXME: is there something else to do?
}

void qMaster::QueryManager::dropCallable(ManagedCallable* c) {
    boost::lock_guard<boost::mutex> lock(_callablesMutex);
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

