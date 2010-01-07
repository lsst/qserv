
// A Driver program to replay qserv xrootd query transactions.

#include <deque>
#include <fstream>
#include <iostream>
#include "lsst/qserv/master/xrdfile.h"
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

using boost::make_shared;


template<class T> struct joinBoostThread  {
    joinBoostThread() {} 
    void operator() (T x) { x->join(); }
};

template<class T> struct tryJoinBoostThread  {
    tryJoinBoostThread() {} 
    bool operator()(T x) { 
	using boost::posix_time::seconds;
	return x->timed_join(seconds(0)); 
    }
};


class Semaphore {  // Duplicated in lsst/qserv/worker/src/MySqlFsFile.cc
public:
    explicit Semaphore(int count=1) : _count(count) {
	assert(count > 0);
    }

    void proberen() {
	// Lock the count variable
	boost::unique_lock<boost::mutex> lock(_countLock);
	while(_count <= 0) {
	//     sleep (release lock) until wakeup
	    _countCondition.wait(lock);
	}
	assert(_count > 0);
	--_count;
	// Lock is unlocked when we leave scope.
    }

    void verhogen() {
	{
	    // Lock the count variable.
	    boost::lock_guard<boost::mutex> lock(_countLock);
	    ++_count;
	}
	// Wake up one of the waiters.
	_countCondition.notify_one();
	
    }

    inline void get() { proberen(); }
    inline void release() { verhogen(); }
private:
    boost::mutex _countLock;
    boost::condition_variable _countCondition;
    int _count;
};

class TransactionSpec {

public:
    std::string path;
    std::string query;
    
    bool isNull() { return path.length() == 0; }

    class Reader {
    public:
	Reader(std::string const& inFile);
	~Reader() { delete[] _rawContents; }
	TransactionSpec getSpec();
    private:
	char* _rawContents;
	int _rawLength;
	int _pos;
    };    
};

class TransactionCallable {
public:
    TransactionCallable(TransactionSpec s) : _spec(s) {}

    void operator()();
private:
    TransactionSpec _spec;
    static Semaphore _sema;
};

// for now, two simultaneous writes (queries)
Semaphore TransactionCallable::_sema(120);

class ClosingCallable {
public:
    ClosingCallable(int fd) : _fd(fd) {}

    void operator()();

    static void addOne(int fd) {
	boost::unique_lock<boost::mutex> lock(_groupLock);
	_closers.create_thread(ClosingCallable(fd));
    }
    static void joinAll() {
	boost::unique_lock<boost::mutex> lock(_groupLock);
	_closers.join_all();
    }

private:
    int _fd;
    static boost::mutex _groupLock;
    static boost::thread_group _closers;
    
};



class Manager {
public:
    Manager() : _highWaterThreads(120) {}
    void setupFile(std::string const& file);
    void run();
private:
    void _joinOne(); 

    std::string _file;
    boost::shared_ptr<TransactionSpec::Reader> _reader;
    typedef std::deque<boost::shared_ptr<boost::thread> > ThreadDeque;
    ThreadDeque _threads;
    int _highWaterThreads;
};

//////////////////////////////////////////////////////////////////////
// TransactionSpec
//////////////////////////////////////////////////////////////////////
TransactionSpec::Reader::Reader(std::string const& file) {
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


TransactionSpec TransactionSpec::Reader::getSpec() {

    int beginPath;
    int endPath;
    int beginQuery;
    int endQuery;
    const int magicLength=4;
    TransactionSpec ts;

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
    
    _pos = endQuery + magicLength; // Advance past the detected marker.
    return ts; // FIXME placeholder
}
//////////////////////////////////////////////////////////////////////
// TransactionCallable
//////////////////////////////////////////////////////////////////////
void TransactionCallable::operator()() {
    using namespace lsst::qserv::master;
    std::cout << _spec.path << " in flight\n";
    XrdTransResult r = xrdOpenWriteReadSaveClose(_spec.path.c_str(),
					    _spec.query.c_str(),
					    _spec.query.length(),
					    8192000,
					    "/dev/null");
#if 0
    if(r.open > 0) { // Spawn a separate thread to close.
	ClosingCallable t(s);
	_threads.push_back(make_shared<boost::thread>(t));
	
    }
#endif
    std::cout << _spec.path << " finished\n";

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


int main(int const argc, char const* argv[]) {
    Manager m;
    std::cout << "Setting up file\n";
    m.setupFile("xrdTransaction.trace");
    std::cout << "Running\n";
    m.run();
    
    return 0;
}
