// -*- LSST-C++ -*-
#ifndef LSST_QSERV_MASTER_THREAD_H
#define LSST_QSERV_MASTER_THREAD_H
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#include "XrdPosix/XrdPosixCallBack.hh"
#include <map>
#include <set>

namespace lsst {
namespace qserv {
namespace master {

class AsyncQueryManager; // Forward

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
    int bufferSize;
    std::string savePath;
    
    bool isNull() const { return path.length() == 0; }

    class Reader {
    public:
	Reader(std::string const& inFile);
	~Reader();
	TransactionSpec getSpec();
    private:
	void _readWholeFile(std::string const& inFile);
	void _setupMmap(std::string const& inFile);
	void _cleanupMmap();
	void _advanceMmap();

	char* _rawContents;
	char* _mmapChunk;
	int _mmapFd;
	int _mmapOffset;
	int _mmapChunkSize;
	int _mmapDefaultSize;
	int _mmapMinimum;
	int _rawLength;
	int _pos;
    };    
};

class TransactionCallable {
public:
    TransactionCallable(TransactionSpec s) : _spec(s) {}
    TransactionSpec const& getSpec() const { return _spec; }
    XrdTransResult const& getResult() const { return _result; }
    void operator()();
private:
    TransactionSpec _spec;
    XrdTransResult _result;
    static Semaphore _sema;
};
 
class Manager {
public:    explicit Manager() : _highWaterThreads(120) {}
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
// class ChunkQuery 
// Handles chunk query execution, like openwritereadsaveclose, but
// with dual asynchronous opening.  Should lessen need for separate
// threads.  Not sure if it will be enough though.
// 
//////////////////////////////////////////////////////////////////////
class ChunkQuery : public XrdPosixCallBack {
public:
    enum WaitState {WRITE_OPEN, WRITE_WRITE, 
		    READ_OPEN, READ_READ,
		    COMPLETE, CORRUPT};
    
    virtual void Complete(int Result);
    explicit ChunkQuery(TransactionSpec const& t, int id,
			AsyncQueryManager* mgr);
    virtual ~ChunkQuery() {}

    void run();
    XrdTransResult const& results() const { return _result; }
    std::string getDesc() const;
private:
    void _sendQuery(int fd);
    void _readResults(int fd);
    void _notifyManager();

    int _id;
    TransactionSpec _spec;
    WaitState _state;
    XrdTransResult _result;
    boost::mutex _mutex;
    std::string _hash;
    std::string _resultUrl;
    std::string _queryHostPort;
    AsyncQueryManager* _manager;
};// class ChunkQuery


class AsyncQueryManager {
public:
    explicit AsyncQueryManager() :_lastId(0) {}

    int add(TransactionSpec const& t);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;
    void joinEverything();

    void finalizeQuery(int id,  XrdTransResult const& r);
private:
    typedef std::map<int, boost::shared_ptr<ChunkQuery> > QueryMap;
    class printQueryMapValue {
    public:
	printQueryMapValue(std::ostream& os_) : os(os_) {}
	void operator()(QueryMap::value_type const& qv) {
	    os << "Query with id=" << qv.first;
	    os << ": " << qv.second->getDesc() << std::endl;
	}
	std::ostream& os;
    };

    int _getNextId() {return ++_lastId;}
    void _printState(std::ostream& os);


    boost::mutex _queriesMutex;
    int _lastId;
    QueryMap _queries;
};

class QueryManager {
public:
    /// A callable object that performs a (chunk-query) transaction according
    /// to its specification, and reports its completion to a query 
    /// manager.  Restarts with new transaction if available.
    class ManagedCallable {
    public:
        explicit ManagedCallable();
	explicit ManagedCallable(QueryManager& qm, int id, 
				 TransactionSpec const& t);
        ManagedCallable& operator=(ManagedCallable const& m);

	void operator()();
        void setResult(XrdTransResult const& r);
        void getResult(XrdTransResult const& r);
        int getId() const { return _id;}
    private:
	QueryManager* _qm;
	int _id;
	TransactionCallable _c;
    };
    typedef std::pair<int, ManagedCallable> IdCallable;


    explicit QueryManager() : _highWaterThreads(120) {}

    int add(TransactionSpec const& t, int id=-1);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;
    void joinEverything();


    ManagedCallable completeAndFetch(int id, XrdTransResult const& r);
    void addCallable(ManagedCallable* c);
    void dropCallable(ManagedCallable* c);
private:
    int _getNextId();
    void _addThreadIfSpace();
    boost::shared_ptr<ManagedCallable> _getNextCallable();

    boost::shared_ptr<boost::thread> _startThread();
    void _tryJoinAll();

    
    typedef std::deque<boost::shared_ptr<boost::thread> > ThreadDeque;
    typedef std::deque<IdCallable> CallableDeque;
    typedef std::map<int, ManagedCallable> CallableMap;
    typedef std::map<int, XrdTransResult> ResultMap;

    ThreadDeque _threads;
    int _highWaterThreads;
    boost::mutex _threadsMutex;
    boost::mutex _callablesMutex;
    boost::mutex _waitingMutex;
    boost::mutex _runningMutex;
    boost::mutex _finishedMutex;
    CallableDeque _waiting;
    CallableMap _running;
    ResultMap _finished;
    std::set<ManagedCallable*> _callables;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_THREAD_H
