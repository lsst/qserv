// -*- LSST-C++ -*-
#ifndef LSST_QSERV_MASTER_THREAD_H
#define LSST_QSERV_MASTER_THREAD_H

// Standard
#include <map>
#include <set>

// Boost
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

// Scalla/xrootd
#include "XrdPosix/XrdPosixCallBack.hh"

// Package
#include "lsst/qserv/master/transaction.h"

namespace lsst {
namespace qserv {
namespace master {

// Forward
class AsyncQueryManager; 
class TableMerger;
class TableMergerConfig;


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


class TransactionSpec::Reader {
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
		    COMPLETE, CORRUPT, ABORTED};
    
    virtual void Complete(int Result);
    explicit ChunkQuery(TransactionSpec const& t, int id,
			AsyncQueryManager* mgr);
    virtual ~ChunkQuery() {}

    void run();
    XrdTransResult const& results() const { return _result; }
    std::string getDesc() const;
    std::string const& getSavePath() const { return _spec.savePath; }
    void requestSquash() { _shouldSquash = true; }

private:
    void _sendQuery(int fd);
    void _readResults(int fd);
    void _notifyManager();
    void _squashAtCallback(int result);

    int _id;
    TransactionSpec _spec;
    WaitState _state;
    XrdTransResult _result;
    boost::mutex _mutex;
    std::string _hash;
    std::string _resultUrl;
    std::string _queryHostPort;
    AsyncQueryManager* _manager;
    bool _shouldSquash;
};// class ChunkQuery


//////////////////////////////////////////////////////////////////////
// class AsyncQueryManager 
// Babysits a related set of queries.  Issues asynchronously handles 
// preparation, status-checking, and post-processing (if a merger has 
// been configured).
// 
//////////////////////////////////////////////////////////////////////
class AsyncQueryManager {
public:
    typedef std::pair<int, XrdTransResult> Result;
    typedef std::deque<Result> ResultDeque;
    typedef boost::shared_ptr<AsyncQueryManager> Ptr;
    
    explicit AsyncQueryManager() :_lastId(0), _isExecFaulty(false) {}
    void configureMerger(TableMergerConfig const& c);

    int add(TransactionSpec const& t, std::string const& resultName);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;
    void joinEverything();
    ResultDeque const& getFinalState() { return _results; }
    void finalizeQuery(int id,  XrdTransResult r, bool aborted); 
    std::string getMergeResultName() const;
    
private:
    typedef std::pair<boost::shared_ptr<ChunkQuery>, std::string> QuerySpec;
    typedef std::map<int, QuerySpec> QueryMap;

    class printQueryMapValue {
    public:
	printQueryMapValue(std::ostream& os_) : os(os_) {}
	void operator()(QueryMap::value_type const& qv) {
	    os << "Query with id=" << qv.first;
	    os << ": " << qv.second.first->getDesc() 
	       << ", " << qv.second.second << std::endl;
	}
	std::ostream& os;
    };

    class squashQuery {
    public:
        squashQuery() {}
        void operator()(QueryMap::value_type const& qv) {
            boost::shared_ptr<ChunkQuery> cq = qv.second.first;
            cq->requestSquash();
        }
    };


    int _getNextId() {
	boost::lock_guard<boost::mutex> m(_idMutex); 
	return ++_lastId;
    }
    void _printState(std::ostream& os);
    void _squashExecution();

    boost::mutex _idMutex;
    boost::mutex _queriesMutex;
    boost::mutex _resultsMutex;
    int _lastId;
    bool _isExecFaulty;
    int _squashCount;
    QueryMap _queries;
    ResultDeque _results;
    boost::shared_ptr<TableMerger> _merger;
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
