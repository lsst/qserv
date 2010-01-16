// -*- LSST-C++ -*-
#ifndef LSST_QSERV_MASTER_THREAD_H
#define LSST_QSERV_MASTER_THREAD_H
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#include <map>

namespace lsst {
namespace qserv {
namespace master {

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
    TransactionSpec const& getSpec() const { return _spec; }
    XrdTransResult const& getResult() const { return _result; }
    void operator()();
private:
    TransactionSpec _spec;
    XrdTransResult _result;
    static Semaphore _sema;
};
 
class Manager {
public:
    explicit Manager() : _highWaterThreads(120) {}
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

class QueryManager {
public:
    class ManagedCallable {
    public:
	explicit ManagedCallable(QueryManager& qm, int id, 
				 TransactionSpec const& t);
	void operator()();
    private:
	QueryManager& _qm;
	int _id;
	TransactionCallable _c;
    };
    typedef std::pair<int, ManagedCallable> IdCallable;


    explicit QueryManager() : _highWaterThreads(120) {}

    int add(TransactionSpec const& t, int id=-1);
    void join(int id);
    bool tryJoin(int id);
    XrdTransResult const& status(int id) const;

    ManagedCallable completeAndFetch(int id, XrdTransResult const& r);
    void addThread(ManagedCallable* c);
    void dropThread(ManagedCallable* c);
private:
    int _getNextId();
    void _addThreadIfSpace();
    
    typedef std::deque<boost::shared_ptr<boost::thread> > ThreadDeque;
    typedef std::deque<IdCallable> CallableDeque;
    typedef std::map<int, ManagedCallable> CallableMap;

    ThreadDeque _threads;
    int _highWaterThreads;
    boost::mutex _waitingMutex;
    boost::mutex _runningMutex;
    boost::mutex _finishedMutex;
    CallableDeque _waiting;
    CallableMap _running;
    CallableMap _finished;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_THREAD_H
