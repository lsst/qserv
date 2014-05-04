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

/// thread.h - A module for threading needs for qserv.  Initially, it
/// provided thread management for subquery threads.  Much of this is
/// no longer needed since we have switched to asynchronous
/// (non-blocking) usage of the Xrootd client libraries.
#ifndef LSST_QSERV_CONTROL_THREAD_H
#define LSST_QSERV_CONTROL_THREAD_H

// System headers
#include <map>
#include <set>

// Third-party headers
#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/make_shared.hpp"
#include "boost/thread.hpp"

// Local headers
#include "control/transaction.h"
#include "xrdc/xrdfile.h"

namespace lsst {
namespace qserv {
namespace control {

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

class Semaphore {  // Duplicated in xrdfs/MySqlFsFile.cc
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
    xrdc::XrdTransResult const& getResult() const { return _result; }
    void operator()();
private:
    TransactionSpec _spec;
    xrdc::XrdTransResult _result;
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
        void setResult(xrdc::XrdTransResult const& r);
        void getResult(xrdc::XrdTransResult const& r);
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
    xrdc::XrdTransResult const& status(int id) const;
    void joinEverything();


    ManagedCallable completeAndFetch(int id, xrdc::XrdTransResult const& r);
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
    typedef std::map<int, xrdc::XrdTransResult> ResultMap;

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

}}} // namespace lsst::qserv::control

#endif // LSST_QSERV_CONTROL_THREAD_H
