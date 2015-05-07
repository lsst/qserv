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

#ifndef LSST_QSERV_UTIL_THREAD_H
#define LSST_QSERV_UTIL_THREAD_H

// System headers
#include <set>

// Third-party headers
#include "XrdSys/XrdSysPthread.hh"

#ifdef DO_NOT_USE_BOOST
#else
#include <mutex>
#endif

namespace lsst {
namespace qserv {
namespace util {

#ifdef DO_NOT_USE_BOOST
/// xrootd-dependent unique_lock
class UniqueLock { // scoped lock.
public:
    explicit UniqueLock(XrdSysMutex& m) : _m(m) {
	_m.Lock();
    }
    ~UniqueLock() { _m.UnLock(); }
private:
    XrdSysMutex& _m;
};

/// An xrootd-dependent semaphore wrapper
class Semaphore {
public:
    explicit Semaphore(int count=0) : _sema(count) {}
    inline void proberen() { _sema.Wait(); }
    inline void verhogen() { _sema.Post(); }
    inline void get() { proberen(); }
    inline void release() { verhogen(); }
private:
    XrdSysSemaphore _sema;
};

#else

class Semaphore {
public:
    explicit Semaphore(int count=1) : _count(count) {
        assert(count > 0);
    }

    void proberen() {
        // Lock the count variable
        std::unique_lock<std::mutex> lock(_countLock);
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
            std::lock_guard<std::mutex> lock(_countLock);
            ++_count;
        }
        // Wake up one of the waiters.
        _countCondition.notify_one();
    }

    inline void get() { proberen(); }
    inline void release() { verhogen(); }
private:
    std::mutex _countLock;
    std::condition_variable _countCondition;
    int _count;
};

#endif

/// An xrootd-dependent thread library that
/// roughly follows std::thread semantics.
class ThreadDetail {
public:
    virtual ~ThreadDetail() {}
    virtual void run() = 0;
    virtual void * (*function())(void *) = 0;

    void setTid(pthread_t t) {
        _tid = t;
    }
protected:
    pthread_t _tid;
};


class ThreadManager {
public:
    typedef std::set<ThreadDetail*> DetailSet;
#if 0
    template <typename Callable>
    static ThreadDetail* newTrackedDetail(Callable& c) {
        ThreadDetail* td = new ThreadDetailSpecific<Callable>(c);
        _detailMutex.Lock();
        _details.insert(td);
        _detailMutex.UnLock();
        return td;
    }
#endif
    static void takeControl(ThreadDetail* td) {
        _detailMutex.Lock();
        _details.insert(td);
        _detailMutex.UnLock();
    }

    static void forgetDetail(ThreadDetail* td) {
        _detailMutex.Lock();
        _details.erase(td);
        delete td;
        _detailMutex.UnLock();
    }
private:
    static XrdSysMutex _detailMutex;
    static DetailSet _details;
};
template <typename Callable>
void* invokeCallableDetail(void* threadDetail) {
    ThreadDetail* td = reinterpret_cast<ThreadDetail*>(threadDetail);
    td->run();
    ThreadManager::forgetDetail(td);
    return 0; // Placeholder--callables return null.
}

template <typename Callable>
class ThreadDetailSpecific : public ThreadDetail {
public:
    ThreadDetailSpecific(Callable const& c) : _c(new Callable(c)) {}
    virtual ~ThreadDetailSpecific() {
        delete _c;
    }
    virtual void run() {
        (*_c)();
    }
    virtual void * (*function())(void *) {
        return invokeCallableDetail<Callable>;
    }
private:
    Callable* _c;
};
template <typename Callable>
ThreadDetail* newDetail(Callable const& c) {
    return new ThreadDetailSpecific<Callable>(c);
}


class Thread {
public:
    explicit Thread(ThreadDetail* td) {
        _detail = td; //ThreadManager::newTrackedDetail<Callable>(c);
        pthread_t newTid;
        XrdSysThread::Run(&newTid, _detail->function(), _detail);
        _detail->setTid(newTid);
    }

private:
    ThreadDetail* _detail;
};

}}} // namespace lsst.qserv.util

#endif // LSST_QSERV_UTIL_THREAD_H
