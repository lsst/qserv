
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_MUTEX_H
#define LSST_QSERV_UTIL_MUTEX_H

// System headers
#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "util/Bug.h"

/// Used to verify a mutex is locked before accessing a protected variable
#define VMUTEX_HELD(vmtx) \
    if (!vmtx.lockedByCaller()) throw lsst::qserv::util::Bug(ERR_LOC, "mutex not locked!");

/// Used to verify a mutex is unlocked before locking a related mutex.
#define VMUTEX_FREE(vmtx) \
    if (vmtx.lockedByCaller()) throw lsst::qserv::util::Bug(ERR_LOC, "mutex not free!");

// This header declarations
namespace lsst::qserv::util {

/// This class implements a verifiable mutex based on std::mutex. It can be used with the
/// VMUTEX_HELD and VMUTEX_FREE macros.
class VMutex : public std::mutex {
public:
    explicit VMutex() {}

    /// Lock the mutex (replaces the corresponding method of the base class)
    void lock() {
        std::mutex::lock();
        _holder = std::this_thread::get_id();
    }

    /// Release the mutex (replaces the corresponding method of the base class)
    void unlock() {
        _holder = std::thread::id();
        std::mutex::unlock();
    }

    /// @return true if the mutex is locked by this thread.
    /// TODO: Rename lockedByThread()
    bool lockedByCaller() const { return _holder == std::this_thread::get_id(); }

protected:
    std::atomic<std::thread::id> _holder;
};

/**
 * Class Mutex extends the standard class std::mutex with extra methods.
 */
class Mutex : public VMutex {
public:
    /// @return identifiers of locked mutexes
    static std::set<unsigned int> lockedId() {
        // make a consistent snapshot of the collection to be returned
        std::set<unsigned int> result;
        std::lock_guard<std::mutex> lg(_lockedIdMtx);
        result = _lockedId;
        return result;
    }

    Mutex() : _id(nextId()) {}

    /// Lock the mutext (replaces the corresponding method of the base class)
    void lock() {
        VMutex::lock();
        addCurrentId();
    }

    /// Release the mutext (replaces the corresponding method of the base class)
    void unlock() {
        removeCurrentId();
        VMutex::unlock();
    }

    /// @return unique identifier of a lock
    unsigned int id() const { return _id; }

private:
    /// @return next identifier in a global series
    static unsigned int nextId() {
        static std::atomic<unsigned int> id{0};
        return id++;
    }

    /// Register the current identifier in a collection of locked mutexes
    void addCurrentId() {
        std::lock_guard<std::mutex> lg(_lockedIdMtx);
        _lockedId.insert(_id);
    }

    /// De-register the current identifier from a collection of locked mutexes
    void removeCurrentId() {
        std::lock_guard<std::mutex> lg(_lockedIdMtx);
        _lockedId.erase(_id);
    }

private:
    static std::mutex _lockedIdMtx;
    static std::set<unsigned int> _lockedId;

    unsigned int _id;
};

/**
 * Class Lock is designed to completement the above defined class Mutex.
 * The current implementation of the class is very similar to std::lock_guard.
 * In addition Lock would also print out 3 debug messages into the log stream when
 * a state transition ocurrs:
 *
 * - before the lock is acquired
 * - right after it's acquired
 * - and before it gets released (when the lock is being destroyed)
 *
 * The lock will assert that no lock is being held on a mutex by the calling
 * thread before attempting to lock the mutex.
 */
class Lock {
public:
    /**
     * Lock the mutex given by a plain reference.
     * @param mutex A mutex object to be locked.
     * @param context A context in which the lock is acquired.
     */
    explicit Lock(Mutex& mutex, std::string const& context = std::string())
            : _mutex(mutex), _context(context) {
        _lock();
    }

    /**
     * Lock the mutex given by a shared pointer.
     * @note A local copy of the shared pointer will be owned by the lock.
     *   A local reference to the Mutex will be reffering an object pointed
     *   to by the pointer to allow unifid inner implementation of
     *   the locking/unlocking algorithms. See the code for further details.
     * @param mutex A mutex object to be locked.
     * @param context A context in which the lock is acquired.
     */
    explicit Lock(std::shared_ptr<Mutex> const& mutexPtr, std::string const& context = std::string())
            : _mutexPtr(mutexPtr), _mutex(*mutexPtr), _context(context) {
        _lock();
    }

    Lock() = delete;
    Lock(Lock const&) = delete;
    Lock& operator=(Lock const&) = delete;

    ~Lock() { _unlock(); }

private:
    void _lock();
    void _unlock();

    std::shared_ptr<Mutex> const _mutexPtr;
    Mutex& _mutex;
    std::string _context;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_MUTEX_H
