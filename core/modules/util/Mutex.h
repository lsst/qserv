
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

/// Mutex.h declares:
///
///   class Mutex
///
/// (see individual class documentation for more information)

// System headers
#include <atomic>
#include <mutex>
#include <set>
#include <string>
#include <thread>

// Qserv headers

// This header declarations

namespace lsst {
namespace qserv {
namespace util {

/**
 * Class Mutex extends the standard class std::mutex with extra methods.
 */
class Mutex
    :   public std::mutex {

public:

    /// @return identifiers of locked mutexes
    static std::set<unsigned int> lockedId() {
        // make a consistent snapshot of the collection to be returned
        std::set<unsigned int> result;
        std::lock_guard<std::mutex> lg(_lockedIdMtx);
        result = _lockedId;
        return result;
    }

    /// Constructor
    Mutex()
        :   _id(nextId()) {
    }

    /**
     * Lock the mutext (replaces the corresponidng method of the base class)
     */
    void lock() {
        std::mutex::lock();
        _holder = std::this_thread::get_id();
        addCurrentId();
    }

    /**
     * Release the mutext (replaces the corresponidng method of the base class)
     */
    void unlock() {
        removeCurrentId();
        _holder = std::thread::id();
        std::mutex::unlock();
    }

    /// @return unique identifier of a lock
    unsigned int id() const { return _id; }

    /**
     * @return true if the mutex is locked by the caller of this method
     */
    bool lockedByCaller() const {
        return _holder == std::this_thread::get_id();
    }

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
    std::atomic<std::thread::id> _holder;
};


/// Type definition for the lock type
//typedef std::lock_guard<Mutex> Lock;

/**
 * Acquire a lock on a mutex and return the lock object (move semantics)
 * The method will also log the locking attempt into the coresponding
 * logging stream if an optional logging context is provided.
 *
 * @param mutex   - mutex object to be locked
 * @param context - context in which the lock was requested (used for logging purposes)
 * @return lock object
 */
//Lock&& lock(Mutex& mutex, std::string const& context=std::string());


/**
 * Class Lock is designed to completement the above defined class Mutex.
 * The current implementation of the class is very similar to std::lock_guard.
 * In addition Lock would also print out 3 debug messages into the log stream when
 * a state transition occures:
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
     * The only form of object construction which is available
     *
     * @param mutex   - object to be locked
     * @param context - context in which the lock is acquired
     */
    explicit Lock(Mutex& mutex, std::string const& context=std::string());

    // Default construction and any fors of copy semantics are prohited

    Lock() = delete;
    Lock(Lock const&) = delete;
    Lock& operator=(Lock const&) = delete;

    ~Lock();

private:
    Mutex& _mutex;
    std::string _context;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_MUTEX_H
