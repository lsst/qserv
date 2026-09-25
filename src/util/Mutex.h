
/*
 * LSST Data Management System
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

/// Complicated preprocessor logic to allow VMutex to be enabled for unit testing,
/// and disabled for production, while not producing compiler warnings.
#ifdef NOT_USING_VMUTEX
#define USING_VMUTEX_VAL 0
#else
#define USING_VMUTEX_VAL 1
#endif

#ifdef MUTEX_UNITTEST
#undef USING_VMUTEX
#define USING_VMUTEX 1
#else
#define USING_VMUTEX USING_VMUTEX_VAL
#endif

#if USING_VMUTEX != 0  // Declarations to use VMutex.

#define VMUTEX util::VMutex
#define MUTEX util::Mutex

namespace lsst::qserv::util {
class VMutex;
}  // namespace lsst::qserv::util

/// Used to verify a mutex is locked before accessing a protected variable.
#define VMUTEX_HELD(vmtx) \
    if (!vmtx.lockedByThread()) throw lsst::qserv::util::VMtxException(ERR_LOC, vmtx, "not locked!");

/// Used to verify a mutex is not locked by this thread before locking a related mutex.
#define VMUTEX_NOT_HELD(vmtx) \
    if (vmtx.lockedByThread()) throw lsst::qserv::util::VMtxException(ERR_LOC, vmtx, "not held!");

/** Define a lock_guard for a VMutex, verifying that the VMutex is not already locked
 * by this thread. This also helps avoid "lock_guard (vmtx);" which compiles but
 * does not hold the mutex past that statement.
 */
#define VLOCK(lName, vmtx)                                                                               \
    if (vmtx.lockedByThread()) throw lsst::qserv::util::VMtxException(ERR_LOC, vmtx, "already locked!"); \
    std::lock_guard<lsst::qserv::util::VMutex> const lName(vmtx);                                        \
    vmtx.setTag(__func__, #lName);

/** Define a unique_lock for a VMutex, verifying that the VMutex is not already locked
 *  by this thread.
 */
#define VLOCKUNIQUE(lName, vmtx)                                                                         \
    if (vmtx.lockedByThread()) throw lsst::qserv::util::VMtxException(ERR_LOC, vmtx, "already locked!"); \
    std::unique_lock<lsst::qserv::util::VMutex> lName(vmtx);                                             \
    vmtx.setTag(__func__, #lName);

/** Using references here should help avoid using these where VLOCK and
 *  VLOCKUNIQUE should be used instead, as there should be compiler warnings/errors
 *  for undefined references.
 *  Only for use when a VLOCK is in a function parameter list.
 */
typedef std::lock_guard<lsst::qserv::util::VMutex> const& VLOCKPARAM;

/**  Only for use when a VLOCKUNIQUE is in a function parameter list.
 *  This needs to be able to call lock and unlock, so it's not const.
 */
typedef std::unique_lock<lsst::qserv::util::VMutex>& VLOCKUNIQPARAM;

#else  // not USING_VMUTEX, declartions to use std::mutex.

#define VMUTEX std::mutex
#define MUTEX std::mutex

#define VMUTEX_HELD(vmtx) ;

#define VMUTEX_NOT_HELD(vmtx) ;

#define VLOCK(lName, vmtx) std::lock_guard const lName(vmtx);

#define VLOCKUNIQUE(lName, vmtx) std::unique_lock lName(vmtx);

/// Only for use when a VLOCK is in a function parameter list.
typedef std::lock_guard<std::mutex> const& VLOCKPARAM;
/// Only for use when a VLOCKUNIQUE is in a function parameter list.
typedef std::unique_lock<std::mutex>& VLOCKUNIQPARAM;

#endif  // USING_VMUTEX

namespace lsst::qserv::util {

/** This class implements a verifiable mutex based on std::mutex. It can be used with the
 *  VMUTEX_HELD, VMUTEX_NOT_HELD, VLOCK, and VLOCKUNIQUE macros.
 *  NOTE: All of the following are important:
 *   - For it to work properly, VLOCK and VLOCKUNIQUE must be used consistently to lock the mutex.
 *   - All VMUTEX definitions should include a list of variables they protect.
 *     - All variables protected by a VMUTEX should have VMUTEX_HELD, VLOCK, or VLOCKUNIQUE before
 *       they are used in a function (except for constructor/destructor members) using the correct mutex.
 *   - std::wait() and its kin will not call setTag(), but the calls will append '~', '!`, and `#`
 *     to the tag in the error message.
 *   - '~' in the tag means any lock could own the mutex.
 *   - '?' in the tag means the mutex was not locked using VLOCK or VLOCKUNIQUE.
 *  Making VMutex a wrapper around std::mutex instead of a child causes lines
 *  like `std::lock_guard<std::mutex> lck(_vmutex);` to be flagged as errors,
 *  which can be desirable.
 *  VMutex does work with std::condition_variable_any.
 */
class VMutex {
public:
    VMutex() {}
    VMutex(VMutex const&) = delete;
    ~VMutex() = default;

    /** Lock the mutex (replaces the corresponding method of the base class)
     *  This should always be called using the VLOCK or VLOCKUNIQUE macros,
     *  which will call setTag() to set the tag for this mutex.
     */
    void lock() {
        _mutex.lock();
        _holder = std::this_thread::get_id();
        // Note that wait() can lock and unlock the mutex and setTag() will not be called.
        _tag += "!";
    }

    /** Release the mutex (replaces the corresponding method of the base class) */
    void unlock() {
        _holder = std::thread::id();
        // Note that wait() can lock and unlock the mutex and setTag() will not be called.
        _tag += "~";
        _mutex.unlock();
    }

    bool try_lock() {
        bool res = _mutex.try_lock();
        if (res) {
            _holder = std::this_thread::get_id();
            _tag = "#";
        }
        return res;
    }

    /** Return true if the mutex is locked by this thread. */
    bool lockedByThread() const { return _holder == std::this_thread::get_id(); }

    /** This should only be called when _mutex is locked. */
    void setTag(std::string const& funcName, std::string const& lockName) {
        _tag = funcName + ":" + lockName;
    }

    /** Return the tag for this mutex, note that '?', '~', '#', and '!' can have
     *  special meaning. wait() can lock and unlock the mutex and setTag() will
     *  not be called, but the special characters will be appended to the tag.
     *  Note: Calling this while _mutex is not locked results in "_mutex not held"
     *  being returned and the contents of _tag are probably completely irrelevant
     *  as what is really needed is a stack trace.
     *  This also makes _tag thread safe, as it is only accessed when _mutex is
     *  locked by the calling thread.
     */
    std::string getTag() const {
        if (std::this_thread::get_id() == _holder) {
            return _tag;
        }
        return "_mutex not held";
    }

protected:
    /** The thread that currently holds the lock. std::thread::id() indicates no thread holds the lock.
     */
    std::atomic<std::thread::id> _holder{std::thread::id()};

private:
    /** While functioning as a mutex for the caller, this also protects the members of this class. */
    std::mutex _mutex;
    std::string _tag{"?"};  ///< Optional for debugging.
};

class VMtxException : public util::Issue {
public:
    explicit VMtxException(util::Issue::Context const& ctx, VMutex const& vmtx, std::string const& msg);
};

/**
 * Class Mutex extends the standard class std::mutex with extra methods.
 * Note: This class adds a second mutex lock and an O log n operation
 *       to the lock/unlock methods. n can be large.
 */
class Mutex : public VMutex {
public:
    /// @return identifiers of locked mutexes
    static std::set<uint64_t> lockedId() {
        // make a consistent snapshot of the collection to be returned
        std::set<uint64_t> result;
        std::lock_guard<std::mutex> lg(_lockedIdMtx);
        result = _lockedId;
        return result;
    }

    Mutex() : _id(nextId()) {}

    /// Lock the mutex (replaces the corresponding method of the base class)
    void lock() {
        VMutex::lock();
        addCurrentId();
    }

    /// Release the mutex (replaces the corresponding method of the base class)
    void unlock() {
        removeCurrentId();
        VMutex::unlock();
    }

    bool try_lock() {
        bool res = VMutex::try_lock();
        if (res) {
            addCurrentId();
        }
        return res;
    }

    /// @return unique identifier of a lock
    uint64_t id() const { return _id; }

private:
    /// @return next identifier in a global series
    static uint64_t nextId() {
        static std::atomic<uint64_t> id{0};
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
    static std::set<uint64_t> _lockedId;

    uint64_t _id;  ///< This could get very large. Wrapping could cause false positives.
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
