
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
    std::thread::id _holder;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_MUTEX_H
