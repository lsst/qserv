
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
#include <mutex>
#include <thread>

// Qserv headers

// This header declarations

namespace lsst {
namespace qserv {
namespace util {

/**
 * Class Mutex extends the standard class std::mutex with an extra method
 * allowing to test if a mutex is locked by a calling thread.
 */
class Mutex
    :   public std::mutex {

public:

    /**
     * Lock the mutext (replaces the corresponidng method of the base class)
     */
    void lock() {
        std::mutex::lock();
        _holder = std::this_thread::get_id();
    }

    /**
     * Release the mutext (replaces the corresponidng method of the base class)
     */
    void unlock() {
        _holder = std::thread::id();
        std::mutex::unlock();
    }

    /**
     * @return true if the mutex is locked by the caller of this method
     */
    bool lockedByCaller() const {
        return _holder == std::this_thread::get_id();
    }

private:
    std::thread::id _holder;
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_MUTEX_H
