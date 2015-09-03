// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
 *
 *  @author: John Gates,
 */

#ifndef LSST_QSERV_UTIL_THREADSAFE_H_
#define LSST_QSERV_UTIL_THREADSAFE_H_

// system headers
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

// external headers

namespace lsst {
namespace qserv {
namespace util {

/** Provide a thread safe method for incrementing a sequence number.
 */
template <class T>
class Sequential {
public:
    explicit Sequential(T seq) : _seq(seq) {};
    // Returns the value before incrementing.
    T incr() {
        std::lock_guard<std::mutex> lock(_mutex);
        T val = _seq++;
        return val;
    }
    T get() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _seq;
    }
private:
    std::mutex _mutex;
    T _seq;
};

/** A flag that can be set/read safely across threads.
 */
template <class T>
class Flag {
public:
    explicit Flag(T flag) : _flag(flag) {};
    Flag& operator=(Flag&& other) = delete;
    Flag(const Flag&&) = delete;
    Flag& operator=(const Flag&) = delete;
    Flag(const Flag&) = delete;
    virtual ~Flag() {};

    /** Sets flag value to 'val' and returns the old value of flag.
     */
    virtual T set(T val) {
        std::lock_guard<std::recursive_mutex> lock(_mutex);
        auto oldVal = _flag;
        _flag = val;
        return oldVal;
    }
    T get() {
        std::lock_guard<std::recursive_mutex> lock(_mutex);
        return _flag;
    }

    std::recursive_mutex& getMutex() { return _mutex; }

protected:
    std::recursive_mutex _mutex;
    T _flag;
};

/** A flag that can be set safely across threads and can be
 * used to wake up threads waiting for a specific value.
 */
template <class T>
class FlagNotify {
public:
    explicit FlagNotify(T flag) : _flag(flag) {};
    virtual ~FlagNotify() {};
    /** Sets flag value to 'val' while notifying others of the change,
     * and returns the old value of flag.
     */
    virtual T set(T val) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto oldVal = _flag;
        _flag = val;
        _condition.notify_all();
        return oldVal;
    }
    void wait(T val) {
        std::unique_lock<std::mutex> lock(_mutex);
        while (_flag != val) {
            _condition.wait(lock);
        }
    }
protected:
    std::condition_variable _condition;
    std::mutex _mutex;
    T _flag;
};

}}} // namespace

#endif /* CORE_MODULES_UTIL_THREADSAFE_H_ */
