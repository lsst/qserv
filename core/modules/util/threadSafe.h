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

// external headers
#include "boost/thread.hpp"

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
        boost::lock_guard<boost::mutex> lock(_mutex);
        T val = _seq++;
        return val;
    }
    T get() {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _seq;
    }
private:
    boost::mutex _mutex;
    T _seq;
};

/** A flag that can be set/read safely across threads.
 */
template <class T>
class Flag {
public:
    explicit Flag(T flag) : _flag(flag) {};
    virtual ~Flag() {};
    virtual void set(T val) {
        boost::lock_guard<boost::mutex> lock(_mutex);
        _flag = val;
    }
    T get() {
        boost::lock_guard<boost::mutex> lock(_mutex);
        return _flag;
    }
protected:
    boost::mutex _mutex;
    T _flag;
};

/** A flag that can be set safely across threads and can be
 * used to wake up threads waiting for a specific value.
 */
template <class T>
class FlagNotify : public Flag<T> {
public:
    explicit FlagNotify(T flag) : Flag<T>(flag) {};
    virtual ~FlagNotify() {};
    virtual void set(T val) {
        boost::lock_guard<boost::mutex> lock(Flag<T>::_mutex);
        Flag<T>::_flag = val;
        _condition.notify_all();
    }
    void wait(T val) {
        boost::unique_lock<boost::mutex> lock(Flag<T>::_mutex);
        while (Flag<T>::_flag != val) {
            _condition.wait(lock);
        }
    }
protected:
    boost::condition_variable _condition;
};

}}} // namespace

#endif /* CORE_MODULES_UTIL_THREADSAFE_H_ */
