// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 */

#ifndef LSST_QSERV_UTIL_THREADPRIORITY_H_
#define LSST_QSERV_UTIL_THREADPRIORITY_H_

// system headers
#include "pthread.h"

// external headers


namespace lsst {
namespace qserv {
namespace util {


/// Class used to temporarily change the priority of the thread it is on.
/// It requires root privileges for this class to change anything.
class ThreadPriority {
public:

    explicit ThreadPriority(pthread_t const& pthreadH) : _pthreadHandle(pthreadH) {}

    ~ThreadPriority() = default;

    void storeOriginalValues();

    /// Use pthread library to discover current priority and policy.
    void getCurrentValues(int &priority, int &policy);
    void getCurrentValues(sched_param &sch, int &policy);

    /// If newPriority is non-zero, the policy must be a real-time policy, like SCHED_FIFO.
    int setPriorityPolicy(int newPriority, int newPolicy=SCHED_FIFO);

    int restoreOriginalValues() {
        return setPriorityPolicy(_originalPriority, _originalPolicy);
    }

private:
    pthread_t _pthreadHandle;         ///< Thread that has its priority changed.
    int _originalPriority{0};         ///< Standard priority for all non real-time policies
    int _originalPolicy{SCHED_OTHER}; ///< Normal scheduling policy.
};

}}} // namespace

#endif /* CORE_MODULES_UTIL_THREADPRIORITY_H_ */
