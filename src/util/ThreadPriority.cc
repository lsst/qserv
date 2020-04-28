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
 *  @author: John Gates,
 */

// Class header
#include "util/ThreadPriority.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.ThreadPriority");
}

namespace lsst {
namespace qserv {
namespace util {


void ThreadPriority::storeOriginalValues() {
    getCurrentValues(_originalPriority, _originalPolicy);
    LOGS(_log, LOG_LVL_DEBUG, "thread priority original " <<
                              " priority=" << _originalPriority <<
                              " policy=" << _originalPolicy);
}


void ThreadPriority::getCurrentValues(int &priority, int &policy) {
    sched_param sch;
    getCurrentValues(sch, policy);
    priority = sch.sched_priority;
}


void ThreadPriority::getCurrentValues(sched_param &sch, int &policy) {
    pthread_getschedparam(_pthreadHandle, &policy, &sch);
}


int ThreadPriority::setPriorityPolicy(int newPriority, int newPolicy) {
    sched_param sch;
    int policy;
    getCurrentValues(sch, policy);
    sch.sched_priority = newPriority;
    int result = pthread_setschedparam(_pthreadHandle, newPolicy, &sch);
    if (result) {
        LOGS(_log, LOG_LVL_ERROR, "failed to set thread priority result=" << result <<
                " EPERM=" << EPERM);
    }
    return result;
}


}}} // namespace lsst/qserv/util


