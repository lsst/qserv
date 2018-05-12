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
#ifndef LSST_QSERV_REPLICA_LOCK_UTILS_H
#define LSST_QSERV_REPLICA_LOCK_UTILS_H

/// LockUtils.h declares:
///
/// macro LOCK
/// (see individual class documentation for more information)

// System headers
#include <cassert>

// Third party headers

// Qserv headers
#include "lsst/log/Log.h"
#include "util/Mutex.h"

// This header declarations

namespace {
#ifndef LSST_QSERV_REPLICA_LOCK_UTILS_LOGGER
#define LSST_QSERV_REPLICA_LOCK_UTILS_LOGGER
LOG_LOGGER _lockUtilsLog = LOG_GET("lsst.qserv.replica.LockUtil");
#endif
}

/*
 * This macro to appear within each block which requires thread safety.
 *
 *   MUTEX   - is a mutex to be locked
 *   CONTEXT - is a context to be reported for the debugging purposes
 */
#ifndef LOCK
#define LOCK(MUTEX,CONTEXT) \
LOGS(_lockUtilsLog, LOG_LVL_DEBUG, CONTEXT << "  LOCK[" << MUTEX.id() << "]:1 " << #MUTEX); \
assert(not MUTEX.lockedByCaller()); \
std::lock_guard<util::Mutex> lock(MUTEX); \
LOGS(_lockUtilsLog, LOG_LVL_DEBUG, CONTEXT << "  LOCK[" << MUTEX.id() << "]:2 " << #MUTEX)
#endif

/*
 * This macro ensures a lock is made by a calling thread at a point
 * where the macro is used.
 *
 *   MUTEX   - is a mutex to be locked
 *   CONTEXT - is a context to be reported for the debugging purposes
 */
#ifndef ASSERT_LOCK
#define ASSERT_LOCK(MUTEX,CONTEXT) \
LOGS(_lockUtilsLog, LOG_LVL_DEBUG, CONTEXT << "  ASSERT LOCK["<< MUTEX.id() << "] " << #MUTEX); \
assert(MUTEX.lockedByCaller());
#endif


#endif // LSST_QSERV_REPLICA_LOCK_UTILS_H
