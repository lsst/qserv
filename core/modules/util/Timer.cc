// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// Generic timer class

#include "util/Timer.h"

// System headers
#include <cstdio>

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.Timer");
}


namespace lsst {
namespace qserv {
namespace util {

std::ostream & Timer::write(std::ostream & os, struct ::timeval const & time) {
    char buf[64];
    // Make sure microseconds are in range [0, 1000000)
    ::time_t s = static_cast<time_t>(time.tv_usec / 1000000);
    ::suseconds_t u = time.tv_usec % 1000000;
    if (u < 0) {
        // round down
        --s;
        u += 1000000;
    }
    s += time.tv_sec;
    struct ::tm breakdown;
    ::gmtime_r(&s, &breakdown);
    // Convert to ISO 8601 UTC date-time string
    size_t n = ::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &breakdown);
    // ...with microsecond precision.
    std::snprintf(buf + n, sizeof(buf) - n, ".%06dZ", static_cast<int>(u));
    os << buf;
    return os;
}

std::ostream & operator<<(std::ostream & os, Timer const & timer) {
    Timer::write(os, timer.startTime);
    os << ' ' << timer.getElapsed();
    return os;
}


LockGuardTimed::LockGuardTimed(std::mutex& mtx, std::string const& note)
    : _mtx(mtx), _note(note) {
    timeToLock.start();
    _mtx.lock();
    timeToLock.stop();
    timeHeld.start();
}

LockGuardTimed::~LockGuardTimed() {
    _mtx.unlock();
    timeHeld.stop();
    LOGS(_log, LOG_LVL_DEBUG, "lockTime " << _note << " toLock=" << timeToLock.getElapsed() << " held=" << timeHeld.getElapsed());
}


}}} // namespace lsst::qserv::util
