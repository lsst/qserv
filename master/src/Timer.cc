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
#include "lsst/qserv/master/Timer.h"

#include <cstdio>

namespace lsst {
namespace qserv {
namespace master {

std::ostream & Timer::write(std::ostream & os, struct ::timeval const & time) {
    char buf[64];
    // Make sure microseconds are in range [0, 1000000)
    struct ::timeval t = time;
    if (t.tv_usec >= 1000000) {
        ::suseconds_t q = t.tv_usec / 1000000;
        ::suseconds_t r = t.tv_usec % 1000000;
        t.tv_sec += static_cast< ::time_t>(q);
        t.tv_usec = r;
    } else if (t.tv_usec < 0) {
        ::suseconds_t q = -t.tv_usec / 1000000;
        ::suseconds_t r = -t.tv_usec % 1000000;
        if (r != 0) {
            q += 1;
            r = 1000000 - r;
        }
        t.tv_sec -= static_cast< ::time_t>(q);
        t.tv_usec = r;
    }
    struct ::tm breakdown;
    ::gmtime_r(&t.tv_sec, &breakdown);
    // Convert to ISO 8601 UTC date-time string
    size_t n = ::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &breakdown);
    // ...with microsecond precision.
    std::snprintf(buf + n, sizeof(buf) - n, ".%06dZ",
                  static_cast<int>(t.tv_usec));
    os << buf;
    return os;
}

std::ostream & operator<<(std::ostream & os, Timer const & timer) {
    Timer::write(os, timer.startTime);
    os << ' ' << timer.getElapsed();
    return os;
}

}}} // namespace lsst::qserv::master
