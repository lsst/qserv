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
#include <float.h>
#include <set>

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
    LOGS(_log, LOG_LVL_DEBUG, "lockTime " << _note << " toLock=" << timeToLock.getElapsed() <<
                              " held=" << timeHeld.getElapsed());
}


TimerHistogram::TimerHistogram(std::string const& label, std::vector<double> const& times) : _label(label) {
    //sort vector and remove duplicates.
    std::set<double> timeSet;
    for (auto& t:times) {
        timeSet.insert(t);
    }
    for (auto& t:timeSet) {
        _buckets.emplace_back(bucket(t));
    }
}


std::string TimerHistogram::addTime(double time, std::string const& note) {
    std::lock_guard<std::mutex> lock(_mtx);
    _total += time;
    ++_totalCount;
    bool found = false;
    for(auto& bkt:_buckets) {
        if (time < bkt.getMaxVal()) {
            ++bkt.count;
            found = true;
            break;
        }
    }
    if (not found) {
        ++_overMaxCount;
    }

    if (note == "") {
        return "";
    }
    return _getString(note);
}


std::string TimerHistogram::getString(std::string const& note) {
    std::lock_guard<std::mutex> lock(_mtx);
    return _getString(note);
}


/// _mtx must be locked before calling this function.
///
std::string TimerHistogram::_getString(std::string const& note) {
    std::stringstream os;
    os << _label << " " << note << " avg=" << (_total/_totalCount) << " ";
    double maxB = -DBL_MAX;
    for (auto& bkt:_buckets) {
        os << " <" << bkt.getMaxVal() << "=" << bkt.count;
        maxB = bkt.getMaxVal();
    }
    os << " >" << maxB << "=" << _overMaxCount;
    return os.str();
}

}}} // namespace lsst::qserv::util
