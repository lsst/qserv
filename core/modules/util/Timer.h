// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_TIMER_H
#define LSST_QSERV_UTIL_TIMER_H

// System headers
#include <cstddef>
#include <ostream>
#include <sys/time.h>
#include <time.h>
#include <mutex>
#include <vector>

namespace lsst {
namespace qserv {
namespace util {

/// A dirt-simple class for instrumenting ops in qserv.
struct Timer {
    Timer() {
        startTime.tv_sec = 0; startTime.tv_usec = 0;
        stopTime.tv_sec = 0; stopTime.tv_usec = 0;
    }

    void start() { ::gettimeofday(&startTime, nullptr); }
    void stop() { ::gettimeofday(&stopTime, nullptr); }

    /// Return the difference in wall clock time between the most recent
    /// calls to start() and stop() in seconds.
    double getElapsed() const {
        ::time_t seconds = stopTime.tv_sec - startTime.tv_sec;
        ::suseconds_t usec = stopTime.tv_usec - startTime.tv_usec;
        return seconds + (usec * 0.000001);
    }

    struct ::timeval startTime;
    struct ::timeval stopTime;

    /// Convert time to an ISO 8601 UTC string with microsecond precision
    /// and write it to the given output stream.
    static std::ostream & write(std::ostream & os,
                                struct ::timeval const & time);
};

std::ostream& operator<<(std::ostream & os, Timer const & tm);

/// This class is used to log how long it takes to lock a mutex
/// and how long the mutex is held.
class LockGuardTimed {
public:
    LockGuardTimed(std::mutex& mtx, std::string const& note);
    LockGuardTimed() = delete;
    LockGuardTimed(LockGuardTimed const&) = delete;
    ~LockGuardTimed();

    LockGuardTimed& operator=(LockGuardTimed const&) = delete;

private:
    std::mutex& _mtx;
    std::string _note;
    Timer timeToLock;
    Timer timeHeld;
};


/// This class is useful for getting an idea of how long something usually takes.
/// It is also fairly easy to locate an abnormally long call in the log by
/// searching for the first instance of a particular histogram value.
class TimerHistogram {
public:
    /// This keeps track of the count of entries with values greater than
    /// the previous bucket's maxVal and less than this bucket's maxVal.
    class bucket {
    public:
        bucket(double maxV) : _maxVal(maxV) {}
        bucket() = delete;
        bucket(bucket const&) = default;

        double getMaxVal() { return _maxVal; }
        int count{0};
    private:
        double _maxVal;
    };

    TimerHistogram(std::string const& label, std::vector<double> const& times);
    TimerHistogram() = delete;
    TimerHistogram(TimerHistogram const&) = delete;
    TimerHistogram& operator=(TimerHistogram const&) = delete;

    // Add a time to the histogram. If note is != "", return a log worthy string of the histogram.
    std::string addTime(double time, std::string const& note="");
    std::string getString(std::string const& note=""); ///< @return a log worthy version of the histogram.

private:
    std::string _getString(std::string const& note);

    std::string _label;
    std::mutex _mtx;
    std::vector<bucket> _buckets;
    uint64_t _overMaxCount{0};
    double _total{0.0};
    uint64_t _totalCount{0};
};

}}} // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_TIMER_H

