// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_MASTER_TIMER_H
#define LSST_QSERV_MASTER_TIMER_H

#include <sys/time.h>
#include <iostream>
// Timer.h : A dirt-simple class for instrumenting ops in qserv.

namespace lsst {
namespace qserv {
namespace master {
    
class Timer {
public:
    void start() { ::gettimeofday(&startTime, NULL); }
    void stop() { ::gettimeofday(&stopTime, NULL); }
    double getElapsed() const { 
        time_t seconds = stopTime.tv_sec - startTime.tv_sec;
        suseconds_t usec = stopTime.tv_usec - startTime.tv_usec;
        return seconds + (usec * 0.000001);
    }
    char const* getStartTimeStr() const {
        char* buf = const_cast<char*>(startTimeStr); // spiritually const
        asctime_r(localtime(&stopTime.tv_sec), buf); 
        buf[strlen(startTimeStr)-1] = 0;
        return startTimeStr;
    }

    char startTimeStr[30];
    struct ::timeval startTime;
    struct ::timeval stopTime;

    friend std::ostream& operator<<(std::ostream& os, Timer const& tm);
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_TIMER_H
 
