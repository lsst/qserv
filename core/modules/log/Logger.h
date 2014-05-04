/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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

// Logger.h:
// class Logger -- A class that handles application-wide logging.
//

#ifndef LSST_QSERV_LOG_LOGGER_H
#define LSST_QSERV_LOG_LOGGER_H

// These directives are for convenience.
#define LOG_STRM(level) lsst::qserv::log::Logger::Instance(lsst::qserv::log::Logger::level)
#define LOGGER(level) if (lsst::qserv::log::Logger::level >= \
    lsst::qserv::log::Logger::Instance().getSeverityThreshold()) \
    lsst::qserv::log::Logger::Instance(lsst::qserv::log::Logger::level)
#define LOGGER_DBG LOGGER(Debug)
#define LOGGER_INF LOGGER(Info)
#define LOGGER_WRN LOGGER(Warning)
#define LOGGER_ERR LOGGER(Error)
#define LOGGER_THRESHOLD(level) lsst::qserv::log::Logger::Instance()\
    .setSeverityThreshold(lsst::qserv::log::Logger::level);
#define LOGGER_THRESHOLD_DBG LOGGER_THRESHOLD(Debug)
#define LOGGER_THRESHOLD_INF LOGGER_THRESHOLD(Info)
#define LOGGER_THRESHOLD_WRN LOGGER_THRESHOLD(Warning)
#define LOGGER_THRESHOLD_ERR LOGGER_THRESHOLD(Error)

// System headers
#include <iostream>
#include <stdio.h>
#include <sys/time.h>

// Third-party headers
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/line.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

namespace lsst {
namespace qserv {
namespace log {

class Logger : public boost::iostreams::filtering_ostream {
public:
    enum Severity { Debug = 0, Info, Warning, Error };
    static Logger& Instance();
    static Logger& Instance(Severity severity);
    void setSeverity(Severity severity);
    Severity getSeverity() const;
    void setSeverityThreshold(Severity severity);
    Severity getSeverityThreshold() const;

private:
    class SeverityFilter;
    class LogFilter;

    // Private constructor, etc. as per singleton pattern.
    Logger();
    Logger(Logger const&);
    Logger& operator=(Logger const&);

    Severity _severity;
    static Severity _severityThreshold; // Application-wide severity threshold.
    static boost::mutex _mutex;

    // Thread local storage to ensure one instance of Logger per thread.
    static boost::thread_specific_ptr<Logger> _instancePtr;
};

}}} // namespace lsst::qserv::log

#endif // LSST_QSERV_LOG_LOGGER_H
