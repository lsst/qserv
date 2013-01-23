// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// Logger is a logging class used right now in the qserv worker plugins.

#ifndef LSST_QSERV_WORKER_LOGGER_H
#define LSST_QSERV_WORKER_LOGGER_H
#include <string>
#include <boost/shared_ptr.hpp>
// Forward
class XrdSysLogger;
class XrdSysError;

namespace lsst { namespace qserv { namespace worker {

/// A class to define a logging facility that:
/// (a)  has logging levels
/// (b)  has selectable outputs: stdout and xrootd's system log
class Logger {
public:
    typedef boost::shared_ptr<Logger> Ptr;

    enum LogLevel { LOG_FATAL=1,
                    LOG_ERROR=2, 
                    LOG_WARN=3, 
                    LOG_INFO=4, 
                    LOG_DEBUG=5,
                    LOG_EVERYTHING=9999 };
    Logger()
        : _logLevel(LOG_EVERYTHING), _prefix("") {
        _init();
    }
    explicit Logger(XrdSysLogger* log) 
        : _log(log), _logLevel(LOG_EVERYTHING), _prefix("") {
        _init();
    }
    void setPrefix(std::string const& prefix) { _prefix = prefix; }
    std::string const& getPrefix(std::string const& prefix) const {
        return _prefix; }
    void setLogLevel(LogLevel logLevel) { _logLevel = logLevel; }
    LogLevel getLogLevel(LogLevel logLevel) const { return _logLevel; }

    inline void fatal(std::string const& s) { message(LOG_FATAL, s); }
    inline void error(std::string const& s) { message(LOG_ERROR, s); }
    inline void warn(std::string const& s) { message(LOG_WARN, s); }
    inline void info(std::string const& s) { message(LOG_INFO, s); }
    inline void debug(std::string const& s) { message(LOG_INFO, s); }

    inline void fatal(char const* s) { message(LOG_FATAL, s); }
    inline void error(char const* s) { message(LOG_ERROR, s); }
    inline void warn(char const* s) { message(LOG_WARN, s); }
    inline void info(char const* s) { message(LOG_INFO, s); }
    inline void debug(char const* s) { message(LOG_INFO, s); }

    inline void message(LogLevel logLevel, std::string const& s) {
        message(logLevel, s.c_str());
    }
    void message(LogLevel logLevel, char const* s);

private:
    void _init();

    std::string _prefix;
    XrdSysLogger* _log;
    boost::shared_ptr<XrdSysError> _xrdSysError;
    LogLevel _logLevel;
};
}}} // namespace lsst::qserv::worker


#endif // LSST_QSERV_WORKER_LOGGER_H
