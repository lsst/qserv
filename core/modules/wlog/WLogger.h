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

#ifndef LSST_QSERV_WLOG_WLOGGER_H
#define LSST_QSERV_WLOG_WLOGGER_H

// System headers
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

// Forward
class XrdSysLogger;
class XrdSysError;

namespace lsst {
namespace qserv {
namespace wlog {

/// A class to define a logging facility that:
/// (a)  has logging levels
/// (b)  has selectable outputs: stdout and xrootd's system log
class WLogger : boost::noncopyable {
public:
    class Printer {
    public:
        virtual ~Printer() {}
        virtual Printer& operator()(char const* s) = 0;
    };
    typedef boost::shared_ptr<WLogger> Ptr;

    enum LogLevel { LOG_FATAL=10,
                    LOG_ERROR=20,
                    LOG_WARN=30,
                    LOG_INFO=40,
                    LOG_DEBUG=50,
                    LOG_EVERYTHING=9999 };
    WLogger()
        : _logLevel(LOG_EVERYTHING), _prefix("") {
        _init();
    }
    explicit WLogger(boost::shared_ptr<Printer> p)
        : _logLevel(LOG_EVERYTHING), _prefix(""), _printer(p) {
        _init();
    }
    explicit WLogger(WLogger::Ptr backend)
        : _logLevel(LOG_EVERYTHING), _backend(backend) {
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

    LogLevel _logLevel;
    std::string _prefix;
    boost::shared_ptr<Printer> _printer;
    WLogger::Ptr _backend;

};

}}} // namespace lsst::qserv::wlog

#endif // LSST_QSERV_WLOG_WLOGGER_H
