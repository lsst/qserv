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
// See Logger.h

#include "log/Logger.h"

namespace {

/// Sink class responsible for synchronization.
class SyncSink : public boost::iostreams::sink {
public:
    SyncSink(std::ostream* os) : _os(os) {}
    std::streamsize write(const char *s, std::streamsize n) {
        boost::mutex::scoped_lock lock(_mutex);
        std::string message(s, n);
        *_os << message << std::flush;
        return n;
    }
private:
    std::ostream* _os;
    static boost::mutex _mutex;
};
boost::mutex SyncSink::_mutex;
static SyncSink syncSink(&std::cout);
static boost::iostreams::stream_buffer<SyncSink> syncBuffer(syncSink);
static std::ostream logStream(&syncBuffer);

} // Anonymous namespace

namespace lsst {
namespace qserv {
namespace log {

// Thread local storage to ensure one instance of Logger per thread.
boost::thread_specific_ptr<Logger> Logger::_instancePtr;

// Application-wide severity threshold.
Logger::Severity Logger::_severityThreshold = Info;
boost::mutex Logger::_mutex;


//Logger::SyncSink Logger::syncSink(&(std::cout));
//boost::iostreams::stream_buffer<Logger::SyncSink> Logger::syncBuffer(syncSink);
//std::ostream Logger::logStream(&syncBuffer);

////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////
// Filters
////////////////////////////////////////////////////////////////////////
/// Filter class responsible for enforcing severity level.
class Logger::SeverityFilter : public boost::iostreams::multichar_output_filter {
public:
    SeverityFilter(Logger* loggerPtr);
    template<typename Sink>
    std::streamsize write(Sink& dest, const char* s, std::streamsize n);
private:
    Logger* _loggerPtr;
};

/// Filter class responsible for formatting Logger output.
class Logger::LogFilter : public boost::iostreams::line_filter {
public:
    LogFilter(Logger* loggerPtr);
private:
    std::string do_filter(const std::string& line);
    std::string getTimeStamp();
    std::string getThreadId();
    std::string getSeverity();
    Logger* _loggerPtr;
};


////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////
#if 0
Logger::SyncSink::SyncSink(std::ostream* os) : boost::iostreams::sink() {
    _os = os;
}
#endif

Logger& Logger::Instance() {
    if (_instancePtr.get() == NULL) _instancePtr.reset(new Logger);
    return *_instancePtr;
}

// Allows retrieval of singleton and setting of severity
// with single function call.
Logger& Logger::Instance(Logger::Severity severity) {
    if (_instancePtr.get() == NULL) _instancePtr.reset(new Logger);
    _instancePtr.get()->setSeverity(severity);
    return *_instancePtr;
}

void Logger::setSeverity(Logger::Severity severity) {
    if (severity != _severity) {
        flush();
        _severity = severity;
    }
}

Logger::Severity Logger::getSeverity() const {
    return _severity;
}

void Logger::setSeverityThreshold(Logger::Severity severity) {
    boost::mutex::scoped_lock lock(Logger::_mutex);
    if (severity != _severityThreshold) {
        flush();
        _severityThreshold = severity;
    }
}

Logger::Severity Logger::getSeverityThreshold() const {
    return _severityThreshold;
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////

Logger::Logger() : boost::iostreams::filtering_ostream() {
    _severity = Info;
    Logger::SeverityFilter severityFilter(this);
    Logger::LogFilter logFilter(this);
    push(severityFilter);
    push(logFilter);
    push(logStream);
}

Logger::SeverityFilter::SeverityFilter(Logger* loggerPtr)
    : boost::iostreams::multichar_output_filter() {
    _loggerPtr = loggerPtr;
}

template<typename Sink>
std::streamsize Logger::SeverityFilter::write(Sink& dest, const char* s, std::streamsize n) {
    if (_loggerPtr->getSeverity() >= _loggerPtr->getSeverityThreshold()) {
        std::streamsize z;
        for (z = 0; z < n; ++z) {
            if (!boost::iostreams::put(dest, s[z]))
                break;
        }
        return z;
    } else
        return n;
}

Logger::LogFilter::LogFilter(Logger* loggerPtr) : boost::iostreams::line_filter() {
    _loggerPtr = loggerPtr;
}

std::string Logger::LogFilter::do_filter(const std::string& line) {
    return getTimeStamp() + " " + getThreadId() + " " + getSeverity() + " " + line;
}

std::string Logger::LogFilter::getTimeStamp() {
    char fmt[64], buf[64];
    struct timeval tv;
    struct tm* tm;
    gettimeofday(&tv, NULL);
    struct tm newtime;
    memset(&newtime, 0, sizeof(struct tm));
    localtime_r(&tv.tv_sec, &newtime);
    strftime(fmt, sizeof fmt, "%Y%m%d %H:%M:%S.%%06u", &newtime);
    snprintf(buf, sizeof buf, fmt, tv.tv_usec);
    return std::string(buf);
}

std::string Logger::LogFilter::getThreadId() {
    return boost::lexical_cast<std::string>(boost::this_thread::get_id());
}

std::string Logger::LogFilter::getSeverity() {
    switch (_loggerPtr->getSeverity()) {
    case Logger::Debug:
        return "DBG";
    case Logger::Info:
        return "INF";
    case Logger::Warning:
        return "WRN";
    case Logger::Error:
        return "ERR";
    default:
        return NULL;
    }
}

}}} // namespace lsst::qserv::log
