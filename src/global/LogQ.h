/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_GLOBAL_LOGQ_H
#define LSST_QSERV_GLOBAL_LOGQ_H

// System headers
#include <atomic>

// LSST headers
#include "lsst/log/Log.h"

namespace lsst::qserv {

/* LogQ is a singleton class that minimizes the impact of logging on performance by using
 * a simple global atomic check before calling the more complicated Log::getLogger code.
 * LOGQ is a macro that uses LogQ.
 * Static initializers may be called before LogQ has been initialized, be aware.
 */
class LogQ {
public:
    using Ptr = std::shared_ptr<LogQ>;

    LogQ() = default;

    static Ptr getLogQ() {
        if (!_logQ) {
            throw std::runtime_error("LogQ has not been initialized.");
        }
        return _logQ;
    }

    static bool isEnabledFor(int level) {
        if (!_logQ) return true;
        return _logQ->_isEnabledFor(level);
    }

    static bool checkLvl(std::string const& logger, int level) {
        if (!lsst::qserv::LogQ::isEnabledFor(level)) {
            return false;
        }
        lsst::log::Log log(lsst::log::Log::getLogger(logger));
        return log.isEnabledFor(level);
    }

    static std::string getLogLevelStr(int level);

    std::string getLogLevelStr() const {
        return getLogLevelStr(_logLevel);
    }

    int getLogLevel() const {
        return _logLevel;
    }

    /// Set the log level based on a string representation of the level.
    /// @param levelStr - a string representation of the log level.
    ///      The first character is used to determine the level and is case insensitive.
    ///      Valid values are: 'T' for TRACE, 'D' for DEBUG, 'I' for INFO, 'W' for WARN, and 'E' for ERROR.
    ///      This is done to increase the likelihood of the string being recognized as a log level with
    ///      the expected behavior.
    /// Log level defaults to DEBUG if the string is not recognized.
    void setLogLevelStr(std::string const& levelStr);

    void setLogLevel(int level);

private:
    bool _isEnabledFor(int level) {
        return level >= _logLevel;
    }

    static Ptr _logQ;
    std::atomic<int> _logLevel{LOG_LVL_TRACE};
};

#define LOGQ(logger, level, message) \
    do { \
        if (lsst::qserv::LogQ::isEnabledFor(level)) { \
           lsst::log::Log log(lsst::log::Log::getLogger(logger)); \
           if (log.isEnabledFor(level)) { \
               LOG_MESSAGE_VIA_STREAM_(log, log4cxx::Level::toLevel(level), message); \
           } \
        } \
    } while (false)


#define LOG_CHECK_LVL(logger, level) \
    lsst::log::Log::getLogger(logger).isEnabledFor(level)

#define LOGQ_CHECK_LVL(logger, level) \
    lsst::qserv::LogQ::checkLvl(logger, level)

}  // namespace lsst::qserv

#endif  // LSST_QSERV_GLOBAL_LOGQ_H
