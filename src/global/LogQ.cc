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
// Class header
#include "global/LogQ.h"

// System headers

using namespace std;

LOG_LOGGER _log = LOG_GET("lsst.qserv.global.LogQ");

namespace lsst::qserv {

LogQ::Ptr LogQ::_logQ = make_shared<LogQ>();

void LogQ::setLogLevelStr(std::string const& levelStr) {
    char levelC = 'D';
    if (levelStr.length() > 0) {
        levelC = toupper(levelStr[0]);
    }
    switch (levelC) {
    case 'T':
        _logLevel = LOG_LVL_TRACE;
        break;
    case 'D':
        _logLevel = LOG_LVL_DEBUG;
        break;
    case 'I':
        _logLevel = LOG_LVL_INFO;
        break;
    case 'W':
        _logLevel = LOG_LVL_WARN;
        break;
    case 'E':
        _logLevel = LOG_LVL_ERROR;
        break;
    default:
        _logLevel = LOG_LVL_DEBUG;
    }
}

void LogQ::setLogLevel(int level) {
    _logLevel = level;
    LOGQ(_log, LOG_LVL_ERROR, "LogQ::setLogLevel: " << getLogLevelStr(level));
}


std::string LogQ::getLogLevelStr(int level) {
    switch (level) {
        case LOG_LVL_TRACE:
            return "TRACE";
        case LOG_LVL_DEBUG:
            return "DEBUG";
        case LOG_LVL_INFO:
            return "INFO";
        case LOG_LVL_WARN:
            return "WARN";
        case LOG_LVL_ERROR:
            return "ERROR";
        default:
            return "unknown";
    }
}


}  // namespace lsst::qserv
