/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "proxy/czarProxy.h"

// System headers
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "czar/Czar.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.proxy.czarProxy");

void initMDC() { LOG_MDC("LWP", to_string(lsst::log::lwpID())); }

shared_ptr<lsst::qserv::czar::Czar> _czar;

// mutex is used for czar initialization only which could potentially
// happen simultaneously from several threads.
mutex _czarMutex;

}  // namespace

namespace lsst::qserv::proxy {

void initCzar(string const& czarName) {
    lock_guard<mutex> lock(_czarMutex);

    // Ignore repeated calls (they are hard to filter on mysql-proxy side)
    if (czar::Czar::getCzar() != nullptr) return;

    // Add some MDC data in every thread
    LOG_MDC_INIT(initMDC);

    // Get a location of the configuration file
    char const* configFilePath = getenv("QSERV_CONFIG");
    if ((configFilePath == nullptr) || (configFilePath[0] == '\0')) {
        throw runtime_error(string(__func__) + " environment variable QSERV_CONFIG is not defined");
    }
    _czar = czar::Czar::createCzar(configFilePath, czarName);
}

czar::SubmitResult submitQuery(string const& query, map<string, string> const& hints) {
    if (::_czar == nullptr) {
        throw runtime_error(string(__func__) + ": czar instance not initialized");
    }
    return ::_czar->submitQuery(query, hints);
}

void killQuery(string const& query, string const& clientId) {
    if (::_czar == nullptr) {
        throw runtime_error(string(__func__) + ": czar instance not initialized");
    }
    ::_czar->killQuery(query, clientId);
}

void log(string const& loggerName, string const& level, string const& fileName, string const& funcName,
         unsigned int lineNo, string const& message) {
    auto logger = lsst::log::Log::getLogger(loggerName);
    auto levelPtr = log4cxx::Level::toLevel(level);
    if (logger.isEnabledFor(levelPtr->toInt())) {
        logger.logMsg(levelPtr,
                      log4cxx::spi::LocationInfo(
                              fileName.data(), log4cxx::spi::LocationInfo::calcShortFileName(fileName.data()),
                              funcName.data(), lineNo),
                      message);
    }
}

}  // namespace lsst::qserv::proxy
