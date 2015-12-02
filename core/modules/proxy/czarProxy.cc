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
#include <sys/types.h>
#include <unistd.h>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "czar/Czar.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.proxy.czarProxy");

// return czar instance, hints may be used only for the first call
lsst::qserv::czar::Czar& czarInstance(std::map<std::string, std::string> const& hints);

}

namespace lsst {
namespace qserv {
namespace proxy {

// Constructors
std::vector<std::string>
submitQuery(std::string const& query, std::map<std::string, std::string> const& hints) {
    return czarInstance(hints).submitQuery(query, hints);
}

std::string
killQuery(std::string const& query, std::string const& clientId) {
    static std::map<std::string, std::string> const hints{
        std::make_pair("client_dst_name", clientId)
    };
    return czarInstance(hints).killQuery(query, clientId);
}

void log(std::string const& loggername, std::string const& level,
         std::string const& filename, std::string const& funcname,
         unsigned int lineno, std::string const& message) {
    lsst::log::Log::log(loggername, log4cxx::Level::toLevel(level),
                        filename, funcname, lineno, "%s", message.c_str());
}


}}} // namespace lsst::qserv::proxy

namespace {

std::string qservConfig() {
    auto qConfig = std::getenv("QSERV_CONFIG");
    if (not qConfig or *qConfig == '\0') {
        throw std::runtime_error("QSERV_CONFIG is not defined");
    }
    return qConfig;
}

std::string czarName(std::map<std::string, std::string> const& hints) {
    // get czar name from hints, QSERV_CZAR_NAME can override
    auto qName = std::getenv("QSERV_CZAR_NAME");
    if (qName and *qName != '\0') {
        return qName;
    }
    auto iter = hints.find("client_dst_name");
    if (iter != hints.end()) {
        // use proxy client id (which includes host name and port number of the
        // proxy-side client connection)
        return iter->second;
    } else {
        return "czar." + std::to_string(getpid());
    }
}

lsst::qserv::czar::Czar&
czarInstance(std::map<std::string, std::string> const& hints) {
    static lsst::qserv::czar::Czar czar(qservConfig(), czarName(hints));
    return czar;
}

}
