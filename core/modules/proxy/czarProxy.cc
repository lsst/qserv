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

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/IterableFormatter.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.proxy.czarProxy");

}

namespace lsst {
namespace qserv {
namespace proxy {

// Constructors
std::vector<std::string>
submitQuery(std::string const& query, std::map<std::string, std::string> const& hints) {

    LOGF(_log, LOG_LVL_INFO, "new query: %s" % query);
    LOGF(_log, LOG_LVL_INFO, "hints: %s" % util::printable(hints));

    std::vector<std::string> result{"", "result_table", "message_table", "ORDER BY"};
    return result;
}

void
killQueryUgly(std::string const& query, std::string const& clientId) {

}

}}} // namespace lsst::qserv::proxy
