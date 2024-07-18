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
#include "czar/ChttpModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "http/RequestQuery.h"

using namespace std;

namespace lsst::qserv::czar {

ChttpModule::ChttpModule(string const& context, httplib::Request const& req, httplib::Response& resp)
        : http::ChttpModule(cconfig::CzarConfig::instance()->replicationAuthKey(),
                            cconfig::CzarConfig::instance()->replicationAdminAuthKey(), req, resp),
          _context(context) {}

string ChttpModule::context() const { return _context; }

void ChttpModule::enforceCzarName(string const& func) const {
    string const czarNameAttrName = "czar";
    string czarName;
    if (method() == "GET") {
        if (!query().has(czarNameAttrName)) {
            throw http::Error(func, "No Czar identifier was provided in the request query.");
        }
        czarName = query().requiredString(czarNameAttrName);
    } else {
        if (!body().has(czarNameAttrName)) {
            throw http::Error(func, "No Czar identifier was provided in the request body.");
        }
        czarName = body().required<string>(czarNameAttrName);
    }
    string const expectedCzarName = cconfig::CzarConfig::instance()->name();
    if (expectedCzarName != czarName) {
        string const msg = "Requested Czar identifier '" + czarName + "' does not match the one '" +
                           expectedCzarName + "' of the current Czar.";
        throw http::Error(func, msg);
    }
}

}  // namespace lsst::qserv::czar
