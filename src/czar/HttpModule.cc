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
#include "czar/HttpModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/Exceptions.h"
#include "http/RequestBody.h"
#include "http/RequestQuery.h"
#include "qhttp/Request.h"

using namespace std;

namespace lsst::qserv::czar {

HttpModule::HttpModule(string const& context, shared_ptr<qhttp::Request> const& req,
                       shared_ptr<qhttp::Response> const& resp)
        : http::ModuleBase(cconfig::CzarConfig::instance()->replicationAuthKey(),
                           cconfig::CzarConfig::instance()->replicationAdminAuthKey(), req, resp),
          _context(context) {}

string HttpModule::context() const { return _context; }

void HttpModule::enforceCzarId(string const& func) const {
    string const czarIdAttrName = "czar";
    string czarId;
    if (req()->method == "GET") {
        if (!query().has(czarIdAttrName)) {
            throw http::Error(func, "No Czar identifier was provided in the request query.");
        }
        czarId = query().requiredString(czarIdAttrName);
    } else {
        if (!body().has(czarIdAttrName)) {
            throw http::Error(func, "No Czar identifier was provided in the request body.");
        }
        czarId = body().required<string>(czarIdAttrName);
    }
    string const expectedCzarId = cconfig::CzarConfig::instance()->id();
    if (expectedCzarId != czarId) {
        string const msg = "Requested Czar identifier '" + czarId + "' does not match the one '" +
                           expectedCzarId + "' of the current Czar.";
        throw http::Error(func, msg);
    }
}

}  // namespace lsst::qserv::czar
