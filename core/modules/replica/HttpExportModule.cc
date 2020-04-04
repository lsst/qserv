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
#include "replica/HttpExportModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpRequestBody.h"
#include "replica/HttpRequestQuery.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace lsst {
namespace qserv {
namespace replica {

HttpExportModule::Ptr HttpExportModule::create(Controller::Ptr const& controller,
                                               string const& taskName,
                                               HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpExportModule(controller, taskName, processorConfig));
}


HttpExportModule::HttpExportModule(Controller::Ptr const& controller,
                                   string const& taskName,
                                   HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpExportModule::executeImpl(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp,
                                   string const& subModuleName) {

    if (subModuleName == "TABLES") {
        _getTables(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpExportModule::_getTables(qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp) {
    debug(__func__);
    throw runtime_error(context() + "::" + string(__func__) + "  not implemented.");
}

}}}  // namespace lsst::qserv::replica
