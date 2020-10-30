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
#include "replica/HttpMetaModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/HttpExceptions.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

unsigned int const HttpMetaModule::version = 2;


void HttpMetaModule::process(Controller::Ptr const& controller,
                             string const& taskName,
                             HttpProcessorConfig const& processorConfig,
                             qhttp::Request::Ptr const& req,
                             qhttp::Response::Ptr const& resp,
                             string const& subModuleName,
                             HttpModule::AuthType const authType) {
    HttpMetaModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpMetaModule::HttpMetaModule(Controller::Ptr const& controller,
                               string const& taskName,
                               HttpProcessorConfig const& processorConfig,
                               qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpMetaModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "VERSION") return _version();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpMetaModule::_version() {
    debug(__func__);
    json result;
    result["version"] = version;
    return result;
}

}}}  // namespace lsst::qserv::replica
