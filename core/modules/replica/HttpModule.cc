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
#include "replica/HttpModule.h"

// Qserv headers
#include "replica/HttpRequestBody.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {
    LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpModule");
}

namespace lsst {
namespace qserv {
namespace replica {

HttpModule::HttpModule(Controller::Ptr const& controller,
                       string const& taskName,
                       HttpProcessorConfig const& processorConfig,
                       qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp)
    :   EventLogger(controller, taskName),
        _processorConfig(processorConfig),
        _req(req),
        _resp(resp),
        _query(req->query) {
}


void HttpModule::execute(string const& subModuleName,
                         AuthType const authType) {
    try {
        _body = HttpRequestBody(_req);
        if (authType == AUTH_REQUIRED) _enforceAuthorization();
        executeImpl(subModuleName);
    } catch (AuthError const& ex) {
        sendError(__func__, "failed to pass authorization requirements, ex: " + string(ex.what()));
    } catch (invalid_argument const& ex) {
        sendError(__func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        sendError(__func__, "operation failed due to: " + string(ex.what()));
    }
}


string HttpModule::context() const {
    return name() + " ";
}


void HttpModule::info(string const& msg) const {
    LOGS(_log, LOG_LVL_INFO, context() << msg);
}


void HttpModule::debug(string const& msg) const {
    LOGS(_log, LOG_LVL_DEBUG, context() << msg);
}


void HttpModule::error(string const& msg) const {
    LOGS(_log, LOG_LVL_ERROR, context() << msg);
}


void HttpModule::sendError(string const& func,
                           string const& errorMsg) const {
    error(func, errorMsg);

    json result;
    result["success"] = 0;
    result["error"] = errorMsg;

    resp()->send(result.dump(), "application/json");
}


void HttpModule::sendData(json& result,
                          bool success) {
    result["success"] = success ? 1 : 0;
    result["error"] = "";

    resp()->send(result.dump(), "application/json");
}


void HttpModule::_enforceAuthorization() const {
    auto authKey = body().required<string>("auth_key");
    if (authKey != _processorConfig.authKey) {
        throw AuthError("authorization key in the request didn't match the one in server configuration");
    }
}

}}}  // namespace lsst::qserv::replica
