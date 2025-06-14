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
#include "http/BaseModule.h"

// Qserv headers
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/RequestQuery.h"
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.http.BaseModule");

string packWarnings(list<string> const& warnings) {
    string packed;
    for (auto const& msg : warnings) {
        if (!packed.empty()) packed += "; ";
        packed += msg;
    }
    return packed;
}
}  // namespace

namespace lsst::qserv::http {

BaseModule::BaseModule(AuthContext const& authContext) : _authContext(authContext) {}

void BaseModule::checkApiVersion(string const& func, unsigned int minVersion, string const& warning) const {
    unsigned int const maxVersion = MetaModule::version;
    unsigned int version = 0;
    string const versionAttrName = "version";
    json const errorEx = json::object({{"min_version", minVersion}, {"max_version", maxVersion}});

    // Intercept exceptions thrown when converting the attribute's value (if provided)
    // in order to inject the allowed range of the version numbers into the extended
    // error sent back to the caller.
    //
    // Note that requests sent w/o explicitly specified API version will still be
    // processed. In this case a warning will be sent in the response object.
    //
    // Note that the version attribute may be present in either the query string
    // or the body of a request. The method will check both locations. The number found
    // in the body will take precedence over the one found in the query string.
    try {
        if (query().has(versionAttrName)) version = query().requiredUInt(versionAttrName);
        if (body().has(versionAttrName)) version = body().requiredUInt(versionAttrName);
        if (version == 0) {
            warn("No version number was provided in the request.");
            return;
        }
    } catch (...) {
        throw http::Error(func, "The optional parameter " + versionAttrName + " is not a number.", errorEx);
    }
    if (!(minVersion <= version && version <= maxVersion)) {
        if (!warning.empty()) warn(warning);
        throw http::Error(func,
                          "The requested version " + to_string(version) +
                                  " of the API is not in the range supported by the service.",
                          errorEx);
    }
}

void BaseModule::enforceInstanceId(string const& func, string const& requiredInstanceId) const {
    string const instanceId = method() == "GET" ? query().requiredString("instance_id")
                                                : body().required<string>("instance_id");
    debug(func, "instance_id: " + instanceId);
    if (instanceId != requiredInstanceId) {
        throw invalid_argument(context() + func + " Qserv instance identifier mismatch. Client sent '" +
                               instanceId + "' instead of '" + requiredInstanceId + "'.");
    }
}

void BaseModule::info(string const& msg) const { LOGS(_log, LOG_LVL_INFO, context() << msg); }

void BaseModule::debug(string const& msg) const { LOGS(_log, LOG_LVL_DEBUG, context() << msg); }

void BaseModule::warn(string const& msg) const {
    LOGS(_log, LOG_LVL_WARN, context() << msg);
    _warnings.push_back(msg);
}

void BaseModule::error(string const& msg) const { LOGS(_log, LOG_LVL_ERROR, context() << msg); }

void BaseModule::sendError(string const& func, string const& errorMsg, json const& errorExt) {
    error(func, errorMsg);
    json result;
    result["success"] = 0;
    result["error"] = errorMsg;
    result["error_ext"] = errorExt.is_null() ? json::object() : errorExt;
    result["warning"] = ::packWarnings(_warnings);
    sendResponse(result.dump(), "application/json");
}

void BaseModule::sendData(json& result) {
    result["success"] = 1;
    result["error"] = "";
    result["error_ext"] = json::object();
    result["warning"] = ::packWarnings(_warnings);
    sendResponse(result.dump(), "application/json");
}

void BaseModule::enforceAuthorization(http::AuthType const authType) {
    switch (authType) {
        case http::AuthType::NONE:
            return;
        case http::AuthType::REQUIRED:
            _enforceKeyAuthorization();
            return;
        case http::AuthType::BASIC:
            _enforceBasicAuthorization();
            return;
        default:
            throw std::invalid_argument(
                    context() + "unknown authorization type: " + std::to_string(static_cast<int>(authType)));
    }
}

void BaseModule::_enforceKeyAuthorization() {
    if (body().has("admin_auth_key")) {
        auto const adminAuthKey = body().required<string>("admin_auth_key");
        if (adminAuthKey != _authContext.adminAuthKey) {
            throw AuthError(context() +
                            "administrator's authorization key 'admin_auth_key' in the request"
                            " doesn't match the one in server configuration");
        }
        _isAdmin = true;
        return;
    }
    if (body().has("auth_key")) {
        auto const authKey = body().required<string>("auth_key");
        if (authKey != _authContext.authKey) {
            throw AuthError(context() +
                            "authorization key 'auth_key' in the request doesn't match"
                            " the one in server configuration");
        }
        return;
    }
    throw AuthError(context() +
                    "none of the authorization keys 'auth_key' or 'admin_auth_key' was found"
                    " in the request. Please, provide one.");
}

void BaseModule::_enforceBasicAuthorization() const {
    // Get and analyze a value of the "Authorization" header in the request.
    // The header is expected to have the "Basic" prefix followed by a base64-encoded
    // string with the user name and password separated by a colon.
    string const authHeader = headerEntry("Authorization");
    if (authHeader.empty()) {
        throw AuthError(context() + "missing 'Authorization' header in the request");
    }
    bool const skipEmpty = true;
    auto const schemeAndCredentials = util::String::split(authHeader, " ", skipEmpty);
    if (schemeAndCredentials.size() != 2) {
        throw AuthError(context() + "invalid 'Authorization' header in the request: " + authHeader);
    }
    auto const& scheme = schemeAndCredentials[0];
    auto const& token = schemeAndCredentials[1];
    if (scheme != "Basic") {
        throw AuthError(context() + "unsupported 'Authorization' scheme: " + scheme);
    }
    string const expectedToken = util::String::toBase64(_authContext.user + ":" + _authContext.password);
    if (token != expectedToken) {
        throw AuthError(context() + "invalid 'Authorization' credentials in the request");
    }
}

}  // namespace lsst::qserv::http
