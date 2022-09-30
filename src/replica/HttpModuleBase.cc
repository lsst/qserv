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
#include "replica/HttpModuleBase.h"

// Qserv headers
#include "replica/HttpExceptions.h"
#include "replica/HttpMetaModule.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpModuleBase");
}

namespace lsst::qserv::replica {

HttpModuleBase::HttpModuleBase(string const& authKey, string const& adminAuthKey,
                               qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : _authKey(authKey), _adminAuthKey(adminAuthKey), _req(req), _resp(resp), _query(req->query) {}

HttpModuleBase::~HttpModuleBase() {}

void HttpModuleBase::execute(string const& subModuleName, HttpAuthType const authType) {
    try {
        _body = HttpRequestBody(_req);
        if (authType == HttpAuthType::REQUIRED) _enforceAuthorization();
        json result = executeImpl(subModuleName);
        _sendData(result);
    } catch (AuthError const& ex) {
        _sendError(__func__, "failed to pass authorization requirements, ex: " + string(ex.what()));
    } catch (HttpError const& ex) {
        _sendError(ex.func(), ex.what(), ex.errorExt());
    } catch (invalid_argument const& ex) {
        _sendError(__func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(__func__, "operation failed due to: " + string(ex.what()));
    }
}

void HttpModuleBase::checkApiVersion(string const& func, unsigned int minVersion) const {
    unsigned int const maxVersion = HttpMetaModule::version;
    unsigned int version = 0;
    string const versionAttrName = "version";
    json const errorEx = json::object({{"min_version", minVersion}, {"max_version", maxVersion}});

    // Intercept exceptions thrown when converting the attribute's value (if provided)
    // in order to inject the allowed range of the version numbers into the extended
    // error sent back to the caller.
    //
    // Note that requests sent w/o explicitly specified API version will still be
    // processed. In this case a warning will be sent in the response object.
    try {
        if (req()->method == "GET") {
            if (!query().has(versionAttrName)) {
                _warningOnVersionMissing = "No version number was provided in the request's query.";
                warn(_warningOnVersionMissing);
                return;
            }
            version = query().requiredUInt(versionAttrName);
        } else {
            if (!body().has(versionAttrName)) {
                _warningOnVersionMissing = "No version number was provided in the request's body.";
                warn(_warningOnVersionMissing);
                return;
            }
            version = body().required<unsigned int>(versionAttrName);
        }
    } catch (...) {
        throw HttpError(func, "The required parameter " + versionAttrName + " is not a number.", errorEx);
    }
    if (!(minVersion <= version && version <= maxVersion)) {
        throw HttpError(func,
                        "The requested version " + to_string(version) +
                                " of the API is not in the range supported by the service.",
                        errorEx);
    }
}

void HttpModuleBase::info(string const& msg) const { LOGS(_log, LOG_LVL_INFO, context() << msg); }

void HttpModuleBase::debug(string const& msg) const { LOGS(_log, LOG_LVL_DEBUG, context() << msg); }

void HttpModuleBase::warn(string const& msg) const { LOGS(_log, LOG_LVL_WARN, context() << msg); }

void HttpModuleBase::error(string const& msg) const { LOGS(_log, LOG_LVL_ERROR, context() << msg); }

void HttpModuleBase::_sendError(string const& func, string const& errorMsg, json const& errorExt) const {
    error(func, errorMsg);
    json result;
    result["success"] = 0;
    result["error"] = errorMsg;
    result["error_ext"] = errorExt.is_null() ? json::object() : errorExt;
    result["warning"] = _warningOnVersionMissing;
    resp()->send(result.dump(), "application/json");
}

void HttpModuleBase::_sendData(json& result) {
    result["success"] = 1;
    result["error"] = "";
    result["error_ext"] = json::object();
    result["warning"] = _warningOnVersionMissing;
    resp()->send(result.dump(), "application/json");
}

void HttpModuleBase::_enforceAuthorization() {
    if (body().has("admin_auth_key")) {
        auto const adminAuthKey = body().required<string>("admin_auth_key");
        if (adminAuthKey != _adminAuthKey) {
            throw AuthError(context() +
                            "administrator's authorization key 'admin_auth_key' in the request"
                            " doesn't match the one in server configuration");
        }
        _isAdmin = true;
        return;
    }
    if (body().has("auth_key")) {
        auto const authKey = body().required<string>("auth_key");
        if (authKey != _authKey) {
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

}  // namespace lsst::qserv::replica
