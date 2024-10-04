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
#include "http/Module.h"

// Qserv headers
#include "http/Exceptions.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::http {

Module::Module(string const& authKey, string const& adminAuthKey) : BaseModule(authKey, adminAuthKey) {}

void Module::execute(string const& subModuleName, http::AuthType const authType) {
    try {
        _parseRequestBodyJSON();
        enforceAuthorization(authType);
        json result = executeImpl(subModuleName);
        sendData(result);
    } catch (AuthError const& ex) {
        sendError(__func__, "failed to pass authorization requirements, ex: " + string(ex.what()));
    } catch (http::Error const& ex) {
        sendError(ex.func(), ex.what(), ex.errorExt());
    } catch (invalid_argument const& ex) {
        sendError(__func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        sendError(__func__, "operation failed due to: " + string(ex.what()));
    }
}

void Module::_parseRequestBodyJSON() {
    string content;
    getRequestBody(content, "application/json");
    if (!content.empty()) {
        try {
            body().objJson = json::parse(content);
            if (body().objJson.is_null() || body().objJson.is_object()) return;
        } catch (...) {
            // Not really interested in knowing specific details of the exception.
            // All what matters here is that the string can't be parsed into
            // a valid JSON object. This will be reported via another exception
            // after this block ends.
            ;
        }
        throw invalid_argument("invalid format of the request body. A simple JSON object was expected");
    }
}

}  // namespace lsst::qserv::http
