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
#include "http/ChttpModule.h"

// System headers
#include <iterator>

// Third-party headers
#include <httplib.h>

// Qserv headers
#include "http/RequestQuery.h"

using namespace std;

namespace lsst::qserv::http {

ChttpModule::ChttpModule(string const& authKey, string const& adminAuthKey, httplib::Request const& req,
                         httplib::Response& resp)
        : Module(authKey, adminAuthKey), _req(req), _resp(resp) {}

string ChttpModule::method() const { return _req.method; }

unordered_map<string, string> ChttpModule::params() const { return _req.path_params; }

RequestQuery ChttpModule::query() const {
    // TODO: The query parameters in CPP-HTTPLIB are stored in the std::multimap
    // container to allow accumulating values of non-unique keys. For now we need
    // to convert the multimap to the std::unordered_map container. This may result
    // in losing some query parameters if they have the same key but different values.
    // Though, the correct solution is to fix the QHTTP library to support
    // the std::multimap container for query parameters.
    unordered_map<string, string> queryParams;
    for (auto const& [key, value] : _req.params) queryParams[key] = value;
    return RequestQuery(queryParams);
}

void ChttpModule::getRequestBody(string& content, string const& requiredContentType) {
    auto itr = _req.headers.find("Content-Type");
    if (itr != _req.headers.end() && itr->second == requiredContentType) {
        content = _req.body;
    }
}

void ChttpModule::sendResponse(string const& content, string const& contentType) {
    _resp.set_content(content, contentType);
}

}  // namespace lsst::qserv::http
