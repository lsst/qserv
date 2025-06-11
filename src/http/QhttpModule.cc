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
#include "http/QhttpModule.h"

// System headers
#include <iterator>

// Qserv headers
#include "http/RequestQuery.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"

using namespace std;

namespace lsst::qserv::http {

QhttpModule::QhttpModule(string const& authKey, string const& adminAuthKey,
                         shared_ptr<qhttp::Request> const& req, shared_ptr<qhttp::Response> const& resp)
        : Module(authKey, adminAuthKey), _req(req), _resp(resp) {}

string QhttpModule::method() const { return _req->method; }

unordered_map<string, string> QhttpModule::params() const { return _req->params; }

RequestQuery QhttpModule::query() const { return RequestQuery(_req->query); }

string QhttpModule::headerEntry(string const& key) const {
    auto it = _req->header.find(key);
    return (it != _req->header.end()) ? it->second : "";
}

void QhttpModule::getRequestBody(string& content, string const& requiredContentType) {
    if (_req->header["Content-Type"] == requiredContentType) {
        content.clear();
        content.reserve(_req->contentLengthBytes());
        content.append(istreambuf_iterator<char>(_req->content), {});
    }
}

void QhttpModule::sendResponse(string const& content, string const& contentType) {
    _resp->send(content, contentType);
}

}  // namespace lsst::qserv::http
