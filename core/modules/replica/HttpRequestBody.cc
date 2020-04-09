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
#include "replica/HttpRequestBody.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

HttpRequestBody::HttpRequestBody(qhttp::Request::Ptr const& req) {

    string const contentType = req->header["Content-Type"];
    string const requiredContentType = "application/json";
    if (contentType != requiredContentType) {
        throw invalid_argument(
                "unsupported content type: '" + contentType + "' instead of: '" +
                requiredContentType + "'");
    }
    // This way of parsing the optional body allows request which have no body
    // in them.
    string content;
    req->content >> content;
    objJson = content.empty() ? json::object() : json::parse(content);
    if (objJson.is_null() or objJson.is_object()) return;
    throw invalid_argument(
            "invalid format of the request body. A simple JSON object was expected");
}

}}} // namespace lsst::qserv::replica