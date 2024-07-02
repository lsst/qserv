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
#include "http/RequestBodyJSON.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::http {

bool RequestBodyJSON::has(json const& obj, string const& name) const {
    if (!obj.is_object()) {
        throw invalid_argument("RequestBodyJSON::" + string(__func__) +
                               " parameter 'obj' is not a valid JSON object");
    }
    return obj.find(name) != obj.end();
}

bool RequestBodyJSON::has(string const& name) const { return has(objJson, name); }

}  // namespace lsst::qserv::http