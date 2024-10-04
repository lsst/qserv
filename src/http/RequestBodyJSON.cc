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

// Qserv headers
#include "global/stringUtil.h"

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

unsigned int RequestBodyJSON::requiredUInt(string const& name) const {
    string const context = "RequestBodyJSON::" + string(__func__) + " ";
    json const value = _get(__func__, name);
    if (value.is_number_unsigned()) {
        return value;
    } else if (value.is_number_integer()) {
        int const ret = value;
        if (ret >= 0) return ret;
        throw invalid_argument(context + "a value of the required parameter " + name +
                               " is a negative integer");
    } else if (value.is_string()) {
        string const str = value;
        try {
            return qserv::stoui(str);
        } catch (exception const& ex) {
            ;
        }
    }
    throw invalid_argument(context + "a value of the required parameter " + name +
                           " is not an unsigned integer");
}

unsigned int RequestBodyJSON::optionalUInt(string const& name, unsigned int defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredUInt(name);
}

int RequestBodyJSON::requiredInt(string const& name) const {
    json const value = _get(__func__, name);
    if (value.is_number_integer()) {
        return value;
    } else if (value.is_string()) {
        string const str = value;
        try {
            return stoi(str);
        } catch (exception const& ex) {
            ;
        }
    }
    throw invalid_argument("RequestBodyJSON::" + string(__func__) + " a value of the required parameter " +
                           name + " is not a signed integer");
}

int RequestBodyJSON::optionalInt(string const& name, int defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredInt(name);
}

json RequestBodyJSON::_get(string const& func, string const& name) const {
    if (!has(name)) {
        throw invalid_argument("RequestBodyJSON::" + func + " required parameter " + name +
                               " is missing in the request body");
    }
    return objJson.at(name);
}

}  // namespace lsst::qserv::http