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
                               " - parameter 'obj' is not a valid JSON object");
    }
    return obj.find(name) != obj.end();
}

bool RequestBodyJSON::has(string const& name) const { return has(objJson, name); }

string RequestBodyJSON::requiredString(string const& name) const {
    json const value = _get(__func__, name);
    if (!value.is_string()) {
        throw invalid_argument("RequestBodyJSON::" + std::string(__func__) + " - a value of the parameter '" +
                               name + "' is not a string");
    }
    return value.get<string>();
}

string RequestBodyJSON::optionalString(string const& name, string const& defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredString(name);
}

bool RequestBodyJSON::requiredBool(std::string const& name) const {
    auto valJson = required<nlohmann::json>(objJson, name);
    if (valJson.is_boolean())
        return valJson.get<bool>();
    else if (valJson.is_number_integer())
        return valJson.get<int64_t>() != 0;
    else if (valJson.is_number_unsigned())
        return valJson.get<uint64_t>() != 0;
    throw std::invalid_argument("RequestBodyJSON::" + std::string(__func__) +
                                " - a value of the parameter '" + name + "' is not a boolean");
}

bool RequestBodyJSON::optionalBool(std::string const& name, bool defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredBool(name);
}

unsigned int RequestBodyJSON::requiredUInt(string const& name) const {
    string const context = "RequestBodyJSON::" + string(__func__) + " ";
    json const value = _get(__func__, name);
    if (value.is_number_unsigned()) {
        return value.get<unsigned int>();
    } else if (value.is_number_integer()) {
        int const ret = value.get<int>();
        if (ret >= 0) return ret;
        throw invalid_argument(context + "- a value of the parameter '" + name + "' is a negative integer");
    } else if (value.is_string()) {
        string const str = value.get<string>();
        try {
            return qserv::stoui(str);
        } catch (...) {
            ;
        }
    }
    throw invalid_argument(
            context + "- a value of the parameter '" + name +
            "' is not an unsigned integer, nor it's a string representation of a non-negative integer");
}

unsigned int RequestBodyJSON::optionalUInt(string const& name, unsigned int defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredUInt(name);
}

int RequestBodyJSON::requiredInt(string const& name) const {
    json const value = _get(__func__, name);
    if (value.is_number_integer()) {
        return value.get<int>();
    } else if (value.is_string()) {
        string const str = value.get<string>();
        try {
            return stoi(str);
        } catch (...) {
            ;
        }
    }
    throw invalid_argument("RequestBodyJSON::" + string(__func__) + " - a value of the parameter '" + name +
                           "' is not a signed integer, nor it's a string representation of a signed integer");
}

int RequestBodyJSON::optionalInt(string const& name, int defaultValue) const {
    if (!has(name)) return defaultValue;
    return requiredInt(name);
}

json RequestBodyJSON::_get(string const& func, string const& name) const {
    if (!has(name)) {
        throw invalid_argument("RequestBodyJSON::" + func + " - parameter '" + name +
                               "' is missing in the request body");
    }
    return objJson.at(name);
}

}  // namespace lsst::qserv::http
