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
#include "replica/config/ConfigurationSchema.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

ConfigurationSchema::ConfigurationSchema(nlohmann::json const& schemaJson) : _schemaJson(schemaJson) {}

string ConfigurationSchema::description(string const& category, string const& param) const {
    return _attributeValue<string>(category, param, "description", "");
}

bool ConfigurationSchema::readOnly(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "read-only", 0) != 0;
}

bool ConfigurationSchema::securityContext(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "security-context", 0) != 0;
}

string ConfigurationSchema::defaultValueAsString(string const& category, string const& param) const {
    return json2string("ConfigurationSchema::" + string(__func__) + " category: '" + category + "' param: '" +
                               param + "' ",
                       _attributeValueJson(category, param, "default"));
}

json ConfigurationSchema::defaultConfigData() const {
    json result = json::object();
    for (auto const& [category, inParametersJson] : _schemaJson.items()) {
        json& outParametersJson = result[category];
        for (auto const& [parameter, value] : inParametersJson.items()) {
            outParametersJson[parameter] = value.at("default");
        }
    }
    return result;
}

map<string, set<string>> ConfigurationSchema::parameters() const {
    map<string, set<string>> result;
    json const data = defaultConfigData();
    for (auto const& [category, inParametersJson] : data.items()) {
        for (auto const& [parameter, _] : inParametersJson.items()) {
            result[category].insert(parameter);
        }
    }
    return result;
}

string ConfigurationSchema::json2string(string const& context, json const& obj) const {
    if (obj.is_string()) return obj.get<string>();
    if (obj.is_boolean()) return obj.get<bool>() ? "1" : "0";
    if (obj.is_number_unsigned()) return to_string(obj.get<uint64_t>());
    if (obj.is_number_integer()) return to_string(obj.get<int64_t>());
    if (obj.is_number_float()) return to_string(obj.get<double>());
    throw invalid_argument(context + "unsupported data type of the value: " + obj.dump());
}

bool ConfigurationSchema::_emptyAllowed(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "empty-allowed", 0) != 0;
}

json ConfigurationSchema::_restrictor(string const& category, string const& param) const {
    return _attributeValue<json>(category, param, "restricted", json());
}

json ConfigurationSchema::_attributeValueJson(string const& category, string const& param,
                                              string const& attr) const {
    auto const categoryItr = _schemaJson.find(category);
    if (categoryItr != _schemaJson.end()) {
        auto const paramItr = categoryItr->find(param);
        if (paramItr != categoryItr->end()) {
            auto const attrItr = paramItr->find(attr);
            if (attrItr != paramItr->end()) return *attrItr;
            throw invalid_argument("ConfigurationSchema::" + string(__func__) + " unknown attribute " + attr +
                                   " of parameter " + category + "." + param + ".");
        }
    }
    throw invalid_argument("ConfigurationSchema::" + string(__func__) + " unknown parameter " + category +
                           "." + param + ".");
}

}  // namespace lsst::qserv::replica
