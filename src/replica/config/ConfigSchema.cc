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
#include "replica/config/ConfigSchema.h"

// System headers
#include <algorithm>
#include <stdexcept>
#include <thread>

using namespace std;
using json = nlohmann::json;

namespace {
unsigned int const num_threads = max(1U, thread::hardware_concurrency());
}  // namespace

namespace lsst::qserv::replica {

json ConfigSchema::sharedSchema(std::string const& category) {
    string const context = "ConfigSchema::" + string(__func__);
    if (category.empty()) throw invalid_argument(context + ": Category must not be empty.");
    if (category == "common") {
        json schema = json::object();
        schema["asio-num-threads"] = {
                {"description",
                 "The number of shared threads managed by BOOST ASIO. Must be greater than 0."},
                {"default", min(8U, num_threads)}};
        schema["request-buf-size-bytes"] = {
                {"description",
                 "The default buffer size for network communications. Must be greater than 0."},
                {"default", 4096}};
        return schema;
    }
    if (category == "database") {
        json schema = json::object();
        schema["allow-reconnect"] = {
                {"description",
                 "The flag controlling the behavior of the database connectors. If the value of the"
                 " parameter is not 0 then the connectors will attempt to reconnect to the database"
                 " service in case of a connection failure."},
                {"empty-allowed", 1},
                {"default", 1}};
        schema["connect-timeout-sec"] = {
                {"description",
                 "The maximum duration of time (in seconds) allowed for establishing a connection"
                 " to the database service."},
                {"default", 3600}};
        schema["max-reconnects"] = {
                {"description",
                 "The maximum number of reconnects allowed for the database connectors."
                 " The limit is applied only if the option 'allow-reconnect' is set to a non-zero value."},
                {"default", 1}};
        schema["schema-upgrade-wait"] = {
                {"description",
                 "If the value of the option is 0 and the schema version of the Replication/Ingest "
                 "system's database is either not available or is less than the one expected by the "
                 "application"
                 " then the application will fail right away. Otherwise, the application will "
                 "keep tracking schema version for a duration specified by the option "
                 "--database-schema-upgrade-wait-timeout."
                 " Note that if the schema version found in the database is higher than the "
                 "expected one then the application will fail right away regardless of a value of either "
                 "option."},
                {"empty-allowed", 1},
                {"default", 1}};
        schema["schema-upgrade-wait-timeout"] = {
                {"description",
                 "The duration (in seconds) for which the application will keep tracking schema version "
                 "if the current schema version is less than the expected one and the "
                 "--database-schema-upgrade-wait option is set to a non-zero value."},
                {"default", 3600}};
        schema["services-pool-size"] = {
                {"description", "The pool size at the client database services connector."},
                {"default", max(8U, num_threads)}};
        schema["repl-db-conn"] = {
                {"description",
                 "A connection URL for the MySQL service of the Replication system's database."},
                {"security-context", 1},
                {"default", "mysql://qsreplica@localhost:3306/qservReplica"}};
        return schema;
    }
    if (category == "registry") {
        json schema = json::object();
        schema["host"] = {
                {"description", "The IP address or the DNS host name for the registry's HTTP server."},
                {"default", "localhost"}};
        schema["port"] = {
                {"description", "The port number for the registry's HTTP server. Must be greater than 0."},
                {"default", 25082}};
        schema["heartbeat-ival-sec"] = {
                {"description",
                 "The heartbeat interval for interactions with the workers Registry service. Must be greater "
                 "than 0."},
                {"default", 5}};
        return schema;
    }
    if (category == "security") {
        json schema = json::object();
        schema["auth-key"] = {{"description",
                               "An authorization key for operations affecting the state of Qserv "
                               "or the Replication/Ingest system."},
                              {"empty-allowed", 1},
                              {"security-context", 1},
                              {"default", ""}};
        schema["admin-auth-key"] = {
                {"description",
                 "An administrator-level authorization key for critical operations affecting the state of "
                 "Qserv of the Replication/Ingest system."},
                {"empty-allowed", 1},
                {"security-context", 1},
                {"default", ""}};
        schema["http-user"] = {
                {"description", "The login name of a user for connecting to the Replication service."},
                {"empty-allowed", 1},
                {"default", ""}};
        schema["http-password"] = {
                {"description",
                 "The login password of a user for connecting to the Replication service. The value "
                 "of the password is ignored if the user is not specified. The password will be used for"
                 " authenticating the user. The password can't be empty if the user is specified."},
                {"empty-allowed", 1},
                {"security-context", 1},
                {"default", ""}};
        schema["instance-id"] = {
                {"description",
                 "A unique identifier of a Qserv instance served by the Replication System."
                 " Its value will be passed along various internal communication lines of"
                 " the system to ensure that all services are related to the same instance."
                 " This mechanism also prevents 'cross-talks' between two (or many) Replication"
                 " System's setups in case of an accidental mis-configuration."},
                {"default", "qserv"}};
        return schema;
    }
    throw std::runtime_error(context + ": Unknown configuration category: " + category);
}

ConfigSchema::ConfigSchema(nlohmann::json const& schema) : _schema(schema) {}

string ConfigSchema::description(string const& category, string const& param) const {
    return _attributeValue<string>(category, param, "description", "");
}

bool ConfigSchema::readOnly(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "read-only", 0) != 0;
}

bool ConfigSchema::securityContext(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "security-context", 0) != 0;
}

string ConfigSchema::defaultValueAsString(string const& category, string const& param) const {
    return json2string(
            "ConfigSchema::" + string(__func__) + " category: '" + category + "' param: '" + param + "' ",
            _attributeValueJson(category, param, "default"));
}

json ConfigSchema::defaultConfigData() const {
    json result = json::object();
    for (auto const& [category, inParametersJson] : _schema.items()) {
        json& outParametersJson = result[category];
        for (auto const& [parameter, value] : inParametersJson.items()) {
            outParametersJson[parameter] = value.at("default");
        }
    }
    return result;
}

map<string, set<string>> ConfigSchema::parameters() const {
    map<string, set<string>> result;
    json const data = defaultConfigData();
    for (auto const& [category, inParametersJson] : data.items()) {
        for (auto const& [parameter, _] : inParametersJson.items()) {
            result[category].insert(parameter);
        }
    }
    return result;
}

bool ConfigSchema::exists(string const& category) const {
    auto const categoryItr = _schema.find(category);
    return categoryItr != _schema.end();
}

bool ConfigSchema::exists(string const& category, string const& param) const {
    auto const categoryItr = _schema.find(category);
    if (categoryItr != _schema.end()) {
        auto const paramItr = categoryItr->find(param);
        if (paramItr != categoryItr->end()) return true;
    }
    return false;
}

string ConfigSchema::json2string(string const& context, json const& obj) const {
    if (obj.is_string()) return obj.get<string>();
    if (obj.is_boolean()) return obj.get<bool>() ? "1" : "0";
    if (obj.is_number_unsigned()) return to_string(obj.get<uint64_t>());
    if (obj.is_number_integer()) return to_string(obj.get<int64_t>());
    if (obj.is_number_float()) return to_string(obj.get<double>());
    throw invalid_argument(context + "unsupported data type of the value: " + obj.dump());
}

bool ConfigSchema::_emptyAllowed(string const& category, string const& param) const {
    return _attributeValue<unsigned int>(category, param, "empty-allowed", 0) != 0;
}

json ConfigSchema::_restrictor(string const& category, string const& param) const {
    return _attributeValue<json>(category, param, "restricted", json());
}

json ConfigSchema::_attributeValueJson(string const& category, string const& param,
                                       string const& attr) const {
    auto const categoryItr = _schema.find(category);
    if (categoryItr != _schema.end()) {
        auto const paramItr = categoryItr->find(param);
        if (paramItr != categoryItr->end()) {
            auto const attrItr = paramItr->find(attr);
            if (attrItr != paramItr->end()) return *attrItr;
            throw invalid_argument("ConfigSchema::" + string(__func__) + " unknown attribute " + attr +
                                   " of parameter " + category + "." + param + ".");
        }
    }
    throw invalid_argument("ConfigSchema::" + string(__func__) + " unknown parameter " + category + "." +
                           param + ".");
}

}  // namespace lsst::qserv::replica
