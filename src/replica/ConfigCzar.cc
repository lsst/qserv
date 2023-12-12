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
#include "replica/ConfigCzar.h"

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/ConfigParserUtils.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

ConfigCzar::ConfigCzar(json const& obj) {
    string const context = "ConfigCzar::" + string(__func__) + "[json]: ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    try {
        parseRequired<string>(name, obj, "name");
        parseRequired<string>(host.addr, obj.at("host"), "addr");
        parseRequired<string>(host.name, obj.at("host"), "name");
        parseOptional<uint16_t>(port, obj, "port");
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}

json ConfigCzar::toJson() const {
    json infoJson = json::object();
    infoJson["name"] = name;
    infoJson["host"] = host.toJson();
    infoJson["port"] = port;
    return infoJson;
}

bool ConfigCzar::operator==(ConfigCzar const& other) const {
    return (name == other.name) && (host == other.host) && (port == other.port);
}

ostream& operator<<(ostream& os, ConfigCzar const& info) {
    os << "ConfigCzar: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
