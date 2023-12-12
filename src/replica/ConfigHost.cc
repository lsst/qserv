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
#include "replica/ConfigHost.h"

// System headers
#include <iostream>
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

json ConfigHost::toJson() const {
    json infoJson;
    infoJson["addr"] = addr;
    infoJson["name"] = name;
    return infoJson;
}

bool ConfigHost::operator==(ConfigHost const& other) const {
    return (addr == other.addr) && (name == other.name);
}

ostream& operator<<(ostream& os, ConfigHost const& info) {
    os << "ConfigHost: " << info.toJson().dump();
    return os;
}

}  // namespace lsst::qserv::replica
