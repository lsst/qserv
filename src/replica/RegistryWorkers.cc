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
#include "replica/RegistryWorkers.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void RegistryWorkers::update(string const& name, json const& workerInfo) {
    string const context = "RegistryWorkers::" + string(__func__) + " ";
    if (name.empty()) throw invalid_argument(context + "worker name is empty.");
    if (!workerInfo.is_object()) throw invalid_argument(context + "not a valid JSON object.");
    replica::Lock const lock(_mtx, context);
    if (!_workers.contains(name)) _workers[name] = json::object();
    json& worker = _workers[name];
    for (auto&& [key, val] : workerInfo.items()) {
        worker[key] = val;
    }
}

void RegistryWorkers::remove(std::string const& name) {
    string const context = "RegistryWorkers::" + string(__func__) + " ";
    if (name.empty()) throw invalid_argument(context + "worker name is empty.");
    replica::Lock const lock(_mtx, context);
    _workers.erase(name);
}

json RegistryWorkers::workers() const {
    string const context = "RegistryWorkers::" + string(__func__) + " ";
    replica::Lock const lock(_mtx, context);
    return _workers;
}

}  // namespace lsst::qserv::replica
