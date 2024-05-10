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
#include "replica/registry/RegistryServices.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

#define CONTEXT_ ("RegistryServices::" + string(__func__) + " ")

namespace lsst::qserv::replica {

void RegistryServices::updateWorker(string const& name, json const& workerInfo) {
    if (name.empty()) throw invalid_argument(CONTEXT_ + "worker name is empty.");
    if (!workerInfo.is_object()) throw invalid_argument(CONTEXT_ + "not a valid JSON object.");
    replica::Lock const lock(_mtx, CONTEXT_);
    if (!_services["workers"].contains(name)) _services["workers"][name] = json::object();
    json& worker = _services["workers"][name];
    for (auto&& [key, val] : workerInfo.items()) {
        worker[key] = val;
    }
}

void RegistryServices::removeWorker(string const& name) {
    if (name.empty()) throw invalid_argument(CONTEXT_ + "worker name is empty.");
    replica::Lock const lock(_mtx, CONTEXT_);
    _services["workers"].erase(name);
}

void RegistryServices::updateCzar(string const& name, json const& czarInfo) {
    if (!czarInfo.is_object()) throw invalid_argument(CONTEXT_ + "not a valid JSON object.");
    replica::Lock const lock(_mtx, CONTEXT_);
    _services["czars"][name] = czarInfo;
}

void RegistryServices::removeCzar(std::string const& name) {
    replica::Lock const lock(_mtx, CONTEXT_);
    _services["czars"].erase(name);
}

void RegistryServices::updateController(string const& name, json const& controllerInfo) {
    if (!controllerInfo.is_object()) throw invalid_argument(CONTEXT_ + "not a valid JSON object.");
    replica::Lock const lock(_mtx, CONTEXT_);
    _services["controllers"][name] = controllerInfo;
}

void RegistryServices::removeController(string const& name) {
    replica::Lock const lock(_mtx, CONTEXT_);
    _services["controllers"].erase(name);
}

json RegistryServices::toJson() const {
    replica::Lock const lock(_mtx, CONTEXT_);
    return _services;
}

}  // namespace lsst::qserv::replica
