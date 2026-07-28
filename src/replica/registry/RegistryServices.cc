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

// Qserv headers
#include "http/Exceptions.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

#define CONTEXT_ ("RegistryServices::" + string(__func__))

namespace lsst::qserv::replica {

void RegistryServices::updateWorker(string const& id, json const& desc) {
    _update(CONTEXT_, "workers", id, desc);
}

void RegistryServices::removeWorker(string const& id) { _remove(CONTEXT_, "workers", id); }

void RegistryServices::updateCzar(string const& id, json const& desc) {
    _update(CONTEXT_, "czars", id, desc);
}

void RegistryServices::removeCzar(std::string const& id) { _remove(CONTEXT_, "czars", id); }

void RegistryServices::updateController(string const& id, json const& desc) {
    _update(CONTEXT_, "controllers", id, desc);
}

void RegistryServices::removeController(string const& id) { _remove(CONTEXT_, "controllers", id); }

json RegistryServices::toJson() const {
    replica::Lock const lock(_mtx, CONTEXT_);
    return _services;
}

void RegistryServices::_update(string const& func, string const& serviceType, string const& serviceId,
                               json const& desc) {
    if (serviceId.empty()) {
        throw invalid_argument(func + ": serviceId is empty for serviceType=" + serviceType);
    }
    if (!desc.is_object()) {
        throw invalid_argument(func + ": the descriptor for serviceId=" + serviceId +
                               " of serviceType=" + serviceType + " is not a valid JSON object.");
    }
    replica::Lock const lock(_mtx, func);
    if (!_services[serviceType].contains(serviceId)) {
        _services[serviceType][serviceId] = json::object();
    }
    json& entry = _services[serviceType][serviceId];
    for (auto&& [key, val] : desc.items()) {
        entry[key] = val;
    }
}

void RegistryServices::_remove(string const& func, string const& serviceType, string const& serviceId) {
    if (serviceId.empty())
        throw invalid_argument(func + ": serviceId is empty for serviceType=" + serviceType);
    replica::Lock const lock(_mtx, func);
    if (_services[serviceType].erase(serviceId) == 0) {
        throw http::ErrorNotFound404(func, "no entry for serviceId=" + serviceId + " of serviceType=" +
                                                   serviceType + " was found in the Registry");
    }
}

}  // namespace lsst::qserv::replica
