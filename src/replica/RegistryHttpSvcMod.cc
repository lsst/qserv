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
#include "replica/RegistryHttpSvcMod.h"

// Qserv header
#include "qhttp/Request.h"
#include "replica/Performance.h"
#include "replica/RegistryWorkers.h"

// System headers
#include <sstream>
#include <stdexcept>

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv;

namespace {
/// @return requestor's IP address
string senderIpAddr(qhttp::Request::Ptr const& req) {
    ostringstream ss;
    ss << req->remoteAddr.address();
    return ss.str();
}
}  // namespace

namespace lsst::qserv::replica {

void RegistryHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                                 qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                 string const& subModuleName, HttpAuthType const authType) {
    RegistryHttpSvcMod module(serviceProvider, workers, req, resp);
    module.execute(subModuleName, authType);
}

RegistryHttpSvcMod::RegistryHttpSvcMod(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModuleBase(serviceProvider->authKey(), serviceProvider->adminAuthKey(), req, resp),
          _serviceProvider(serviceProvider),
          _workers(workers) {}

string RegistryHttpSvcMod::context() const { return "REGISTRY-HTTP-SVC "; }

json RegistryHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    string const context_ = context() + "::" + string(__func__) + "  ";
    if (subModuleName == "WORKERS") {
        _enforceInstanceId(context_, query().requiredString("instance_id"));
        return _getWorkers();
    } else {
        _enforceInstanceId(context_, body().required<string>("instance_id"));
        if (subModuleName == "ADD-WORKER")
            return _addWorker();
        else if (subModuleName == "DELETE-WORKER")
            return _deleteWorker();
    }
    throw invalid_argument(context_ + "unsupported sub-module: '" + subModuleName + "'");
}

void RegistryHttpSvcMod::_enforceInstanceId(string const& context_, string const& instanceId) const {
    if (_serviceProvider->instanceId() == instanceId) return;
    throw invalid_argument(context_ + "Qserv instance identifier mismatch. Client sent '" + instanceId +
                           "' instead of '" + _serviceProvider->instanceId() + "'.");
}

json RegistryHttpSvcMod::_getWorkers() const { return json::object({{"workers", _workers.workers()}}); }

json RegistryHttpSvcMod::_addWorker() {
    json worker = body().required<json>("worker");
    string const name = worker.at("name").get<string>();
    string const host = ::senderIpAddr(req());
    uint64_t const loggedTime = PerformanceUtils::now();

    debug(__func__, "name:        " + name);
    debug(__func__, "host:        " + host);
    debug(__func__, "logged_time: " + to_string(loggedTime));

    // Inject these special attributed into the object. The rest will be
    // passed through to the controllers.
    worker["host"] = host;
    worker["logged_time"] = loggedTime;

    _workers.insert(worker);
    return json::object({{"workers", _workers.workers()}});
}

json RegistryHttpSvcMod::_deleteWorker() {
    string const name = params().at("name");
    debug(__func__, "name: " + name);
    _workers.remove(name);
    return json::object({{"workers", _workers.workers()}});
}

}  // namespace lsst::qserv::replica
