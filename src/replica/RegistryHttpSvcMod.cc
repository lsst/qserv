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
#include "replica/RegistryWorkers.h"
#include "util/TimeUtils.h"

// System headers
#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <vector>

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

/**
 * Check if a key is one of the special attributes related to the security
 * context of the workers registration protocol.
 * @param key The key to be checked.
 * @return 'true' if the key is one of the special keys.
 */
bool isSecurityContextKey(string const& key) {
    vector<string> const securityContextKeys = {"authKey", "adminAuthKey", "instance_id", "name"};
    return securityContextKeys.end() != find(securityContextKeys.begin(), securityContextKeys.end(), key);
}

}  // namespace

namespace lsst::qserv::replica {

void RegistryHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                                 qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                 string const& subModuleName, http::AuthType const authType) {
    RegistryHttpSvcMod module(serviceProvider, workers, req, resp);
    module.execute(subModuleName, authType);
}

RegistryHttpSvcMod::RegistryHttpSvcMod(ServiceProvider::Ptr const& serviceProvider, RegistryWorkers& workers,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : http::ModuleBase(serviceProvider->authKey(), serviceProvider->adminAuthKey(), req, resp),
          _serviceProvider(serviceProvider),
          _workers(workers) {}

string RegistryHttpSvcMod::context() const { return "REGISTRY-HTTP-SVC "; }

json RegistryHttpSvcMod::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    enforceInstanceId(func, _serviceProvider->instanceId());
    if (subModuleName == "WORKERS")
        return _getWorkers();
    else if (subModuleName == "ADD-WORKER")
        return _addWorker("replication");
    else if (subModuleName == "ADD-QSERV-WORKER")
        return _addWorker("qserv");
    else if (subModuleName == "DELETE-WORKER")
        return _deleteWorker();
    throw invalid_argument(context() + "unsupported sub-module: '" + subModuleName + "'");
}

json RegistryHttpSvcMod::_getWorkers() const { return json::object({{"workers", _workers.workers()}}); }

json RegistryHttpSvcMod::_addWorker(string const& kind) {
    json const worker = body().required<json>("worker");
    string const name = worker.at("name").get<string>();
    string const host = ::senderIpAddr(req());
    uint64_t const loggedTime = util::TimeUtils::now();

    debug(__func__, "[" + kind + "] name:        " + name);
    debug(__func__, "[" + kind + "] host:        " + host);
    debug(__func__, "[" + kind + "] logged_time: " + to_string(loggedTime));

    // Prepare the payload to be merged into the worker registration entry.
    // Note that the merged payload is cleaned from any security-related contents.
    json workerEntry = json::object({{kind, json::object({{"host", host}, {"logged_time", loggedTime}})}});
    for (auto&& [key, val] : worker.items()) {
        if (!::isSecurityContextKey(key)) workerEntry[kind][key] = val;
    }
    _workers.update(name, workerEntry);
    return json::object({{"workers", _workers.workers()}});
}

json RegistryHttpSvcMod::_deleteWorker() {
    string const name = params().at("name");
    debug(__func__, " name: " + name);
    _workers.remove(name);
    return json::object({{"workers", _workers.workers()}});
}

}  // namespace lsst::qserv::replica
