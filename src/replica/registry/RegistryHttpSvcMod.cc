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
#include "replica/registry/RegistryHttpSvcMod.h"

// Qserv header
#include "global/stringUtil.h"
#include "http/Auth.h"
#include "qhttp/Request.h"
#include "replica/registry/RegistryServices.h"
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
string senderIpAddr(shared_ptr<qhttp::Request> const& req) {
    ostringstream ss;
    ss << req->remoteAddr.address();
    return ss.str();
}

/**
 * Check if a key is one of the special attributes related to the security
 * context of the services registration protocol.
 * @param key The key to be checked.
 * @return 'true' if the key is one of the special keys.
 */
bool isSecurityContextKey(string const& key) {
    vector<string> const securityContextKeys = {"authKey", "adminAuthKey", "instance_id", "name"};
    return securityContextKeys.end() != find(securityContextKeys.begin(), securityContextKeys.end(), key);
}
}  // namespace

namespace lsst::qserv::replica {

void RegistryHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider, RegistryServices& services,
                                 shared_ptr<qhttp::Request> const& req,
                                 shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                 http::AuthType const authType) {
    RegistryHttpSvcMod module(serviceProvider, services, req, resp);
    module.execute(subModuleName, authType);
}

RegistryHttpSvcMod::RegistryHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                                       RegistryServices& services, shared_ptr<qhttp::Request> const& req,
                                       shared_ptr<qhttp::Response> const& resp)
        : http::QhttpModule(serviceProvider->httpAuthContext(), req, resp),
          _serviceProvider(serviceProvider),
          _services(services) {}

string RegistryHttpSvcMod::context() const { return "REGISTRY-HTTP-SVC "; }

json RegistryHttpSvcMod::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    enforceInstanceId(func, _serviceProvider->instanceId());
    if (subModuleName == "SERVICES")
        return _getServices();
    else if (subModuleName == "ADD-WORKER")
        return _addWorker("replication");
    else if (subModuleName == "ADD-QSERV-WORKER")
        return _addWorker("qserv");
    else if (subModuleName == "DELETE-WORKER")
        return _deleteWorker();
    else if (subModuleName == "ADD-CZAR")
        return _addCzar();
    else if (subModuleName == "DELETE-CZAR")
        return _deleteCzar();
    else if (subModuleName == "ADD-CONTROLLER")
        return _addController();
    else if (subModuleName == "DELETE-CONTROLLER")
        return _deleteController();
    throw invalid_argument(context() + "unsupported sub-module: '" + subModuleName + "'");
}

json RegistryHttpSvcMod::_getServices() const {
    checkApiVersion(__func__, 34);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_addWorker(string const& kind) {
    checkApiVersion(__func__, 34);
    json const worker = body().required<json>("worker");
    string const name = worker.at("name").get<string>();
    string const hostAddr = ::senderIpAddr(req());
    uint64_t const updateTimeMs = util::TimeUtils::now();

    debug(__func__, "[" + kind + "] name:           " + name);
    debug(__func__, "[" + kind + "] host-addr:      " + hostAddr);
    debug(__func__, "[" + kind + "] update-time-ms: " + to_string(updateTimeMs));

    // Prepare the payload to be merged into the worker registration entry.
    // Note that the merged payload is cleaned from any security-related contents.
    json workerEntry =
            json::object({{kind, json::object({{"host-addr", hostAddr}, {"update-time-ms", updateTimeMs}})}});
    for (auto&& [key, val] : worker.items()) {
        if (!::isSecurityContextKey(key)) workerEntry[kind][key] = val;
    }
    _services.updateWorker(name, workerEntry);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_deleteWorker() {
    checkApiVersion(__func__, 34);
    string const name = params().at("name");
    debug(__func__, " name: " + name);
    _services.removeWorker(name);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_addCzar() {
    checkApiVersion(__func__, 34);
    json const czar = body().required<json>("czar");
    string const name = czar.at("name");
    string const hostAddr = ::senderIpAddr(req());
    uint64_t const updateTimeMs = util::TimeUtils::now();

    debug(__func__, "name:           " + name);
    debug(__func__, "host-addr:      " + hostAddr);
    debug(__func__, "update-time-ms: " + to_string(updateTimeMs));

    // Prepare the payload to be merged into the czar registration entry.
    // Note that the merged payload is cleaned from any security-related contents.
    json czarEntry = json::object({{"host-addr", hostAddr}, {"update-time-ms", updateTimeMs}});
    for (auto&& [key, val] : czar.items()) {
        if (!::isSecurityContextKey(key)) czarEntry[key] = val;
    }
    _services.updateCzar(name, czarEntry);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_deleteCzar() {
    checkApiVersion(__func__, 34);
    string const name = params().at("name");
    debug(__func__, " name: " + name);
    _services.removeCzar(name);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_addController() {
    checkApiVersion(__func__, 34);
    json const controller = body().required<json>("controller");
    string const name = controller.at("name");
    string const hostAddr = ::senderIpAddr(req());
    uint64_t const updateTimeMs = util::TimeUtils::now();

    debug(__func__, "name:           " + name);
    debug(__func__, "host-addr:      " + hostAddr);
    debug(__func__, "update-time-ms: " + to_string(updateTimeMs));

    // Prepare the payload to be merged into the Controller registration entry.
    // Note that the merged payload is cleaned from any security-related contents.
    json controllerEntry = json::object({{"host-addr", hostAddr}, {"update-time-ms", updateTimeMs}});
    for (auto&& [key, val] : controller.items()) {
        if (!::isSecurityContextKey(key)) controllerEntry[key] = val;
    }
    _services.updateController(name, controllerEntry);
    return json::object({{"services", _services.toJson()}});
}

json RegistryHttpSvcMod::_deleteController() {
    checkApiVersion(__func__, 34);
    string const name = params().at("name");
    debug(__func__, " name: " + name);
    _services.removeController(name);
    return json::object({{"services", _services.toJson()}});
}

}  // namespace lsst::qserv::replica
