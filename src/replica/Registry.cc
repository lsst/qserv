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
#include "replica/Registry.h"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ConfigWorker.h"
#include "replica/HttpClient.h"

// LSST headers
#include "lsst/log/Log.h"

// Standard headers
#include <netdb.h>
#include <stdexcept>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <thread>

// Third-party headers
#include "boost/asio.hpp"

using namespace std;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Registry");

string context(string const& func) { return "REGISTRY " + func + " "; }

/**
 * @brief Get the FQDN of the current host.
 * @note If there will be more than one canonical name associated with the host
 *   then  the result will contain them all separated by comma, such as in:
 * @code
 *   <fqdn-1>,<fqdn-2>,...
 * @endcode
 * @return The FQDN (or FQDNs)
 * @throws std::runtime_error In case if the information couldn't be retreived.
 */
string get_current_host_fqdn() {
    // Get the short name of the current host first.
    boost::system::error_code ec;
    string const hostname = boost::asio::ip::host_name(ec);
    if (ec.value() != 0) {
        throw runtime_error("Registry::" + string(__func__) +
                            " boost::asio::ip::host_name failed: " + ec.category().name() + string(":") +
                            to_string(ec.value()) + "[" + ec.message() + "]");
    }

    // Get the host's FQDN(s)
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;     /* either IPV4 or IPV6 */
    hints.ai_socktype = SOCK_STREAM; /* IP */
    hints.ai_flags = AI_CANONNAME;   /* canonical name */
    struct addrinfo* info;
    while (true) {
        int const retCode = getaddrinfo(hostname.data(), "http", &hints, &info);
        if (retCode == 0) break;
        if (retCode == EAI_AGAIN) continue;
        throw runtime_error("Registry::" + string(__func__) +
                            " getaddrinfo failed: " + gai_strerror(retCode));
    }
    string fqdn;
    for (struct addrinfo* p = info; p != NULL; p = p->ai_next) {
        if (!fqdn.empty()) fqdn += ",";
        fqdn += p->ai_canonname;
    }
    freeaddrinfo(info);
    return fqdn;
}

}  // namespace

namespace lsst::qserv::replica {

Registry::Ptr Registry::create(ServiceProvider::Ptr const& serviceProvider) {
    return Ptr(new Registry(serviceProvider));
}

Registry::Registry(ServiceProvider::Ptr const& serviceProvider)
        : _serviceProvider(serviceProvider),
          _baseUrl("http://" + serviceProvider->config()->get<string>("registry", "host") + ":" +
                   to_string(serviceProvider->config()->get<uint16_t>("registry", "port"))) {}

vector<WorkerInfo> Registry::workers() const {
    vector<WorkerInfo> coll;
    json const resultJson = _request("GET", "/workers?instance_id=" + _serviceProvider->instanceId());
    for (auto const& [name, workerJson] : resultJson.at("workers").items()) {
        string const host = workerJson.at("host").get<string>();
        WorkerInfo worker;
        if (_serviceProvider->config()->isKnownWorker(name)) {
            worker = _serviceProvider->config()->workerInfo(name);
        } else {
            worker.name = name;
        }
        worker.svcHost.addr = host;
        worker.svcHost.name = workerJson.at("svc-host-name").get<string>();
        worker.svcPort = workerJson.at("svc-port").get<uint16_t>();
        worker.fsHost.addr = host;
        worker.fsHost.name = workerJson.at("fs-host-name").get<string>();
        worker.fsPort = workerJson.at("fs-port").get<uint16_t>();
        worker.dataDir = workerJson.at("data-dir").get<string>();
        worker.loaderHost.addr = host;
        worker.loaderHost.name = workerJson.at("loader-host-name").get<string>();
        worker.loaderPort = workerJson.at("loader-port").get<uint16_t>();
        worker.loaderTmpDir = workerJson.at("loader-tmp-dir").get<string>();
        worker.exporterHost.addr = host;
        worker.exporterHost.name = workerJson.at("exporter-host-name").get<string>();
        worker.exporterPort = workerJson.at("exporter-port").get<uint16_t>();
        worker.exporterTmpDir = workerJson.at("exporter-tmp-dir").get<string>();
        worker.httpLoaderHost.addr = host;
        worker.httpLoaderHost.name = workerJson.at("http-loader-host-name").get<string>();
        worker.httpLoaderPort = workerJson.at("http-loader-port").get<uint16_t>();
        worker.httpLoaderTmpDir = workerJson.at("http-loader-tmp-dir").get<string>();
        coll.push_back(std::move(worker));
    }
    return coll;
}

void Registry::add(string const& name) const {
    string const hostName = ::get_current_host_fqdn();
    auto const config = _serviceProvider->config();
    json const request =
            json::object({{"instance_id", _serviceProvider->instanceId()},
                          {"auth_key", _serviceProvider->authKey()},
                          {"worker",
                           {{"name", name},
                            {"svc-host-name", hostName},
                            {"svc-port", config->get<uint16_t>("worker", "svc-port")},
                            {"fs-host-name", hostName},
                            {"fs-port", config->get<uint16_t>("worker", "fs-port")},
                            {"data-dir", config->get<string>("worker", "data-dir")},
                            {"loader-host-name", hostName},
                            {"loader-port", config->get<uint16_t>("worker", "loader-port")},
                            {"loader-tmp-dir", config->get<string>("worker", "loader-tmp-dir")},
                            {"exporter-host-name", hostName},
                            {"exporter-port", config->get<uint16_t>("worker", "exporter-port")},
                            {"exporter-tmp-dir", config->get<string>("worker", "exporter-tmp-dir")},
                            {"http-loader-host-name", hostName},
                            {"http-loader-port", config->get<uint16_t>("worker", "http-loader-port")},
                            {"http-loader-tmp-dir", config->get<string>("worker", "http-loader-tmp-dir")}}}});
    _request("POST", "/worker", request);
}

void Registry::remove(string const& name) const {
    json const request = json::object(
            {{"instance_id", _serviceProvider->instanceId()}, {"auth_key", _serviceProvider->authKey()}});
    _request("DELETE", "/worker/" + name, request);
}

json Registry::_request(string const& method, string const& resource, json const& request) const {
    string const url = _baseUrl + resource;
    vector<string> const headers =
            request.empty() ? vector<string>({}) : vector<string>({"Content-Type: application/json"});
    HttpClient client(method, url, request.empty() ? string() : request.dump(), headers);
    json const response = client.readAsJson();
    if (0 == response.at("success").get<int>()) {
        string const msg = ::context(__func__) + "'" + method + "' request to '" + url +
                           "' failed, error: '" + response.at("error").get<string>() + "'.";
        LOGS(_log, LOG_LVL_ERROR, msg);
        throw runtime_error(msg);
    }
    return response;
}

}  // namespace lsst::qserv::replica
