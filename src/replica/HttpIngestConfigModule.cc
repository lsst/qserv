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
#include "replica/HttpIngestConfigModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpClient.h"
#include "replica/HttpExceptions.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void HttpIngestConfigModule::process(Controller::Ptr const& controller, string const& taskName,
                                     HttpProcessorConfig const& processorConfig,
                                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                     string const& subModuleName, HttpAuthType const authType) {
    HttpIngestConfigModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpIngestConfigModule::HttpIngestConfigModule(Controller::Ptr const& controller, string const& taskName,
                                               HttpProcessorConfig const& processorConfig,
                                               qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpIngestConfigModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "GET")
        return _get();
    else if (subModuleName == "UPDATE")
        return _update();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpIngestConfigModule::_get() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const database = body().required<string>("database");
    auto const databaseInfo = config->databaseInfo(database);

    debug(__func__, "database=" + database);

    // Ignore parameters that were never configured for the database.

    auto const getInt = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] = stoi(
                    databaseServices->ingestParam(databaseInfo.name, HttpClientConfig::category, key).value);
        } catch (DatabaseServicesNotFound const&) {
        }
    };
    auto const getLong = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] = stol(
                    databaseServices->ingestParam(databaseInfo.name, HttpClientConfig::category, key).value);
        } catch (DatabaseServicesNotFound const&) {
        }
    };
    auto const getStr = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] =
                    databaseServices->ingestParam(databaseInfo.name, HttpClientConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {
        }
    };

    json result({{"database", databaseInfo.name}});

    getInt(result, HttpClientConfig::sslVerifyHostKey);
    getInt(result, HttpClientConfig::sslVerifyPeerKey);
    getStr(result, HttpClientConfig::caPathKey);
    getStr(result, HttpClientConfig::caInfoKey);
    getStr(result, HttpClientConfig::caInfoValKey);

    getInt(result, HttpClientConfig::proxySslVerifyHostKey);
    getInt(result, HttpClientConfig::proxySslVerifyPeerKey);
    getStr(result, HttpClientConfig::proxyCaPathKey);
    getStr(result, HttpClientConfig::proxyCaInfoKey);
    getStr(result, HttpClientConfig::proxyCaInfoValKey);

    getStr(result, HttpClientConfig::proxyKey);
    getStr(result, HttpClientConfig::noProxyKey);
    getLong(result, HttpClientConfig::httpProxyTunnelKey);

    getLong(result, HttpClientConfig::connectTimeoutKey);
    getLong(result, HttpClientConfig::timeoutKey);
    getLong(result, HttpClientConfig::lowSpeedLimitKey);
    getLong(result, HttpClientConfig::lowSpeedTimeKey);

    return json({{"config", result}});
}

json HttpIngestConfigModule::_update() {
    string const context = __func__;
    debug(context);

    auto const database = body().required<string>("database");
    debug(context, "database=" + database);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const databaseInfo = config->databaseInfo(database);

    auto const update = [&](string const& key, string const& val) {
        debug(context, key + "=" + val);
        databaseServices->saveIngestParam(databaseInfo.name, HttpClientConfig::category, key, val);
    };
    auto const updateInt = [&](string const& key) {
        if (body().has(key)) update(key, to_string(body().required<int>(key)));
    };
    auto const updateLong = [&](string const& key) {
        if (body().has(key)) update(key, to_string(body().required<long>(key)));
    };
    auto const updateStr = [&](string const& key) {
        if (body().has(key)) update(key, body().required<string>(key));
    };
    updateInt(HttpClientConfig::sslVerifyHostKey);
    updateInt(HttpClientConfig::sslVerifyPeerKey);
    updateStr(HttpClientConfig::caPathKey);
    updateStr(HttpClientConfig::caInfoKey);
    updateStr(HttpClientConfig::caInfoValKey);

    updateInt(HttpClientConfig::proxySslVerifyHostKey);
    updateInt(HttpClientConfig::proxySslVerifyPeerKey);
    updateStr(HttpClientConfig::proxyCaPathKey);
    updateStr(HttpClientConfig::proxyCaInfoKey);
    updateStr(HttpClientConfig::proxyCaInfoValKey);

    updateStr(HttpClientConfig::proxyKey);
    updateStr(HttpClientConfig::noProxyKey);
    updateLong(HttpClientConfig::httpProxyTunnelKey);

    updateLong(HttpClientConfig::connectTimeoutKey);
    updateLong(HttpClientConfig::timeoutKey);
    updateLong(HttpClientConfig::lowSpeedLimitKey);
    updateLong(HttpClientConfig::lowSpeedTimeKey);

    return json::object();
}

}  // namespace lsst::qserv::replica
