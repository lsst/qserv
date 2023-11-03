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
#include "global/stringUtil.h"
#include "http/Client.h"
#include "http/Exceptions.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"

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
    checkApiVersion(__func__, 14);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const database = query().requiredString("database");
    auto const databaseInfo = config->databaseInfo(database);

    debug(__func__, "database=" + database);

    // Ignore parameters that were never configured for the database.

    auto const getUInt = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] = lsst::qserv::stoui(
                    databaseServices->ingestParam(databaseInfo.name, http::ClientConfig::category, key)
                            .value);
        } catch (DatabaseServicesNotFound const&) {
        }
    };
    auto const getInt = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] =
                    stoi(databaseServices->ingestParam(databaseInfo.name, http::ClientConfig::category, key)
                                 .value);
        } catch (DatabaseServicesNotFound const&) {
        }
    };
    auto const getLong = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] =
                    stol(databaseServices->ingestParam(databaseInfo.name, http::ClientConfig::category, key)
                                 .value);
        } catch (DatabaseServicesNotFound const&) {
        }
    };
    auto const getStr = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] =
                    databaseServices->ingestParam(databaseInfo.name, http::ClientConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {
        }
    };

    json result({{"database", databaseInfo.name}});

    getInt(result, http::ClientConfig::sslVerifyHostKey);
    getInt(result, http::ClientConfig::sslVerifyPeerKey);
    getStr(result, http::ClientConfig::caPathKey);
    getStr(result, http::ClientConfig::caInfoKey);
    getStr(result, http::ClientConfig::caInfoValKey);

    getInt(result, http::ClientConfig::proxySslVerifyHostKey);
    getInt(result, http::ClientConfig::proxySslVerifyPeerKey);
    getStr(result, http::ClientConfig::proxyCaPathKey);
    getStr(result, http::ClientConfig::proxyCaInfoKey);
    getStr(result, http::ClientConfig::proxyCaInfoValKey);

    getStr(result, http::ClientConfig::proxyKey);
    getStr(result, http::ClientConfig::noProxyKey);
    getLong(result, http::ClientConfig::httpProxyTunnelKey);

    getLong(result, http::ClientConfig::connectTimeoutKey);
    getLong(result, http::ClientConfig::timeoutKey);
    getLong(result, http::ClientConfig::lowSpeedLimitKey);
    getLong(result, http::ClientConfig::lowSpeedTimeKey);
    getUInt(result, http::ClientConfig::asyncProcLimitKey);

    return json({{"config", result}});
}

json HttpIngestConfigModule::_update() {
    debug(__func__);
    checkApiVersion(__func__, 14);

    auto const database = body().required<string>("database");
    debug(__func__, "database=" + database);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const databaseInfo = config->databaseInfo(database);

    auto const update = [&](string const& key, string const& val) {
        debug(__func__, key + "=" + val);
        databaseServices->saveIngestParam(databaseInfo.name, http::ClientConfig::category, key, val);
    };
    auto const updateUInt = [&](string const& key) {
        if (body().has(key)) update(key, to_string(body().required<unsigned int>(key)));
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
    updateInt(http::ClientConfig::sslVerifyHostKey);
    updateInt(http::ClientConfig::sslVerifyPeerKey);
    updateStr(http::ClientConfig::caPathKey);
    updateStr(http::ClientConfig::caInfoKey);
    updateStr(http::ClientConfig::caInfoValKey);

    updateInt(http::ClientConfig::proxySslVerifyHostKey);
    updateInt(http::ClientConfig::proxySslVerifyPeerKey);
    updateStr(http::ClientConfig::proxyCaPathKey);
    updateStr(http::ClientConfig::proxyCaInfoKey);
    updateStr(http::ClientConfig::proxyCaInfoValKey);

    updateStr(http::ClientConfig::proxyKey);
    updateStr(http::ClientConfig::noProxyKey);
    updateLong(http::ClientConfig::httpProxyTunnelKey);

    updateLong(http::ClientConfig::connectTimeoutKey);
    updateLong(http::ClientConfig::timeoutKey);
    updateLong(http::ClientConfig::lowSpeedLimitKey);
    updateLong(http::ClientConfig::lowSpeedTimeKey);
    updateUInt(http::ClientConfig::asyncProcLimitKey);

    return json::object();
}

}  // namespace lsst::qserv::replica
