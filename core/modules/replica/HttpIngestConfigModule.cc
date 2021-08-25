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
#include "replica/HttpExceptions.h"
#include "replica/IngestConfigTypes.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

void HttpIngestConfigModule::process(Controller::Ptr const& controller,
                                     string const& taskName,
                                     HttpProcessorConfig const& processorConfig,
                                     qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp,
                                     string const& subModuleName,
                                     HttpModule::AuthType const authType) {
    HttpIngestConfigModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpIngestConfigModule::HttpIngestConfigModule(Controller::Ptr const& controller,
                                               string const& taskName,
                                               HttpProcessorConfig const& processorConfig,
                                               qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpIngestConfigModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "GET") return _get();
    else if (subModuleName == "UPDATE") return _update();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
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
            obj[key] = stoi(databaseServices->ingestParam(
                    databaseInfo.name, HttpFileReaderConfig::category, key).value);
        } catch (DatabaseServicesNotFound const&) {}
    };
    auto const getLong = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] = stol(databaseServices->ingestParam(
                    databaseInfo.name, HttpFileReaderConfig::category, key).value);
        } catch (DatabaseServicesNotFound const&) {}
    };
    auto const getStr = [&databaseServices, &databaseInfo](json& obj, string const& key) {
        try {
            obj[key] = databaseServices->ingestParam(
                    databaseInfo.name, HttpFileReaderConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {}
    };

    json result({{"database", databaseInfo.name}});

    getInt(result, HttpFileReaderConfig::sslVerifyHostKey);
    getInt(result, HttpFileReaderConfig::sslVerifyPeerKey);
    getStr(result, HttpFileReaderConfig::caPathKey);
    getStr(result, HttpFileReaderConfig::caInfoKey);
    getStr(result, HttpFileReaderConfig::caInfoValKey);

    getInt(result, HttpFileReaderConfig::proxySslVerifyHostKey);
    getInt(result, HttpFileReaderConfig::proxySslVerifyPeerKey);
    getStr(result, HttpFileReaderConfig::proxyCaPathKey);
    getStr(result, HttpFileReaderConfig::proxyCaInfoKey);
    getStr(result, HttpFileReaderConfig::proxyCaInfoValKey);

    getLong(result, HttpFileReaderConfig::connectTimeoutKey);
    getLong(result, HttpFileReaderConfig::timeoutKey);
    getLong(result, HttpFileReaderConfig::lowSpeedLimitKey);
    getLong(result, HttpFileReaderConfig::lowSpeedTimeKey);

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
        databaseServices->saveIngestParam(
            databaseInfo.name,
            HttpFileReaderConfig::category,
            key,
            val);
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
    updateInt(HttpFileReaderConfig::sslVerifyHostKey);
    updateInt(HttpFileReaderConfig::sslVerifyPeerKey);
    updateStr(HttpFileReaderConfig::caPathKey);
    updateStr(HttpFileReaderConfig::caInfoKey);
    updateStr(HttpFileReaderConfig::caInfoValKey);

    updateInt(HttpFileReaderConfig::proxySslVerifyHostKey);
    updateInt(HttpFileReaderConfig::proxySslVerifyPeerKey);
    updateStr(HttpFileReaderConfig::proxyCaPathKey);
    updateStr(HttpFileReaderConfig::proxyCaInfoKey);
    updateStr(HttpFileReaderConfig::proxyCaInfoValKey);

    updateLong(HttpFileReaderConfig::connectTimeoutKey);
    updateLong(HttpFileReaderConfig::timeoutKey);
    updateLong(HttpFileReaderConfig::lowSpeedLimitKey);
    updateLong(HttpFileReaderConfig::lowSpeedTimeKey);

    return json::object();
}

}}}  // namespace lsst::qserv::replica
