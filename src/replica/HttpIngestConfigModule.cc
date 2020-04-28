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

    return json({{"config", result}});
}


json HttpIngestConfigModule::_update() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const database = body().required<string>("database");
    auto const databaseInfo = config->databaseInfo(database);

    debug(__func__, "database=" + database);

    auto const updateInt = [&](string const& context, string const& key) {
        if (body().has(key)) {
            string const val = to_string(body().required<int>(key));
            debug(context, key + "=" + val);
            databaseServices->saveIngestParam(
                    databaseInfo.name, HttpFileReaderConfig::category, key, val);
        }
    };
    auto const updateStr = [&](string const& context, string const& key) {
        if (body().has(key)) {
            string const val = body().required<string>(key);
            debug(context, key + "=" + val);
            databaseServices->saveIngestParam(
                    databaseInfo.name, HttpFileReaderConfig::category, key, val);
        }
    };
    updateInt(__func__, HttpFileReaderConfig::sslVerifyHostKey);
    updateInt(__func__, HttpFileReaderConfig::sslVerifyPeerKey);
    updateStr(__func__, HttpFileReaderConfig::caPathKey);
    updateStr(__func__, HttpFileReaderConfig::caInfoKey);
    updateStr(__func__, HttpFileReaderConfig::caInfoValKey);

    updateInt(__func__, HttpFileReaderConfig::proxySslVerifyHostKey);
    updateInt(__func__, HttpFileReaderConfig::proxySslVerifyPeerKey);
    updateStr(__func__, HttpFileReaderConfig::proxyCaPathKey);
    updateStr(__func__, HttpFileReaderConfig::proxyCaInfoKey);
    updateStr(__func__, HttpFileReaderConfig::proxyCaInfoValKey);

    return json::object();
}

}}}  // namespace lsst::qserv::replica
