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
#include "replica/HttpModule.h"

// Qserv headers
#include "css/CssAccess.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpModule");
}

namespace lsst::qserv::replica {

HttpModule::HttpModule(Controller::Ptr const& controller, string const& taskName,
                       HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp)
        : EventLogger(controller, taskName),
          HttpModuleBase(controller->serviceProvider()->authKey(),
                         controller->serviceProvider()->adminAuthKey(), req, resp),
          _processorConfig(processorConfig) {}

string HttpModule::context() const { return name() + " "; }

database::mysql::Connection::Ptr HttpModule::qservMasterDbConnection(string const& database) const {
    return database::mysql::Connection::open(Configuration::qservCzarDbParams(database));
}

shared_ptr<css::CssAccess> HttpModule::qservCssAccess(bool readOnly) const {
    auto const config = controller()->serviceProvider()->config();
    // Use all parmeters of the connection from the czar's MySQL connection parameters object.
    auto const connectionParams = Configuration::qservCzarDbParams("qservCssData");
    map<string, string> cssConfig;
    cssConfig["technology"] = "mysql";
    // Address translation is required because CSS MySQL connector doesn't set
    // the TCP protocol option for 'localhost' and tries to connect via UNIX socket.
    cssConfig["hostname"] = connectionParams.host == "localhost" ? "127.0.0.1" : connectionParams.host;
    cssConfig["port"] = to_string(connectionParams.port);
    cssConfig["username"] = connectionParams.user;
    cssConfig["password"] = connectionParams.password;
    cssConfig["database"] = connectionParams.database;
    return css::CssAccess::createFromConfig(cssConfig, config->get<string>("controller", "empty-chunks-dir"));
}

bool HttpModule::autoBuildSecondaryIndex(string const& database) const {
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    try {
        DatabaseIngestParam const paramInfo =
                databaseServices->ingestParam(database, "secondary-index", "auto-build");
        return paramInfo.value != "0";
    } catch (DatabaseServicesNotFound const& ex) {
        info(__func__, "the secondary index auto-build mode was not specified");
    }
    return false;
}

bool HttpModule::localLoadSecondaryIndex(string const& database) const {
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    try {
        DatabaseIngestParam const paramInfo =
                databaseServices->ingestParam(database, "secondary-index", "local-load");
        return paramInfo.value != "0";
    } catch (DatabaseServicesNotFound const& ex) {
        info(__func__, "the secondary index local-load mode was not specified");
    }
    return false;
}

DatabaseInfo HttpModule::getDatabaseInfo(string const& func, bool throwIfPublished) const {
    debug(func);
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();
    string database;
    if (body().has("database")) {
        database = body().required<string>("database");
    } else {
        if (!body().has("transaction_id")) {
            throw invalid_argument(context() + "::" + func +
                                   " this service expects either 'database' or "
                                   " 'transaction_id' to be provided to define a scope of the request.");
        }
        TransactionId const transactionId = body().required<TransactionId>("transaction_id");
        debug(func, "transactionId=" + to_string(transactionId));
        auto const transactionInfo = databaseServices->transaction(transactionId);
        database = transactionInfo.database;
    }
    debug(func, "database=" + database);

    auto const databaseInfo = config->databaseInfo(database);
    if (throwIfPublished && databaseInfo.isPublished) {
        throw HttpError(context() + "::" + func, "database '" + databaseInfo.name + " is already published.");
    }
    return databaseInfo;
}

}  // namespace lsst::qserv::replica
