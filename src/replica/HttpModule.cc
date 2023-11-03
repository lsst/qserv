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
#include "http/Exceptions.h"
#include "replica/Configuration.h"
#include "replica/ConfigDatabase.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/ServiceManagementJob.h"
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

string HttpModule::reconfigureWorkers(DatabaseInfo const& databaseInfo, bool allWorkers,
                                      unsigned int workerResponseTimeoutSec) const {
    string const noParentJobId;
    auto const job = ServiceReconfigJob::create(
            allWorkers, workerResponseTimeoutSec, controller(), noParentJobId, nullptr,
            controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr : resultData.workers) {
        auto&& worker = itr.first;
        auto&& success = itr.second;
        if (not success) {
            error += "reconfiguration failed on worker: " + worker + " ";
        }
    }
    return error;
}

bool HttpModule::autoBuildDirectorIndex(string const& databaseName) const {
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    try {
        DatabaseIngestParam const paramInfo =
                databaseServices->ingestParam(databaseName, "secondary-index", "auto-build");
        return paramInfo.value != "0";
    } catch (DatabaseServicesNotFound const& ex) {
        info(__func__, "the director index auto-build mode was not specified");
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
        throw http::Error(context() + "::" + func,
                          "database '" + databaseInfo.name + " is already published.");
    }
    return databaseInfo;
}

}  // namespace lsst::qserv::replica
