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
#include "replica/contr/HttpConfigurationModule.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "http/Exceptions.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/config/ConfigurationSchema.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/// @return A JSON object with metadata for the general parameters.
json meta4general(Configuration::Ptr const& config) {
    json result;
    for (auto&& itr : ConfigurationSchema::parameters()) {
        string const& category = itr.first;
        for (auto&& parameter : itr.second) {
            json& obj = result[category][parameter];
            obj["read_only"] = ConfigurationSchema::readOnly(category, parameter) ? 0 : 1;
            obj["description"] = ConfigurationSchema::description(category, parameter);
            obj["security_context"] = ConfigurationSchema::securityContext(category, parameter) ? 1 : 0;
        }
    }
    return result;
}
}  // namespace

namespace lsst::qserv::replica {

void HttpConfigurationModule::process(Controller::Ptr const& controller, string const& taskName,
                                      HttpProcessorConfig const& processorConfig,
                                      qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                      string const& subModuleName, http::AuthType const authType) {
    HttpConfigurationModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpConfigurationModule::HttpConfigurationModule(Controller::Ptr const& controller, string const& taskName,
                                                 HttpProcessorConfig const& processorConfig,
                                                 qhttp::Request::Ptr const& req,
                                                 qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpConfigurationModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _get();
    else if (subModuleName == "UPDATE-GENERAL")
        return _updateGeneral();
    else if (subModuleName == "UPDATE-WORKER")
        return _updateWorker();
    else if (subModuleName == "DELETE-WORKER")
        return _deleteWorker();
    else if (subModuleName == "ADD-WORKER")
        return _addWorker();
    else if (subModuleName == "DELETE-DATABASE-FAMILY")
        return _deleteFamily();
    else if (subModuleName == "ADD-DATABASE-FAMILY")
        return _addFamily();
    else if (subModuleName == "DELETE-DATABASE")
        return _deleteDatabase();
    else if (subModuleName == "ADD-DATABASE")
        return _addDatabase();
    else if (subModuleName == "[UN-]PUBLISH-DATABASE")
        return _unpublishDatabase();
    else if (subModuleName == "DELETE-TABLE")
        return _deleteTable();
    else if (subModuleName == "ADD-TABLE")
        return _addTable();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpConfigurationModule::_get() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    json result;
    result["config"] = config->toJson();
    result["config"]["meta"] = meta4general(config);
    return result;
}

json HttpConfigurationModule::_updateGeneral() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    string const category = body().required<string>("category");
    string const parameter = body().required<string>("parameter");
    string const value = body().required<string>("value");
    if (ConfigurationSchema::readOnly(category, parameter)) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "  this is the read-only parameter that can't be changed via this method.");
    }
    config->setFromString(category, parameter, value);
    json result;
    result["config"] = config->toJson();
    result["config"]["meta"] = meta4general(config);
    return result;
}

json HttpConfigurationModule::_updateWorker() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const workerName = params().at("worker");

    // Update requested worker attribute changes into the latest transient state
    // of the worker. Then update worker's configuration in the database.
    ConfigWorker worker = config->worker(workerName);

    // Get optional parameters of the query. Note the default values which
    // are expected to be replaced by actual values provided by a client in
    // parameters found in the query.
    auto const updateBool = [&](string const& func, string const& name, bool& out) {
        int const val = query().optionalInt(name);
        debug(func, name + "=" + to_string(val));
        ConfigWorker::update(val, out);
    };
    updateBool(__func__, "is-enabled", worker.isEnabled);
    updateBool(__func__, "is-read-only", worker.isReadOnly);

    json result;
    result["config"]["workers"][workerName] = config->updateWorker(worker).toJson();
    return result;
}

json HttpConfigurationModule::_deleteWorker() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const workerName = params().at("worker");
    controller()->serviceProvider()->config()->deleteWorker(workerName);
    return json::object();
}

json HttpConfigurationModule::_addWorker() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    ConfigWorker worker;
    worker.name = body().required<string>("worker");
    worker.isEnabled = body().required<int>("is-enabled") != 0;
    worker.isReadOnly = body().required<int>("is-read-only") != 0;

    debug(__func__, "name=" + worker.name);
    debug(__func__, "is-enabled=" + to_string(worker.isEnabled ? 1 : 0));
    debug(__func__, "is-read-only=" + to_string(worker.isReadOnly ? 1 : 0));

    json result;
    result["config"]["workers"][worker.name] =
            controller()->serviceProvider()->config()->addWorker(worker).toJson();
    return result;
}

json HttpConfigurationModule::_deleteFamily() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const familyName = params().at("family");
    controller()->serviceProvider()->config()->deleteDatabaseFamily(familyName);
    return json::object();
}

json HttpConfigurationModule::_addFamily() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    DatabaseFamilyInfo family;
    family.name = body().required<string>("name");
    family.replicationLevel = body().required<unsigned int>("replication_level");
    family.numStripes = body().required<unsigned int>("num_stripes");
    family.numSubStripes = body().required<unsigned int>("num_sub_stripes");
    family.overlap = body().required<double>("overlap");

    debug(__func__, "name=" + family.name);
    debug(__func__, "replication_level=" + to_string(family.replicationLevel));
    debug(__func__, "num_stripes=" + to_string(family.numStripes));
    debug(__func__, "num_sub_stripes=" + to_string(family.numSubStripes));
    debug(__func__, "overlap=" + to_string(family.overlap));

    if (0 == family.replicationLevel) {
        throw http::Error(__func__, "'replication_level' can't be equal to 0");
    }
    if (0 == family.numStripes) {
        throw http::Error(__func__, "'num_stripes' can't be equal to 0");
    }
    if (0 == family.numSubStripes) {
        throw http::Error(__func__, "'num_sub_stripes' can't be equal to 0");
    }
    if (family.overlap <= 0) {
        throw http::Error(__func__, "'overlap' can't be less or equal to 0");
    }

    json result;
    result["config"]["database_families"][family.name] =
            controller()->serviceProvider()->config()->addDatabaseFamily(family).toJson();
    return result;
}

json HttpConfigurationModule::_deleteDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const database = params().at("database");
    controller()->serviceProvider()->config()->deleteDatabase(database);
    return json::object();
}

json HttpConfigurationModule::_addDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = body().required<string>("database");
    string const familyName = body().required<string>("family");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "family=" + familyName);

    json result;
    result["config"]["databases"][databaseName] =
            controller()->serviceProvider()->config()->addDatabase(databaseName, familyName).toJson();
    return result;
}

json HttpConfigurationModule::_unpublishDatabase() {
    debug(__func__);
    auto const database = params().at("database");
    bool const publish = body().optional<int>("publish", 0) != 0;
    debug(__func__, "database=" + database);
    debug(__func__, "publish=" + bool2str(publish));

    // The publish parameter was introduced in API version 34.
    checkApiVersion(__func__, publish ? 34 : 12);

    if (!isAdmin()) {
        throw http::Error(__func__, "administrator's privileges are required to (un-)publish databases.");
    }
    auto const config = controller()->serviceProvider()->config();
    DatabaseInfo const databaseInfo =
            publish ? config->publishDatabase(database) : config->unPublishDatabase(database);
    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    bool const allWorkers = true;
    string const error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw http::Error(__func__, error);
    json result;
    result["config"]["databases"][database] = databaseInfo.toJson();
    return result;
}

json HttpConfigurationModule::_deleteTable() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const database = params().at("database");
    auto const table = params().at("table");
    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->deleteTable(database, table).toJson();
    return result;
}

json HttpConfigurationModule::_addTable() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    // FIXME: extend the service to accept all attributes of the table.
    // Create a method in the base class to extract standard attributes from
    // from the requests's body. Use this method here and in the method HttpIngestModule::_addTable

    TableInfo table;
    table.database = body().required<string>("database");
    table.name = body().required<string>("name");
    table.isPartitioned = body().required<int>("is_partitioned") != 0;

    debug(__func__, "database=" + table.database);
    debug(__func__, "table=" + table.name);
    debug(__func__, "is_partitioned=" + bool2str(table.isPartitioned));

    json result;
    result["config"]["databases"][table.database] =
            controller()->serviceProvider()->config()->addTable(table).toJson();
    return result;
}

}  // namespace lsst::qserv::replica
