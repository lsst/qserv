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
#include "replica/HttpConfigurationModule.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/lexical_cast.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ConfigDatabase.h"
#include "replica/ConfigurationSchema.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

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
                                      string const& subModuleName, HttpAuthType const authType) {
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
    else if (subModuleName == "UNPUBLISH-DATABASE")
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
    auto const config = controller()->serviceProvider()->config();
    json result;
    result["config"] = config->toJson();
    result["config"]["meta"] = meta4general(config);
    return result;
}

json HttpConfigurationModule::_updateGeneral() {
    debug(__func__);
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

    auto const config = controller()->serviceProvider()->config();
    auto const worker = params().at("worker");

    // Update requested worker attribute changes into the latest transient state
    // of the worker. Then update worker's configuration in the database.
    WorkerInfo info = config->workerInfo(worker);

    // Get optional parameters of the query. Note the default values which
    // are expected to be replaced by actual values provided by a client in
    // parameters found in the query.
    auto const updateBool = [&](string const& func, string const& name, bool& out) {
        int const val = query().optionalInt(name);
        debug(func, name + "=" + to_string(val));
        WorkerInfo::update(val, out);
    };
    updateBool(__func__, "is-enabled", info.isEnabled);
    updateBool(__func__, "is-read-only", info.isReadOnly);

    json result;
    result["config"]["workers"][worker] = config->updateWorker(info).toJson();
    return result;
}

json HttpConfigurationModule::_deleteWorker() {
    debug(__func__);
    auto const worker = params().at("worker");
    controller()->serviceProvider()->config()->deleteWorker(worker);
    return json::object();
}

json HttpConfigurationModule::_addWorker() {
    debug(__func__);

    WorkerInfo info;
    info.name = body().required<string>("worker");
    info.isEnabled = body().required<int>("is-enabled") != 0;
    info.isReadOnly = body().required<int>("is-read-only") != 0;

    debug(__func__, "name=" + info.name);
    debug(__func__, "is-enabled=" + to_string(info.isEnabled ? 1 : 0));
    debug(__func__, "is-read-only=" + to_string(info.isReadOnly ? 1 : 0));

    json result;
    result["config"]["workers"][info.name] =
            controller()->serviceProvider()->config()->addWorker(info).toJson();
    return result;
}

json HttpConfigurationModule::_deleteFamily() {
    debug(__func__);
    auto const family = params().at("family");
    controller()->serviceProvider()->config()->deleteDatabaseFamily(family);
    return json::object();
}

json HttpConfigurationModule::_addFamily() {
    debug(__func__);

    DatabaseFamilyInfo info;
    info.name = body().required<string>("name");
    info.replicationLevel = body().required<unsigned int>("replication_level");
    info.numStripes = body().required<unsigned int>("num_stripes");
    info.numSubStripes = body().required<unsigned int>("num_sub_stripes");
    info.overlap = body().required<double>("overlap");

    debug(__func__, "name=" + info.name);
    debug(__func__, "replication_level=" + to_string(info.replicationLevel));
    debug(__func__, "num_stripes=" + to_string(info.numStripes));
    debug(__func__, "num_sub_stripes=" + to_string(info.numSubStripes));
    debug(__func__, "overlap=" + to_string(info.overlap));

    if (0 == info.replicationLevel) {
        throw HttpError(__func__, "'replication_level' can't be equal to 0");
    }
    if (0 == info.numStripes) {
        throw HttpError(__func__, "'num_stripes' can't be equal to 0");
    }
    if (0 == info.numSubStripes) {
        throw HttpError(__func__, "'num_sub_stripes' can't be equal to 0");
    }
    if (info.overlap <= 0) {
        throw HttpError(__func__, "'overlap' can't be less or equal to 0");
    }

    json result;
    result["config"]["database_families"][info.name] =
            controller()->serviceProvider()->config()->addDatabaseFamily(info).toJson();
    return result;
}

json HttpConfigurationModule::_deleteDatabase() {
    debug(__func__);
    auto const database = params().at("database");
    controller()->serviceProvider()->config()->deleteDatabase(database);
    return json::object();
}

json HttpConfigurationModule::_addDatabase() {
    debug(__func__);

    string const database = body().required<string>("database");
    string const family = body().required<string>("family");

    debug(__func__, "database=" + database);
    debug(__func__, "family=" + family);

    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->addDatabase(database, family).toJson();
    return result;
}

json HttpConfigurationModule::_unpublishDatabase() {
    debug(__func__);
    auto const database = params().at("database");
    debug(__func__, "database=" + database);
    if (!isAdmin()) {
        throw HttpError(__func__,
                        "administrator's privileges are required to un-publish databases.");
    }
    DatabaseInfo const databaseInfo = controller()->serviceProvider()->config()->unPublishDatabase(database);
    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    bool const allWorkers = true;
    string const error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);
    json result;
    result["config"]["databases"][database] = databaseInfo.toJson();
    return result;
}

json HttpConfigurationModule::_deleteTable() {
    debug(__func__);
    auto const database = params().at("database");
    auto const table = params().at("table");
    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->deleteTable(database, table).toJson();
    return result;
}

json HttpConfigurationModule::_addTable() {
    debug(__func__);

    auto const database = body().required<string>("database");
    auto const table = body().required<string>("name");
    auto const isPartitioned = body().required<int>("is_partitioned") != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "is_partitioned=" + string(isPartitioned ? "1" : "0"));

    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->addTable(database, table, isPartitioned).toJson();
    return result;
}

}  // namespace lsst::qserv::replica
