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
#include "replica/ConfigurationTypes.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * Inspect parameters of the request's query to see if the specified parameter
 * is one of those. And if so then extract its value, convert it into an appropriate
 * type and save in the Configuration.
 * 
 * @param param The general Configuration parameter.
 * @param query Input parameters of the HTTP request.
 * @param config A pointer to the Configuration service.
 * @param logger The logger function to report  to the Configuration service
 * @return 'true' if the parameter was found and saved
 */
template<typename T>
bool saveConfigParameter(T& param,
                         unordered_map<string,string> const& query,
                         Configuration::Ptr const& config,
                         function<void(string const&)> const& logger) {
    auto itr = query.find(param.key);
    if (itr != query.end()) {
        param.value = boost::lexical_cast<decltype(param.value)>(itr->second);
        param.save(*config);
        logger("updated " + param.key + "=" + itr->second);
        return true;
    }
    return false;
}
}

namespace lsst {
namespace qserv {
namespace replica {

void HttpConfigurationModule::process(Controller::Ptr const& controller,
                                      string const& taskName,
                                      HttpProcessorConfig const& processorConfig,
                                      qhttp::Request::Ptr const& req,
                                      qhttp::Response::Ptr const& resp,
                                      string const& subModuleName,
                                      HttpModule::AuthType const authType) {
    HttpConfigurationModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpConfigurationModule::HttpConfigurationModule(Controller::Ptr const& controller,
                                                 string const& taskName,
                                                 HttpProcessorConfig const& processorConfig,
                                                 qhttp::Request::Ptr const& req,
                                                 qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpConfigurationModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty()) return _get();
    else if (subModuleName == "UPDATE-GENERAL") return _updateGeneral();
    else if (subModuleName == "UPDATE-WORKER") return _updateWorker();
    else if (subModuleName == "DELETE-WORKER") return _deleteWorker();
    else if (subModuleName == "ADD-WORKER") return _addWorker();
    else if (subModuleName == "DELETE-DATABASE-FAMILY") return _deleteFamily();
    else if (subModuleName == "ADD-DATABASE-FAMILY") return _addFamily();
    else if (subModuleName == "DELETE-DATABASE") return _deleteDatabase();
    else if (subModuleName == "ADD-DATABASE") return _addDatabase();
    else if (subModuleName == "DELETE-TABLE") return _deleteTable();
    else if (subModuleName == "ADD-TABLE") return _addTable();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpConfigurationModule::_get() {
    debug(__func__);
    auto const config  = controller()->serviceProvider()->config();
    ConfigurationGeneralParams general;
    json result;
    result["config"] = config->toJson();
    result["config"]["general"] = general.toJson(*config);
    return result;
}


json HttpConfigurationModule::_updateGeneral() {
    debug(__func__);

    json result;
    try {
        ConfigurationGeneralParams general;

        auto   const config  = controller()->serviceProvider()->config();
        string const context = __func__;
        auto   const logger  = [this, context](string const& msg) {
            this->debug(context, msg);
        };
        ::saveConfigParameter(general.requestBufferSizeBytes, req()->query, config, logger);
        ::saveConfigParameter(general.retryTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.controllerThreads, req()->query, config, logger);
        ::saveConfigParameter(general.controllerRequestTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.jobTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.jobHeartbeatTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.controllerHttpPort, req()->query, config, logger);
        ::saveConfigParameter(general.controllerHttpThreads, req()->query, config, logger);
        ::saveConfigParameter(general.controllerEmptyChunksDir, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdAutoNotify, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdHost, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdPort, req()->query, config, logger);
        ::saveConfigParameter(general.xrootdTimeoutSec, req()->query, config, logger);
        ::saveConfigParameter(general.databaseServicesPoolSize, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabaseHost, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabasePort, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabaseUser, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabaseName, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabaseServicesPoolSize, req()->query, config, logger);
        ::saveConfigParameter(general.qservMasterDatabaseTmpDir, req()->query, config, logger);
        ::saveConfigParameter(general.workerTechnology, req()->query, config, logger);
        ::saveConfigParameter(general.workerNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.fsNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.workerFsBufferSizeBytes, req()->query, config, logger);
        ::saveConfigParameter(general.loaderNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.exporterNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.httpLoaderNumProcessingThreads, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultSvcPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultFsPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultDataDir, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultDbPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultDbUser, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultLoaderPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultLoaderTmpDir, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultExporterPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultExporterTmpDir, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultHttpLoaderPort, req()->query, config, logger);
        ::saveConfigParameter(general.workerDefaultHttpLoaderTmpDir, req()->query, config, logger);
        result["config"] = config->toJson();
        result["config"]["general"] = general.toJson(*config);
    } catch (boost::bad_lexical_cast const& ex) {
        throw HttpError(__func__, "invalid value of a configuration parameter: " + string(ex.what()));
    }
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
    auto const updateString = [&](string const& func, string const& name, string& out) {
        string const val = query().optionalString(name);
        debug(func, name + "=" + val);
        WorkerInfo::update(val, out);
    };
    auto const updatePort = [&](string const& func, string const& name, uint16_t& out) {
        uint16_t const val = query().optionalUInt16(name);
        debug(func, name + "=" + to_string(val));
        WorkerInfo::update(val, out);
    };

    updateBool(__func__, "is_enabled",   info.isEnabled);
    updateBool(__func__, "is_read_only", info.isReadOnly);

    updateString(__func__, "svc_host", info.svcHost);
    updatePort(  __func__, "svc_port", info.svcPort);

    updateString(__func__, "fs_host",  info.fsHost);
    updatePort(  __func__, "fs_port",  info.fsPort);
    updateString(__func__, "data_dir", info.dataDir);

    updateString(__func__, "db_host", info.dbHost);
    updatePort(  __func__, "db_port", info.dbPort);
    updateString(__func__, "db_user", info.dbUser);

    updateString(__func__, "loader_host",    info.loaderHost);
    updatePort(  __func__, "loader_port",    info.loaderPort);
    updateString(__func__, "loader_tmp_dir", info.loaderTmpDir);

    updateString(__func__, "exporter_host",    info.exporterHost);
    updatePort(  __func__, "exporter_port",    info.exporterPort);
    updateString(__func__, "exporter_tmp_dir", info.exporterTmpDir);

    updateString(__func__, "http_loader_host",    info.httpLoaderHost);
    updatePort(  __func__, "http_loader_port",    info.httpLoaderPort);
    updateString(__func__, "http_loader_tmp_dir", info.httpLoaderTmpDir);

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

    info.isEnabled  = body().required<int>("is_enabled")   != 0;
    info.isReadOnly = body().required<int>("is_read_only") != 0;

    info.svcHost = body().required<string>(  "svc_host");
    info.svcPort = body().optional<uint16_t>("svc_port", 0);

    info.fsHost  = body().required<string>(  "fs_host", string());
    info.fsPort  = body().optional<uint16_t>("fs_port", 0);
    info.dataDir = body().required<string>(  "data_dir", string());

    info.dbHost = body().required<string>(  "db_host", string());
    info.dbPort = body().optional<uint16_t>("db_port", 0);
    info.dbUser = body().required<string>(  "db_user", string());

    info.loaderHost   = body().required<string>(  "loader_host", string());
    info.loaderPort   = body().optional<uint16_t>("loader_port", 0);
    info.loaderTmpDir = body().required<string>(  "loader_tmp_dir", string());

    info.exporterHost   = body().required<string>(  "exporter_host", string());
    info.exporterPort   = body().optional<uint16_t>("exporter_port", 0);
    info.exporterTmpDir = body().required<string>(  "exporter_tmp_dir", string());

    info.httpLoaderHost   = body().required<string>(  "http_loader_host", string());
    info.httpLoaderPort   = body().optional<uint16_t>("http_loader_port", 0);
    info.httpLoaderTmpDir = body().required<string>(  "http_loader_tmp_dir", string());

    debug(__func__, "name=" + info.name);

    debug(__func__, "is_enabled="   + to_string(info.isEnabled  ? 1 : 0));
    debug(__func__, "is_read_only=" + to_string(info.isReadOnly ? 1 : 0));

    debug(__func__, "svc_host=" +           info.svcHost);
    debug(__func__, "svc_port=" + to_string(info.svcPort));

    debug(__func__, "fs_host="  +           info.fsHost);
    debug(__func__, "fs_port="  + to_string(info.fsPort));
    debug(__func__, "data_dir=" +           info.dataDir);        

    debug(__func__, "db_host=" +           info.dbHost);
    debug(__func__, "db_port=" + to_string(info.dbPort));
    debug(__func__, "db_user=" +           info.dbUser);        

    debug(__func__, "loader_host="    +           info.loaderHost);
    debug(__func__, "loader_port="    + to_string(info.loaderPort));
    debug(__func__, "loader_tmp_dir=" +           info.loaderTmpDir);        

    debug(__func__, "exporter_host="    +           info.exporterHost);
    debug(__func__, "exporter_port="    + to_string(info.exporterPort));
    debug(__func__, "exporter_tmp_dir=" +           info.exporterTmpDir);        

    debug(__func__, "http_loader_host="    +           info.httpLoaderHost);
    debug(__func__, "http_loader_port="    + to_string(info.httpLoaderPort));
    debug(__func__, "http_loader_tmp_dir=" +           info.httpLoaderTmpDir);        

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
    info.name             = body().required<string>("name");
    info.replicationLevel = body().required<unsigned int>("replication_level");
    info.numStripes       = body().required<unsigned int>("num_stripes");
    info.numSubStripes    = body().required<unsigned int>("num_sub_stripes");
    info.overlap          = body().required<double>("overlap");

    debug(__func__, "name="              +           info.name);
    debug(__func__, "replication_level=" + to_string(info.replicationLevel));
    debug(__func__, "num_stripes="       + to_string(info.numStripes));
    debug(__func__, "num_sub_stripes="   + to_string(info.numSubStripes));
    debug(__func__, "overlap="           + to_string(info.overlap));

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
    debug(__func__, "family="   + family);

    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->addDatabase(database, family).toJson();
    return result;
}


json HttpConfigurationModule::_deleteTable() {
    debug(__func__);
    auto const database = params().at("database");
    auto const table    = params().at("table");
    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->deleteTable(database, table).toJson();
    return result;
}


json HttpConfigurationModule::_addTable() {
    debug(__func__);

    auto const database      = body().required<string>("database");
    auto const table         = body().required<string>("name");
    auto const isPartitioned = body().required<int>("is_partitioned") != 0;

    debug(__func__, "database="       + database);
    debug(__func__, "table="          + table);
    debug(__func__, "is_partitioned=" + string(isPartitioned ? "1" : "0"));

    json result;
    result["config"]["databases"][database] =
            controller()->serviceProvider()->config()->addTable(database, table, isPartitioned).toJson();
    return result;
}

}}}  // namespace lsst::qserv::replica
