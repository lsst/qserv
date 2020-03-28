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
#include "replica/HttpRequestQuery.h"
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
 * @param struct_  helper type specific to the Configuration parameter
 * @param query    parameters of the HTTP request
 * @param config   pointer to the Configuration service
 * @param logger   the logger function to report  to the Configuration service
 * @return          'true' f the parameter was found and saved
 */
template<typename T>
bool saveConfigParameter(T& struct_,
                         unordered_map<string,string> const& query,
                         Configuration::Ptr const& config,
                         function<void(string const&)> const& logger) {
    auto const itr = query.find(struct_.key);
    if (itr != query.end()) {
        struct_.value = boost::lexical_cast<decltype(struct_.value)>(itr->second);
        struct_.save(config);
        logger("updated " + struct_.key + "=" + itr->second);
        return true;
    }
    return false;
}
}

namespace lsst {
namespace qserv {
namespace replica {

HttpConfigurationModule::Ptr HttpConfigurationModule::create(
                                Controller::Ptr const& controller,
                                string const& taskName,
                                HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpConfigurationModule(
        controller, taskName, processorConfig
    ));
}


HttpConfigurationModule::HttpConfigurationModule(Controller::Ptr const& controller,
                                                 string const& taskName,
                                                 HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpConfigurationModule::executeImpl(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp,
                                          string const& subModuleName) {

    if (subModuleName.empty()) {
        _get(req, resp);
    } else if (subModuleName == "UPDATE-GENERAL") {
        _updateGeneral(req, resp);
    } else if (subModuleName == "UPDATE-WORKER") {
        _updateWorker(req, resp);
    } else if (subModuleName == "DELETE-WORKER") {
        _deleteWorker(req, resp);
    } else if (subModuleName == "ADD-WORKER") {
        _addWorker(req, resp);
    } else if (subModuleName == "DELETE-DATABASE-FAMILY") {
        _deleteFamily(req, resp);
    } else if (subModuleName == "ADD-DATABASE-FAMILY") {
        _addFamily(req, resp);
    } else if (subModuleName == "DELETE-DATABASE") {
        _deleteDatabase(req, resp);
    } else if (subModuleName == "ADD-DATABASE") {
        _addDatabase(req, resp);
    } else if (subModuleName == "DELETE-TABLE") {
        _deleteTable(req, resp);
    } else if (subModuleName == "ADD-TABLE") {
        _addTable(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpConfigurationModule::_get(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp) {
    debug(__func__);
    json result;
    result["config"] = Configuration::toJson(controller()->serviceProvider()->config());
    sendData(resp, result);
}


void HttpConfigurationModule::_updateGeneral(qhttp::Request::Ptr const& req,
                                             qhttp::Response::Ptr const& resp) {
    debug(__func__);

    try {
        ConfigurationGeneralParams general;

        auto   const config  = controller()->serviceProvider()->config();
        string const context = __func__;
        auto   const logger  = [this, context](string const& msg) {
            this->debug(context, msg);
        };
        ::saveConfigParameter(general.requestBufferSizeBytes,      req->query, config, logger);
        ::saveConfigParameter(general.retryTimeoutSec,             req->query, config, logger);
        ::saveConfigParameter(general.controllerThreads,           req->query, config, logger);
        ::saveConfigParameter(general.controllerHttpPort,          req->query, config, logger);
        ::saveConfigParameter(general.controllerHttpThreads,       req->query, config, logger);
        ::saveConfigParameter(general.controllerRequestTimeoutSec, req->query, config, logger);
        ::saveConfigParameter(general.jobTimeoutSec,               req->query, config, logger);
        ::saveConfigParameter(general.jobHeartbeatTimeoutSec,      req->query, config, logger);
        ::saveConfigParameter(general.xrootdAutoNotify,            req->query, config, logger);
        ::saveConfigParameter(general.xrootdHost,                  req->query, config, logger);
        ::saveConfigParameter(general.xrootdPort,                  req->query, config, logger);
        ::saveConfigParameter(general.xrootdTimeoutSec,            req->query, config, logger);
        ::saveConfigParameter(general.databaseServicesPoolSize,    req->query, config, logger);
        ::saveConfigParameter(general.workerTechnology,            req->query, config, logger);
        ::saveConfigParameter(general.workerNumProcessingThreads,  req->query, config, logger);
        ::saveConfigParameter(general.fsNumProcessingThreads,      req->query, config, logger);
        ::saveConfigParameter(general.workerFsBufferSizeBytes,     req->query, config, logger);
        ::saveConfigParameter(general.loaderNumProcessingThreads,   req->query, config, logger);
        ::saveConfigParameter(general.exporterNumProcessingThreads, req->query, config, logger);

        json result;
        result["config"] = Configuration::toJson(config);
        sendData(resp, result);

    } catch (boost::bad_lexical_cast const& ex) {
        sendError(resp, __func__, "invalid value of a configuration parameter: " + string(ex.what()));
    }
}


void HttpConfigurationModule::_updateWorker(qhttp::Request::Ptr const& req,
                                            qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = req->params.at("name");

    // Get optional parameters of the query. Note the default values which
    // are expected to be replaced by actual values provided by a client in
    // parameters found in the query.

    HttpRequestQuery const query(req->query);
    string   const svcHost    = query.optionalString("svc_host");
    uint16_t const svcPort    = query.optionalUInt16("svc_port");
    string   const fsHost     = query.optionalString("fs_host");
    uint16_t const fsPort     = query.optionalUInt16("fs_port");
    string   const dataDir    = query.optionalString("data_dir");
    int      const isEnabled  = query.optionalInt(   "is_enabled");
    int      const isReadOnly = query.optionalInt(   "is_read_only");

    debug(__func__, "svc_host="     +           svcHost);
    debug(__func__, "svc_port="     + to_string(svcPort));
    debug(__func__, "fs_host="      +           fsHost);
    debug(__func__, "fs_port="      + to_string(fsPort));
    debug(__func__, "data_dir="     +           dataDir);
    debug(__func__, "is_enabled="   + to_string(isEnabled));
    debug(__func__, "is_read_only=" + to_string(isReadOnly));


    if (not  svcHost.empty()) config->setWorkerSvcHost(worker, svcHost);
    if (0 != svcPort)         config->setWorkerSvcPort(worker, svcPort);
    if (not  fsHost.empty())  config->setWorkerFsHost( worker, fsHost);
    if (0 != fsPort)          config->setWorkerFsPort( worker, fsPort);
    if (not  dataDir.empty()) config->setWorkerDataDir(worker, dataDir);

    if (isEnabled >= 0) {
        if (isEnabled != 0) config->disableWorker(worker, true);
        if (isEnabled == 0) config->disableWorker(worker, false);
    }
    if (isReadOnly >= 0) {
        if (isReadOnly != 0) config->setWorkerReadOnly(worker, true);
        if (isReadOnly == 0) config->setWorkerReadOnly(worker, false);
    }
    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_deleteWorker(qhttp::Request::Ptr const& req,
                                            qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const worker = req->params.at("name");

    config->deleteWorker(worker);
    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_addWorker(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    WorkerInfo info;
    HttpRequestQuery const query(req->query);
    info.name       = query.requiredString("name");
    info.svcHost    = query.requiredString("svc_host");
    info.svcPort    = query.requiredUInt16("svc_port");
    info.fsHost     = query.requiredString("fs_host");
    info.fsPort     = query.requiredUInt16("fs_port");
    info.dataDir    = query.requiredString("data_dir");
    info.isEnabled  = query.requiredBool(  "is_enabled");
    info.isReadOnly = query.requiredBool(  "is_read_only");

    debug(__func__, "name="         +           info.name);
    debug(__func__, "svc_host="     +           info.svcHost);
    debug(__func__, "svc_port="     + to_string(info.svcPort));
    debug(__func__, "fs_host="      +           info.fsHost);
    debug(__func__, "fs_port="      + to_string(info.fsPort));
    debug(__func__, "data_dir="     +           info.dataDir);        
    debug(__func__, "is_enabled="   + to_string(info.isEnabled  ? 1 : 0));
    debug(__func__, "is_read_only=" + to_string(info.isReadOnly ? 1 : 0));

    config->addWorker(info);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_deleteFamily(qhttp::Request::Ptr const& req,
                                            qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const family = req->params.at("name");

    config->deleteDatabaseFamily(family);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);

}


void HttpConfigurationModule::_addFamily(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestQuery const query(req->query);
    DatabaseFamilyInfo info;
    info.name             = query.requiredString("name");
    info.replicationLevel = query.requiredUInt64("replication_level");
    info.numStripes       = query.requiredUInt(  "num_stripes");
    info.numSubStripes    = query.requiredUInt(  "num_sub_stripes");
    info.overlap          = query.requiredDouble("overlap");

    debug(__func__, "name="              +           info.name);
    debug(__func__, "replication_level=" + to_string(info.replicationLevel));
    debug(__func__, "num_stripes="       + to_string(info.numStripes));
    debug(__func__, "num_sub_stripes="   + to_string(info.numSubStripes));
    debug(__func__, "overlap="           + to_string(info.overlap));

    if (0 == info.replicationLevel) {
        sendError(resp, __func__, "'replication_level' can't be equal to 0");
        return;
    }
    if (0 == info.numStripes) {
        sendError(resp, __func__, "'num_stripes' can't be equal to 0");
        return;
    }
    if (0 == info.numSubStripes) {
        sendError(resp, __func__, "'num_sub_stripes' can't be equal to 0");
        return;
    }
    if (info.overlap <= 0) {
        sendError(resp, __func__, "'overlap' can't be less or equal to 0");
        return;
    }
    config->addDatabaseFamily(info);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_deleteDatabase(qhttp::Request::Ptr const& req,
                                              qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const database = req->params.at("name");

    config->deleteDatabase(database);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_addDatabase(qhttp::Request::Ptr const& req,
                                           qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestQuery const query(req->query);
    DatabaseInfo info;
    info.name   = query.requiredString("name");
    info.family = query.requiredString("family");

    debug(__func__, "name="   + info.name);
    debug(__func__, "family=" + info.family);

    config->addDatabase(info);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_deleteTable(qhttp::Request::Ptr const& req,
                                           qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const table = req->params.at("name");

    HttpRequestQuery const query(req->query);
    auto const database = query.requiredString("database");

    config->deleteTable(database, table);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}


void HttpConfigurationModule::_addTable(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestQuery const query(req->query);
    auto const table         = query.requiredString("name");
    auto const database      = query.requiredString("database");
    auto const isPartitioned = query.requiredBool(  "is_partitioned");

    debug(__func__, "name="           + table);
    debug(__func__, "database="       + database);
    debug(__func__, "is_partitioned=" + to_string(isPartitioned ? 1 : 0));

    config->addTable(database, table, isPartitioned);

    json result;
    result["config"] = Configuration::toJson(config);

    sendData(resp, result);
}

}}}  // namespace lsst::qserv::replica
