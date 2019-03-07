/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/HttpProcessor.h"

// System headers
#include <atomic>
#include <functional>
#include <map>
#include <iomanip>
#include <stdexcept>
#include <sstream>

// Third party headers
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "util/BlockPost.h"
#include "replica/ConfigurationTypes.h"
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"


using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;

namespace {

namespace qhttp = lsst::qserv::qhttp;
using namespace lsst::qserv::replica;

/**
 * Extract a value of an optional parameter of a query (string value)
 * 
 * @param req
 *   HTTP request object
 * 
 * @param name
 *   the name of a parameter to look for
 * 
 * @param defaultValue
 *   the default value to be returned if no parameter was found
 * 
 * @return
 *   the found value or the default value
 */
string getQueryParamStr(qhttp::Request::Ptr req,
                        string const& param,
                        string const& defaultValue) {
    auto&& itr = req->query.find(param);
    if (itr == req->query.end()) return defaultValue;
    return itr->second;
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 */
string getRequiredQueryParamStr(qhttp::Request::Ptr req,
                                string const& param) {
    auto const val = getQueryParamStr(req, param, string());
    if (val.empty()) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


/**
 * Extract a value of an optional parameter of a query (unsigned 16-bit)
 * 
 * @param req
 *   HTTP request object
 * 
 * @param name
 *   the name of a parameter to look for
 * 
 * @param defaultValue
 *   the default value to be returned if no parameter was found
 * 
 * @return
 *   the found value or the default value
 */
uint16_t getQueryParamUInt16(qhttp::Request::Ptr req,
                             string const& param,
                             uint16_t defaultValue) {
    auto&& itr = req->query.find(param);
    if (itr == req->query.end()) return defaultValue;
    unsigned long val = stoul(itr->second);
    if (val >= USHRT_MAX) {
        throw out_of_range(
           "HttpProcessor::" + string(__func__) + " invalid value of parameter: " + param);
    }
    return static_cast<uint16_t>(val);
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 */
uint16_t getRequiredQueryParamUInt16(qhttp::Request::Ptr req,
                                string const& param) {
    auto const val = getQueryParamUInt16(req, param, 0U);
    if (val == 0L) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


/**
 * Extract a value of an optional parameter of a query (unsigned 64-bit)
 * 
 * @param req
 *   HTTP request object
 * 
 * @param name
 *   the name of a parameter to look for
 * 
 * @param defaultValue
 *   the default value to be returned if no parameter was found
 * 
 * @return
 *   the found value or the default value
 */
uint64_t getQueryParam(qhttp::Request::Ptr req,
                       string const& param,
                       uint64_t defaultValue) {
    auto&& itr = req->query.find(param);
    if (itr == req->query.end()) return defaultValue;
    return stoull(itr->second);
}


/**
 * Extract a value of an optional parameter of a query (int)
 * 
 * @param req
 *   HTTP request object
 * 
 * @param name
 *   the name of a parameter to look for
 * 
 * @param defaultValue
 *   the default value to be returned if no parameter was found
 * 
 * @return
 *   the found value or the default value
 */
int getQueryParamInt(qhttp::Request::Ptr req,
                     string const& param,
                     int defaultValue) {
    auto&& itr = req->query.find(param);
    if (itr == req->query.end()) return defaultValue;
    return stoi(itr->second);
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 */
bool getRequiredQueryParamBool(qhttp::Request::Ptr req,
                               string const& param) {
    auto const val = getQueryParamInt(req, param, -1);
    if (val < 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val != 0;
}


/**
 * Extract a value of an optional parameter of a query (boolean value)
 * 
 * @param req
 *   HTTP request object
 * 
 * @param name
 *   the name of a parameter to look for
 * 
 * @param defaultValue
 *   the default value to be returned if no parameter was found
 * 
 * @return
 *   the found value or the default value
 */
bool getQueryParamBool(qhttp::Request::Ptr req,
                       string const& param,
                       bool defaultValue) {
    auto&& itr = req->query.find(param);
    if (itr == req->query.end()) return defaultValue;
    return not (itr->second.empty() or (itr->second == "0"));
}


/**
 * Inspect parameters of the request's query to see if the specified parameter
 * is one of those. And if so then extract its value, convert it into an appropriate
 * type and save in the Configuration.
 * 
 * @param struct_
 *   helper type specific to the Configuration parameter
 * 
 * @param req
 *   input request descriptor
 * 
 * @param config
 *   pointer to the Configuration service
 * 
 * @param logger
 *   the logger function to report  to the Configuration service
 *
 * @return
 *   'true' f the parameter was found and saved
 */
template<typename T>
bool saveConfigParameter(T& struct_,
                         qhttp::Request::Ptr const& req,
                         Configuration::Ptr const& config,
                         string const& context,
                         function<void(string const&)> const& logger) {
    auto const itr = req->query.find(struct_.key);
    if (itr != req->query.end()) {
        struct_.value = boost::lexical_cast<decltype(struct_.value)>(itr->second);
        struct_.save(config);
        logger(context + " updated Configuration parameter " + struct_.key + "=" + itr->second);
        return true;
    }
    return false;
}

}  // namespace


namespace lsst {
namespace qserv {
namespace replica {

HttpProcessor::Ptr HttpProcessor::create(
                        Controller::Ptr const& controller,
                        HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                        HealthMonitorTask::Ptr const& healthMonitorTask,
                        ReplicationTask::Ptr const& replicationTask,
                        DeleteWorkerTask::Ptr const& deleteWorkerTask) {

    auto ptr = Ptr(new HttpProcessor(
        controller,
        onWorkerEvict,
        healthMonitorTask,
        replicationTask,
        deleteWorkerTask
    ));
    ptr->_initialize();
    return ptr;
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                             HealthMonitorTask::Ptr const& healthMonitorTask,
                             ReplicationTask::Ptr const& replicationTask,
                             DeleteWorkerTask::Ptr const& deleteWorkerTask)
    :   _controller(controller),
        _onWorkerEvict(onWorkerEvict),
        _healthMonitorTask(healthMonitorTask),
        _replicationTask(replicationTask),
        _deleteWorkerTask(deleteWorkerTask),
        _log(LOG_GET("lsst.qserv.replica.HttpProcessor")) {
}


HttpProcessor::~HttpProcessor() {
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    auto self = shared_from_this();

    controller()->serviceProvider()->httpServer()->addHandlers({

        {"GET",    "/replication/v1/level",          bind(&HttpProcessor::_getReplicationLevel, self, _1, _2)},

        {"GET",    "/replication/v1/worker",         bind(&HttpProcessor::_listWorkerStatuses,  self, _1, _2)},
        {"GET",    "/replication/v1/worker/:name",   bind(&HttpProcessor::_getWorkerStatus,     self, _1, _2)},

        {"GET",    "/replication/v1/controller",     bind(&HttpProcessor::_listControllers,     self, _1, _2)},
        {"GET",    "/replication/v1/controller/:id", bind(&HttpProcessor::_getControllerInfo,   self, _1, _2)},

        {"GET",    "/replication/v1/request",        bind(&HttpProcessor::_listRequests,        self, _1, _2)},
        {"GET",    "/replication/v1/request/:id",    bind(&HttpProcessor::_getRequestInfo,      self, _1, _2)},

        {"GET",    "/replication/v1/job",            bind(&HttpProcessor::_listJobs,            self, _1, _2)},
        {"GET",    "/replication/v1/job/:id",        bind(&HttpProcessor::_getJobInfo,          self, _1, _2)},

        {"GET",    "/replication/v1/config",               bind(&HttpProcessor::_getConfig,           self, _1, _2)},
        {"PUT",    "/replication/v1/config/general",       bind(&HttpProcessor::_updateGeneralConfig, self, _1, _2)},
        {"PUT",    "/replication/v1/config/worker/:name",  bind(&HttpProcessor::_updateWorkerConfig,  self, _1, _2)},
        {"POST",   "/replication/v1/config/worker/:name",  bind(&HttpProcessor::_addWorkerConfig,     self, _1, _2)},
        {"DELETE", "/replication/v1/config/worker/:name",  bind(&HttpProcessor::_deleteWorkerConfig,  self, _1, _2)}
    });
    controller()->serviceProvider()->httpServer()->start();
}


string HttpProcessor::_context() const {
    return "HTTP-PROCESSOR ";
}


void HttpProcessor::_info(std::string const& msg) {
    LOGS(_log, LOG_LVL_INFO, _context() << msg);
}


void HttpProcessor::_debug(std::string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, _context() << msg);
}


void HttpProcessor::_error(std::string const& msg) {
    LOGS(_log, LOG_LVL_ERROR, _context() << msg);
}


void HttpProcessor::_getReplicationLevel(qhttp::Request::Ptr req,
                                         qhttp::Response::Ptr resp) {
    _debug(__func__);

    util::Lock lock(_replicationLevelMtx, "HttpProcessor::" + string(__func__));

    // Check if a cached report can be used
    //
    // TODO: add a cache control parameter to the class's constructor

    if (not _replicationLevelReport.empty()) {
        uint64_t lastReportAgeMs = PerformanceUtils::now() - _replicationLevelReportTimeMs;
        if (lastReportAgeMs < 240 * 1000) {
            resp->send(_replicationLevelReport, "application/json");
            return;
        }
    }

    // Otherwise, get the fresh snapshot of the replica distributions

    try {

        auto const config = controller()->serviceProvider()->config();

        HealthMonitorTask::WorkerResponseDelay const delays =
            _healthMonitorTask->workerResponseDelay();

        vector<string> disabledQservWorkers;
        vector<string> disabledReplicationWorkers;
        for (auto&& entry: delays) {
            auto&& worker =  entry.first;

            unsigned int const qservProbeDelaySec = entry.second.at("qserv");
            if (qservProbeDelaySec > 0) {
                disabledQservWorkers.push_back(worker);
            }
            unsigned int const replicationSystemProbeDelaySec = entry.second.at("replication");
            if (replicationSystemProbeDelaySec > 0) {
                disabledReplicationWorkers.push_back(worker);
            }
        }

        json resultJson;
        for (auto&& family: config->databaseFamilies()) {

            size_t const replicationLevel = config->databaseFamilyInfo(family).replicationLevel;
            resultJson["families"][family]["level"] = replicationLevel;

            for (auto&& database: config->databases(family)) {
                _debug(string(__func__) + "  database=" + database);

                // Get observed replication levels for workers which are on-line
                // as well as for the whole cluster (if there in-active workers).

                auto const onlineQservLevels =
                    controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                        database,
                        disabledQservWorkers);

                auto const allQservLevels = disabledQservWorkers.empty() ?
                    onlineQservLevels : 
                    controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                        database);

                auto const onLineReplicationSystemLevels =
                    controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                        database,
                        disabledReplicationWorkers);

                auto const allReplicationSystemLevels = disabledReplicationWorkers.empty() ?
                    onLineReplicationSystemLevels :
                    controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                        database);

                // Get the numbers of 'orphan' chunks in each context. These chunks (if any)
                // will be associated with the replication level 0. Also note, that these
                // chunks will be contributing into the total number of chunks when computing
                // the percentage of each replication level.

                size_t const numOrphanQservChunks = disabledQservWorkers.empty() ?
                    0 :
                    controller()->serviceProvider()->databaseServices()->numOrphanChunks(
                        database,
                        disabledQservWorkers);

                size_t const numOrphanReplicationSystemChunks = disabledReplicationWorkers.empty() ?
                    0 :
                    controller()->serviceProvider()->databaseServices()->numOrphanChunks(
                        database,
                        disabledReplicationWorkers);

                // The maximum level is needed to initialize result with zeros for
                // a contiguous range of levels [0,maxObservedLevel]. The non-empty
                // cells will be filled from the above captured reports.
                //
                // Also, while doing so compute the total number of chunks in each context.

                unsigned int maxObservedLevel = 0;

                size_t numOnlineQservChunks = numOrphanQservChunks;
                for (auto&& entry: onlineQservLevels) {
                    maxObservedLevel = max(maxObservedLevel, entry.first);
                    numOnlineQservChunks += entry.second;
                }

                size_t numAllQservChunks = 0;
                for (auto&& entry: allQservLevels) {
                    maxObservedLevel = max(maxObservedLevel, entry.first);
                    numAllQservChunks += entry.second;
                }

                size_t numOnlineReplicationSystemChunks = numOrphanReplicationSystemChunks;
                for (auto&& entry: onLineReplicationSystemLevels) {
                    maxObservedLevel = max(maxObservedLevel, entry.first);
                    numOnlineReplicationSystemChunks += entry.second;
                }

                size_t numAllReplicationSystemChunks = 0;
                for (auto&& entry: allReplicationSystemLevels) {
                    maxObservedLevel = max(maxObservedLevel, entry.first);
                    numAllReplicationSystemChunks += entry.second;
                }

                // Pre-initialize the database-specific result with zeroes for all
                // levels in the range of [0,maxObservedLevel]

                json databaseJson;

                for (int level = maxObservedLevel; level >= 0; --level) {
                    databaseJson["levels"][level]["qserv"      ]["online"]["num_chunks"] = 0;
                    databaseJson["levels"][level]["qserv"      ]["online"]["percent"   ] = 0.;
                    databaseJson["levels"][level]["qserv"      ]["all"   ]["num_chunks"] = 0;
                    databaseJson["levels"][level]["qserv"      ]["all"   ]["percent"   ] = 0.;
                    databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = 0;
                    databaseJson["levels"][level]["replication"]["online"]["percent"   ] = 0.;
                    databaseJson["levels"][level]["replication"]["all"   ]["num_chunks"] = 0;
                    databaseJson["levels"][level]["replication"]["all"   ]["percent"   ] = 0.;
                }

                // Fill-in non-blank areas

                for (auto&& entry: onlineQservLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks   = entry.second;
                    double const percent     = 100. * numChunks / numOnlineQservChunks;
                    databaseJson["levels"][level]["qserv"]["online"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["qserv"]["online"]["percent"   ] = percent;
                }
                for (auto&& entry: allQservLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks   = entry.second;
                    double const percent     = 100. * numChunks / numAllQservChunks;
                    databaseJson["levels"][level]["qserv"]["all"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["qserv"]["all"]["percent"   ] = percent;
                }
                for (auto&& entry: onLineReplicationSystemLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks   = entry.second;
                    double const percent     = 100. * numChunks / numOnlineReplicationSystemChunks;
                    databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["replication"]["online"]["percent"   ] = percent;
                }
                for (auto&& entry: allReplicationSystemLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks   = entry.second;
                    double const percent     = 100. * numChunks / numAllReplicationSystemChunks;
                    databaseJson["levels"][level]["replication"]["all"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["replication"]["all"]["percent"   ] = percent;
                }
                {
                    double const percent = 100. * numOrphanQservChunks / numAllQservChunks;
                    databaseJson["levels"][0]["qserv"]["online"]["num_chunks"] = numOrphanQservChunks;
                    databaseJson["levels"][0]["qserv"]["online"]["percent"   ] = percent;
                }
                {
                    double const percent     = 100. * numOrphanReplicationSystemChunks / numAllReplicationSystemChunks;
                    databaseJson["levels"][0]["replication"]["online"]["num_chunks"] = numOrphanReplicationSystemChunks;
                    databaseJson["levels"][0]["replication"]["online"]["percent"   ] = percent;
                }
                resultJson["families"][family]["databases"][database] = databaseJson;
            }
        }

        // Update the cache
        _replicationLevelReport = resultJson.dump();
        _replicationLevelReportTimeMs = PerformanceUtils::now();

        resp->send(_replicationLevelReport, "application/json");

    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_listWorkerStatuses(qhttp::Request::Ptr req,
                                        qhttp::Response::Ptr resp) {
    _debug(__func__);

    try {
        HealthMonitorTask::WorkerResponseDelay delays =
            _healthMonitorTask->workerResponseDelay();

        json resultJson = json::array();
        for (auto&& worker: controller()->serviceProvider()->config()->allWorkers()) {

            json workerJson;

            workerJson["worker"] = worker;

            WorkerInfo const info =
                controller()->serviceProvider()->config()->workerInfo(worker);

            uint64_t const numReplicas =
                controller()->serviceProvider()->databaseServices()->numWorkerReplicas(worker);

            workerJson["replication"]["num_replicas"] = numReplicas;
            workerJson["replication"]["isEnabled"]    = info.isEnabled  ? 1 : 0;
            workerJson["replication"]["isReadOnly"]   = info.isReadOnly ? 1 : 0;

            auto itr = delays.find(worker);
            if (delays.end() != itr) {
                workerJson["replication"]["probe_delay_s"] = itr->second["replication"];
                workerJson["qserv"      ]["probe_delay_s"] = itr->second["qserv"];
            } else {
                workerJson["replication"]["probe_delay_s"] = 0;
                workerJson["qserv"      ]["probe_delay_s"] = 0;
            }
            resultJson.push_back(workerJson);
        }
        resp->send(resultJson.dump(), "application/json");

    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getWorkerStatus(qhttp::Request::Ptr req,
                                     qhttp::Response::Ptr resp) {
    _debug(__func__);
    resp->sendStatus(404);
}


void HttpProcessor::_listControllers(qhttp::Request::Ptr req,
                                     qhttp::Response::Ptr resp) {
    _debug(__func__);
    
    try {

        // Extract parameters of the query

        uint64_t const fromTimeStamp = ::getQueryParam(req, "from",        0);
        uint64_t const toTimeStamp   = ::getQueryParam(req, "to",          std::numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam(req, "max_entries", 0);

        _debug(string(__func__) + " from="        + to_string(fromTimeStamp));
        _debug(string(__func__) + " to="          + to_string(toTimeStamp));
        _debug(string(__func__) + " max_entries=" + to_string(maxEntries));

        // Just descriptions of the Controllers. No persistent logs in this
        // report.

        json controllersJson;

        auto const controllers =
            controller()->serviceProvider()->databaseServices()->controllers(
                fromTimeStamp,
                toTimeStamp,
                maxEntries);

        for (auto&& info: controllers) {
            bool const isCurrent = info.id == controller()->identity().id;
            controllersJson.push_back(info.toJson(isCurrent));
        }
        json resultJson;
        resultJson["controllers"] = controllersJson;
        resp->send(resultJson.dump(), "application/json");

    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getControllerInfo(qhttp::Request::Ptr req,
                                       qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const id = req->params["id"];
    try {

        // Extract parameters of the query

        bool     const log           = ::getQueryParamBool(req, "log",            false);
        uint64_t const fromTimeStamp = ::getQueryParam(    req, "log_from",       0);
        uint64_t const toTimeStamp   = ::getQueryParam(    req, "log_to",         std::numeric_limits<uint64_t>::max());
        size_t   const maxEvents     = ::getQueryParam(    req, "log_max_events", 0);

        _debug(string(__func__) + " log="            +    string(log ? "true" : "false"));
        _debug(string(__func__) + " log_from="       + to_string(fromTimeStamp));
        _debug(string(__func__) + " log_to="         + to_string(toTimeStamp));
        _debug(string(__func__) + " log_max_events=" + to_string(maxEvents));

        json resultJson;

        // General description of the Controller

        auto const dbSvc = controller()->serviceProvider()->databaseServices();
        auto const controllerInfo = dbSvc->controller(id);

        bool const isCurrent = controllerInfo.id == controller()->identity().id;
        resultJson["controller"] = controllerInfo.toJson(isCurrent);

        // Pull the Controller log data if requested
        
        json jsonLog = json::array();
        if (log) {
            auto const events =
                dbSvc->readControllerEvents(
                    id,
                    fromTimeStamp,
                    toTimeStamp,
                    maxEvents);
            for (auto&& event: events) {
                jsonLog.push_back(event.toJson());
            }
        }
        resultJson["log"] = jsonLog;
        resp->send(resultJson.dump(), "application/json");

    } catch (DatabaseServicesNotFound const& ex) {
        _error(string(__func__) + " no Controller found for id: " + id);
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_listRequests(qhttp::Request::Ptr req,
                                  qhttp::Response::Ptr resp) {
    _debug(__func__);

    try {

        // Extract parameters of the query

        string   const jobId         = ::getQueryParamStr(req, "job_id",      "");
        uint64_t const fromTimeStamp = ::getQueryParam   (req, "from",        0);
        uint64_t const toTimeStamp   = ::getQueryParam   (req, "to",          std::numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam   (req, "max_entries", 0);

        _debug(string(__func__) + " job_id="      + jobId);
        _debug(string(__func__) + " from="        + to_string(fromTimeStamp));
        _debug(string(__func__) + " to="          + to_string(toTimeStamp));
        _debug(string(__func__) + " max_entries=" + to_string(maxEntries));

        // Pull descriptions of the Requests

        json requestsJson;

        auto const requests =
            controller()->serviceProvider()->databaseServices()->requests(
                jobId,
                fromTimeStamp,
                toTimeStamp,
                maxEntries);

        for (auto&& info: requests) {
            requestsJson.push_back(info.toJson());
        }
        json resultJson;
        resultJson["requests"] = requestsJson;
        resp->send(resultJson.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getRequestInfo(qhttp::Request::Ptr req,
                                    qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const id = req->params["id"];
    try {
        json requestJson;
        requestJson["request"] =
            controller()->serviceProvider()->databaseServices()->request(id).toJson();

        resp->send(requestJson.dump(), "application/json");

    } catch (DatabaseServicesNotFound const& ex) {
        _error(string(__func__) + " no Request found for id: " + id);
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_listJobs(qhttp::Request::Ptr req,
                              qhttp::Response::Ptr resp) {
    _debug(__func__);

    try {

        // Extract parameters of the query

        string   const controllerId  = ::getQueryParamStr(req, "controller_id", "");
        string   const parentJobId   = ::getQueryParamStr(req, "parent_job_id", "");
        uint64_t const fromTimeStamp = ::getQueryParam   (req, "from",          0);
        uint64_t const toTimeStamp   = ::getQueryParam   (req, "to",            std::numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam   (req, "max_entries",   0);

        _debug(string(__func__) + " controller_id=" + controllerId);
        _debug(string(__func__) + " parent_job_id=" + parentJobId);
        _debug(string(__func__) + " from="          + to_string(fromTimeStamp));
        _debug(string(__func__) + " to="            + to_string(toTimeStamp));
        _debug(string(__func__) + " max_entries="   + to_string(maxEntries));

        // Pull descriptions of the Jobs

        json jobsJson;

        auto const jobs =
            controller()->serviceProvider()->databaseServices()->jobs(
                controllerId,
                parentJobId,
                fromTimeStamp,
                toTimeStamp,
                maxEntries);

        for (auto&& info: jobs) {
            jobsJson.push_back(info.toJson());
        }
        json resultJson;
        resultJson["jobs"] = jobsJson;
        resp->send(resultJson.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getJobInfo(qhttp::Request::Ptr req,
                                qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const id = req->params["id"];
    try {
        json jobJson;
        jobJson["job"] =
            controller()->serviceProvider()->databaseServices()->job(id).toJson();

        resp->send(jobJson.dump(), "application/json");

    } catch (DatabaseServicesNotFound const& ex) {
        _error(string(__func__) + " no Request found for id: " + id);
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getConfig(qhttp::Request::Ptr req,
                               qhttp::Response::Ptr resp) {
    _debug(__func__);

    try {
        resp->send(_configToJson().dump(), "application/json");
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_updateGeneralConfig(qhttp::Request::Ptr req,
                                         qhttp::Response::Ptr resp) {
    _debug(__func__);

    try {

        ConfigurationGeneralParams general;

        auto const config = controller()->serviceProvider()->config();
        auto const logger = [this](string const& msg) { this->_debug(msg); };

        ::saveConfigParameter(general.requestBufferSizeBytes,      req, config, __func__, logger);
        ::saveConfigParameter(general.retryTimeoutSec,             req, config, __func__, logger);
        ::saveConfigParameter(general.controllerThreads,           req, config, __func__, logger);
        ::saveConfigParameter(general.controllerHttpPort,          req, config, __func__, logger);
        ::saveConfigParameter(general.controllerHttpThreads,       req, config, __func__, logger);
        ::saveConfigParameter(general.controllerRequestTimeoutSec, req, config, __func__, logger);
        ::saveConfigParameter(general.jobTimeoutSec,               req, config, __func__, logger);
        ::saveConfigParameter(general.jobHeartbeatTimeoutSec,      req, config, __func__, logger);
        ::saveConfigParameter(general.xrootdAutoNotify,            req, config, __func__, logger);
        ::saveConfigParameter(general.xrootdHost,                  req, config, __func__, logger);
        ::saveConfigParameter(general.xrootdPort,                  req, config, __func__, logger);
        ::saveConfigParameter(general.xrootdTimeoutSec,            req, config, __func__, logger);
        ::saveConfigParameter(general.databaseServicesPoolSize,    req, config, __func__, logger);
        ::saveConfigParameter(general.workerTechnology,            req, config, __func__, logger);
        ::saveConfigParameter(general.workerNumProcessingThreads,  req, config, __func__, logger);
        ::saveConfigParameter(general.fsNumProcessingThreads,      req, config, __func__, logger);
        ::saveConfigParameter(general.workerFsBufferSizeBytes,     req, config, __func__, logger);

        resp->send(_configToJson().dump(), "application/json");

    } catch (boost::bad_lexical_cast const& ex) {
        _error(string(__func__) + " invalid value of a configuration parameter: " + string(ex.what()));
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_updateWorkerConfig(qhttp::Request::Ptr req,
                                        qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const name = req->params["name"];
    try {
        int      const isEnabled  = ::getQueryParamInt   (req, "is_enabled",   -1);
        int      const isReadOnly = ::getQueryParamInt   (req, "is_read_only", -1);
        string   const svcHost    = ::getQueryParamStr   (req, "svc_host",     string());
        uint16_t const svcPort    = ::getQueryParamUInt16(req, "svc_port",     0U);
        string   const fsHost     = ::getQueryParamStr   (req, "fs_host",      string());
        uint16_t const fsPort     = ::getQueryParamUInt16(req, "fs_port",      0U);
        string   const dataDir    = ::getQueryParamStr   (req, "data_dir",     string());

        _debug(string(__func__) + " is_enabled="   + to_string(isEnabled));
        _debug(string(__func__) + " is_read_only=" + to_string(isReadOnly));
        _debug(string(__func__) + " svc_host="     + svcHost);
        _debug(string(__func__) + " svc_port="     + to_string(svcPort));
        _debug(string(__func__) + " fs_host="      + fsHost);
        _debug(string(__func__) + " fs_port="      + to_string(fsPort));
        _debug(string(__func__) + " data_dir="     + dataDir);

        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_addWorkerConfig(qhttp::Request::Ptr req,
                                     qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const name = req->params["name"];
    try {

        // All parameters must be provided
        
        bool     const isEnabled  = ::getRequiredQueryParamBool  (req, "is_enabled");
        bool     const isReadOnly = ::getRequiredQueryParamBool  (req, "is_read_only");
        string   const svcHost    = ::getRequiredQueryParamStr   (req, "svc_host");
        uint16_t const svcPort    = ::getRequiredQueryParamUInt16(req, "svc_port");
        string   const fsHost     = ::getRequiredQueryParamStr   (req, "fs_host");
        uint16_t const fsPort     = ::getRequiredQueryParamUInt16(req, "fs_port");
        string   const dataDir    = ::getRequiredQueryParamStr   (req, "data_dir");

        _debug(string(__func__) + " is_enabled="   + to_string(isEnabled  ? 1 : 0));
        _debug(string(__func__) + " is_read_only=" + to_string(isReadOnly ? 1 : 0));
        _debug(string(__func__) + " svc_host="     + svcHost);
        _debug(string(__func__) + " svc_port="     + to_string(svcPort));
        _debug(string(__func__) + " fs_host="      + fsHost);
        _debug(string(__func__) + " fs_port="      + to_string(fsPort));
        _debug(string(__func__) + " data_dir="     + dataDir);        

        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_deleteWorkerConfig(qhttp::Request::Ptr req,
                                        qhttp::Response::Ptr resp) {
    _debug(__func__);

    auto const name = req->params["name"];
    try {
        resp->send(_configToJson().dump(), "application/json");
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


json HttpProcessor::_configToJson() const {

    json configJson;

    auto const config = controller()->serviceProvider()->config();

    // General parameters

    ConfigurationGeneralParams general;
    configJson["general"] = general.toJson(config);

    // Workers

    json workersJson;
    for (auto&& worker: config->allWorkers()) {
        auto const wi = config->workerInfo(worker);
        workersJson.push_back(wi.toJson());
    }
    configJson["workers"] = workersJson;

    // Database families, databases, and tables

    json familiesJson;
    for (auto&& family: config->databaseFamilies()) {
        auto const fi = config->databaseFamilyInfo(family);
        json familyJson = fi.toJson();
        for (auto&& database: config->databases(family)) {
            auto const di = config->databaseInfo(database);
            familyJson["databases"].push_back(di.toJson());
        }
        familiesJson.push_back(familyJson);
    }
    configJson["families"] = familiesJson;

    json resultJson;
    resultJson["config"] = configJson;

    return resultJson;
}

}}} // namespace lsst::qserv::replica
