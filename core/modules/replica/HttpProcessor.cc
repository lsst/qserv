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
#include "replica/HttpProcessor.h"

// System headers
#include <functional>
#include <iterator>
#include <map>
#include <iomanip>
#include <set>
#include <stdexcept>
#include <sstream>

// Third party headers
#include <boost/lexical_cast.hpp>

// Qserv headers
#include "global/constants.h"
#include "global/intTypes.h"
#include "replica/ConfigurationTypes.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"
#include "replica/QservMgtServices.h"
#include "replica/QservStatusJob.h"
#include "replica/SqlCreateDbRequest.h"
#include "replica/SqlCreateTableRequest.h"
#include "replica/SqlDeleteDbRequest.h"
#include "replica/SqlDeleteTablePartitionRequest.h"
#include "replica/SqlDeleteTableRequest.h"
#include "replica/SqlDisableDbRequest.h"
#include "replica/SqlEnableDbRequest.h"
#include "replica/SqlGrantAccessRequest.h"
#include "replica/SqlQueryRequest.h"
#include "replica/SqlRemoveTablePartitionsRequest.h"


using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;
namespace qhttp = lsst::qserv::qhttp;
using namespace lsst::qserv::replica;

namespace {

/**
 * Extract a value of an optional parameter of a query (string value)
 * 
 * @param query
 *   parameters of the HTTP request
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
string getQueryParamStr(unordered_map<string,string> const& query,
                        string const& param,
                        string const& defaultValue=string()) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
    return itr->second;
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 *
 * @see getQueryParamStr
 */
string getRequiredQueryParamStr(unordered_map<string,string> const& query,
                                string const& param) {
    auto const val = getQueryParamStr(query, param);
    if (val.empty()) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


/**
 * Extract a value of an optional parameter of a query (unsigned 16-bit)
 * 
 * @param query
 *   parameters of the HTTP request
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
uint16_t getQueryParamUInt16(unordered_map<string,string> const& query,
                             string const& param,
                             uint16_t defaultValue=0) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
    unsigned long val = stoul(itr->second);
    if (val >= numeric_limits<uint16_t>::max()) {
        throw out_of_range(
                "HttpProcessor::" + string(__func__) + " value of parameter: " + param +
                " exceeds allowed limit for type 'uint16_t'");
    }
    return static_cast<uint16_t>(val);
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 *
 * @see getQueryParamUInt16
 */
uint16_t getRequiredQueryParamUInt16(unordered_map<string,string> const& query,
                                     string const& param) {
    auto const val = getQueryParamUInt16(query, param, 0);
    if (val == 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


/**
 * Extract a value of an optional parameter of a query (unsigned 64-bit)
 * 
 * @param query
 *   parameters of the HTTP request
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
uint64_t getQueryParam(unordered_map<string,string> const& query,
                       string const& param,
                       uint64_t defaultValue=0) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
    return stoull(itr->second);
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 *
 * @see getQueryParam
 */
uint64_t getRequiredQueryParam(unordered_map<string,string> const& query,
                               string const& param) {
    auto const val = getQueryParam(query, param, 0);
    if (val == 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val;
}


/**
 * Extract a value of an optional parameter of a query (unsigned int)
 * 
 * @param query
 *   parameters of the HTTP request
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
unsigned int getRequiredQueryParamUInt(unordered_map<string,string> const& query,
                                       string const& param) {
    auto&& itr = query.find(param);
    if (itr == query.end()) {
        throw invalid_argument("mandatory parameter '" + param + "' is missing");
    }
    unsigned long val = stoul(itr->second);
    if (val > numeric_limits<unsigned int>::max()) {
        throw out_of_range(
                "HttpProcessor::" + string(__func__) + " value of parameter: " + param +
                " exceeds allowed limit for type 'unsigned int'");
    }
    return static_cast<unsigned int>(val);
}


/**
 * Extract a value of an optional parameter of a query (int)
 * 
 * @param query
 *   parameters of the HTTP request
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
int getQueryParamInt(unordered_map<string,string> const& query,
                     string const& param,
                     int defaultValue=-1) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
    return stoi(itr->second);
}

unsigned int getQueryParamUInt(unordered_map<string,string> const& query,
                               string const& param,
                               unsigned int defaultValue=0) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
    return stoul(itr->second);
}


/**
 * Safe version of the above defined method which requires that
 * the parameter was provided and it had a valid value.
 * 
 * @see getQueryParamInt
 */
bool getRequiredQueryParamBool(unordered_map<string,string> const& query,
                               string const& param) {
    auto const val = getQueryParamInt(query, param);
    if (val < 0) {
        throw invalid_argument(
                string(__func__) + " parameter '" + param + "' is missing or has an invalid value");
    }
    return val != 0;
}


/**
 * Extract a value of an optional parameter of a query (boolean value)
 * 
 * @param query
 *   parameters of the HTTP request
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
bool getQueryParamBool(unordered_map<string,string> const& query,
                       string const& param,
                       bool defaultValue=false) {
    auto&& itr = query.find(param);
    if (itr == query.end()) return defaultValue;
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
 * @param query
 *   parameters of the HTTP request
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


/**
 * Helper class HttpRequestBody parses a body of an HTTP request
 * which has the following header:
 * 
 *   Content-Type: application/json
 * 
 * Exceptions may be thrown by the constructor of the class if
 * the request has an unexpected content type, or if its payload
 * is not a proper JSON object.
 */
class HttpRequestBody {

public:

    /// Key-value pairs
    map<string,string> kv;

    HttpRequestBody() = delete;

    /**
     * The constructor will parse and evaluate a body of an HTTP request
     * and populate the 'kv' dictionary. Exceptions may be thrown in
     * the following scenarios:
     *
     * - the required HTTP header is not found in the request
     * - the body doesn't have a valid JSON string (unless the body is empty)
     * 
     * @param req
     *   the request to be parsed
     */
    explicit HttpRequestBody(qhttp::Request::Ptr const& req) {

        string const contentType = req->header["Content-Type"];
        string const requiredContentType = "application/json";
        if (contentType != requiredContentType) {
            throw invalid_argument(
                    "unsupported content type: '" + contentType + "' instead of: '" +
                    requiredContentType + "'");
        }
        json objJson;
        req->content >> objJson;
        if (objJson.is_null()) {
            ;   // empty body detected
        } else if (objJson.is_object()) {
            for (auto&& elem : objJson.items()) {
                kv[elem.key()] = elem.value();
            }
        } else {
            throw invalid_argument(
                    "invalid format of the request body. A simple JSON object with"
                    " <key>:<val> pairs of string data types for values"
                    " was expected");
        }
    }

    /// @return a value of a required parameter
    string required(string const& name) const {
        auto itr = kv.find(name);
        if (kv.end() == itr) {
            throw invalid_argument(
                    "required parameter " + name + " is missing in the request body");
        }
        return itr->second;
    }

    /// @return a value of an optional parameter if found. Otherwise return
    /// the default value.
    string optional(string const& name, string const& defaultValue) const {
        auto itr = kv.find(name);
        if (kv.end() == itr) return defaultValue;
        return itr->second;
    }
};

}  // namespace


namespace lsst {
namespace qserv {
namespace replica {

HttpProcessor::Ptr HttpProcessor::create(
                        Controller::Ptr const& controller,
                        HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                        unsigned int workerResponseTimeoutSec,
                        HealthMonitorTask::Ptr const& healthMonitorTask,
                        ReplicationTask::Ptr const& replicationTask,
                        DeleteWorkerTask::Ptr const& deleteWorkerTask) {

    auto ptr = Ptr(new HttpProcessor(
        controller,
        onWorkerEvict,
        workerResponseTimeoutSec,
        healthMonitorTask,
        replicationTask,
        deleteWorkerTask
    ));
    ptr->_initialize();
    return ptr;
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                             unsigned int workerResponseTimeoutSec,
                             HealthMonitorTask::Ptr const& healthMonitorTask,
                             ReplicationTask::Ptr const& replicationTask,
                             DeleteWorkerTask::Ptr const& deleteWorkerTask)
    :   _controller(controller),
        _onWorkerEvict(onWorkerEvict),
        _workerResponseTimeoutSec(workerResponseTimeoutSec),
        _healthMonitorTask(healthMonitorTask),
        _log(LOG_GET("lsst.qserv.replica.HttpProcessor")) {
}


HttpProcessor::~HttpProcessor() {
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    auto self = shared_from_this();

    controller()->serviceProvider()->httpServer()->addHandlers({

        {"GET",    "/replication/v1/level", bind(&HttpProcessor::_getReplicationLevel, self, _1, _2)},
        {"GET",    "/replication/v1/worker", bind(&HttpProcessor::_listWorkerStatuses, self, _1, _2)},
        {"GET",    "/replication/v1/worker/:name", bind(&HttpProcessor::_getWorkerStatus, self, _1, _2)},
        {"GET",    "/replication/v1/controller", bind(&HttpProcessor::_listControllers, self, _1, _2)},
        {"GET",    "/replication/v1/controller/:id", bind(&HttpProcessor::_getControllerInfo, self, _1, _2)},
        {"GET",    "/replication/v1/request", bind(&HttpProcessor::_listRequests, self, _1, _2)},
        {"GET",    "/replication/v1/request/:id", bind(&HttpProcessor::_getRequestInfo, self, _1, _2)},
        {"GET",    "/replication/v1/job", bind(&HttpProcessor::_listJobs, self, _1, _2)},
        {"GET",    "/replication/v1/job/:id", bind(&HttpProcessor::_getJobInfo, self, _1, _2)},
        {"GET",    "/replication/v1/config", bind(&HttpProcessor::_getConfig, self, _1, _2)},
        {"PUT",    "/replication/v1/config/general", bind(&HttpProcessor::_updateGeneralConfig, self, _1, _2)},
        {"PUT",    "/replication/v1/config/worker/:name", bind(&HttpProcessor::_updateWorkerConfig, self, _1, _2)},
        {"DELETE", "/replication/v1/config/worker/:name", bind(&HttpProcessor::_deleteWorkerConfig, self, _1, _2)},
        {"POST",   "/replication/v1/config/worker", bind(&HttpProcessor::_addWorkerConfig, self, _1, _2)},
        {"DELETE", "/replication/v1/config/family/:name", bind(&HttpProcessor::_deleteFamilyConfig,   self, _1, _2)},
        {"POST",   "/replication/v1/config/family", bind(&HttpProcessor::_addFamilyConfig, self, _1, _2)},
        {"DELETE", "/replication/v1/config/database/:name", bind(&HttpProcessor::_deleteDatabaseConfig, self, _1, _2)},
        {"POST",   "/replication/v1/config/database", bind(&HttpProcessor::_addDatabaseConfig, self, _1, _2)},
        {"DELETE", "/replication/v1/config/table/:name", bind(&HttpProcessor::_deleteTableConfig, self, _1, _2)},
        {"POST",   "/replication/v1/config/table", bind(&HttpProcessor::_addTableConfig, self, _1, _2)},
        {"POST",   "/replication/v1/sql/query", bind(&HttpProcessor::_sqlQuery, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/worker/status", bind(&HttpProcessor::_getQservManyWorkersStatus, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/worker/status/:name", bind(&HttpProcessor::_getQservWorkerStatus, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/master/query", bind(&HttpProcessor::_getQservManyUserQuery, self, _1, _2)},
        {"GET",    "/replication/v1/qserv/master/query/:id", bind(&HttpProcessor::_getQservUserQuery, self, _1, _2)}
    });
    controller()->serviceProvider()->httpServer()->start();
}


string HttpProcessor::_context() const {
    return "HTTP-PROCESSOR ";
}


void HttpProcessor::_info(string const& msg) const {
    LOGS(_log, LOG_LVL_INFO, _context() << msg);
}


void HttpProcessor::_debug(string const& msg) const {
    LOGS(_log, LOG_LVL_DEBUG, _context() << msg);
}


void HttpProcessor::_error(string const& msg) const {
    LOGS(_log, LOG_LVL_ERROR, _context() << msg);
}


void HttpProcessor::_getReplicationLevel(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp) {
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
                    size_t const numChunks = entry.second;
                    double const percent = numOnlineQservChunks == 0
                            ? 0. : 100. * numChunks / numOnlineQservChunks;
                    databaseJson["levels"][level]["qserv"]["online"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["qserv"]["online"]["percent"   ] = percent;
                }
                for (auto&& entry: allQservLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks = entry.second;
                    double const percent = numAllQservChunks == 0
                            ? 0. : 100. * numChunks / numAllQservChunks;
                    databaseJson["levels"][level]["qserv"]["all"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["qserv"]["all"]["percent"   ] = percent;
                }
                for (auto&& entry: onLineReplicationSystemLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks = entry.second;
                    double const percent = numOnlineReplicationSystemChunks == 0
                            ? 0. : 100. * numChunks / numOnlineReplicationSystemChunks;
                    databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["replication"]["online"]["percent"   ] = percent;
                }
                for (auto&& entry: allReplicationSystemLevels) {
                    unsigned int const level = entry.first;
                    size_t const numChunks = entry.second;
                    double const percent = numAllReplicationSystemChunks == 0
                            ? 0. : 100. * numChunks / numAllReplicationSystemChunks;
                    databaseJson["levels"][level]["replication"]["all"]["num_chunks"] = numChunks;
                    databaseJson["levels"][level]["replication"]["all"]["percent"   ] = percent;
                }
                {
                    double const percent = numAllQservChunks == 0
                            ? 0 : 100. * numOrphanQservChunks / numAllQservChunks;
                    databaseJson["levels"][0]["qserv"]["online"]["num_chunks"] = numOrphanQservChunks;
                    databaseJson["levels"][0]["qserv"]["online"]["percent"   ] = percent;
                }
                {
                    double const percent = numAllReplicationSystemChunks == 0
                            ? 0 : 100. * numOrphanReplicationSystemChunks / numAllReplicationSystemChunks;
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


void HttpProcessor::_listWorkerStatuses(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
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


void HttpProcessor::_getWorkerStatus(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);
    resp->sendStatus(404);
}


void HttpProcessor::_listControllers(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);
    
    try {

        // Extract optional parameters of the query

        uint64_t const fromTimeStamp = ::getQueryParam(req->query, "from");
        uint64_t const toTimeStamp   = ::getQueryParam(req->query, "to", numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam(req->query, "max_entries");

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


void HttpProcessor::_getControllerInfo(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = req->params.at("id");

        // Extract optional parameters of the query

        bool     const log           = ::getQueryParamBool(req->query, "log");
        uint64_t const fromTimeStamp = ::getQueryParam(    req->query, "log_from");
        uint64_t const toTimeStamp   = ::getQueryParam(    req->query, "log_to", numeric_limits<uint64_t>::max());
        size_t   const maxEvents     = ::getQueryParam(    req->query, "log_max_events");

        _debug(string(__func__) + " log="            +    string(log ? "1" : "0"));
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
        _error(string(__func__) + " no such controller found");
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_listRequests(qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        // Extract optional parameters of the query

        string   const jobId         = ::getQueryParamStr(req->query, "job_id");
        uint64_t const fromTimeStamp = ::getQueryParam   (req->query, "from");
        uint64_t const toTimeStamp   = ::getQueryParam   (req->query, "to", numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam   (req->query, "max_entries");

        _debug(string(__func__) + " job_id="      +           jobId);
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


void HttpProcessor::_getRequestInfo(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = req->params.at("id");

        json requestJson;
        requestJson["request"] =
            controller()->serviceProvider()->databaseServices()->request(id).toJson();

        resp->send(requestJson.dump(), "application/json");

    } catch (DatabaseServicesNotFound const& ex) {
        _error(string(__func__) + " no such request found");
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_listJobs(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        // Extract optional parameters of the query

        string   const controllerId  = ::getQueryParamStr(req->query, "controller_id");
        string   const parentJobId   = ::getQueryParamStr(req->query, "parent_job_id");
        uint64_t const fromTimeStamp = ::getQueryParam   (req->query, "from");
        uint64_t const toTimeStamp   = ::getQueryParam   (req->query, "to", numeric_limits<uint64_t>::max());
        size_t   const maxEntries    = ::getQueryParam   (req->query, "max_entries");

        _debug(string(__func__) + " controller_id=" +           controllerId);
        _debug(string(__func__) + " parent_job_id=" +           parentJobId);
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


void HttpProcessor::_getJobInfo(qhttp::Request::Ptr const& req,
                                qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = req->params.at("id");

        json jobJson;
        jobJson["job"] =
            controller()->serviceProvider()->databaseServices()->job(id).toJson();

        resp->send(jobJson.dump(), "application/json");

    } catch (DatabaseServicesNotFound const& ex) {
        _error(string(__func__) + " no such job found");
        resp->sendStatus(404);
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getConfig(qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        resp->send(_configToJson().dump(), "application/json");
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_updateGeneralConfig(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        ConfigurationGeneralParams general;

        auto   const config  = controller()->serviceProvider()->config();
        string const context = __func__;
        auto   const logger  = [this, context](string const& msg) {
            this->_debug(context + " " + msg);
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

        resp->send(_configToJson().dump(), "application/json");

    } catch (boost::bad_lexical_cast const& ex) {
        _error(string(__func__) + " invalid value of a configuration parameter: " + string(ex.what()));
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_updateWorkerConfig(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const worker = req->params.at("name");

        // Get optional parameters of the query. Note the default values which
        // are expected to be replaced by actual values provided by a client in
        // parameters found in the query.

        string   const svcHost    = ::getQueryParamStr   (req->query, "svc_host");
        uint16_t const svcPort    = ::getQueryParamUInt16(req->query, "svc_port");
        string   const fsHost     = ::getQueryParamStr   (req->query, "fs_host");
        uint16_t const fsPort     = ::getQueryParamUInt16(req->query, "fs_port");
        string   const dataDir    = ::getQueryParamStr   (req->query, "data_dir");
        int      const isEnabled  = ::getQueryParamInt   (req->query, "is_enabled");
        int      const isReadOnly = ::getQueryParamInt   (req->query, "is_read_only");

        _debug(string(__func__) + " svc_host="     +           svcHost);
        _debug(string(__func__) + " svc_port="     + to_string(svcPort));
        _debug(string(__func__) + " fs_host="      +           fsHost);
        _debug(string(__func__) + " fs_port="      + to_string(fsPort));
        _debug(string(__func__) + " data_dir="     +           dataDir);
        _debug(string(__func__) + " is_enabled="   + to_string(isEnabled));
        _debug(string(__func__) + " is_read_only=" + to_string(isReadOnly));

        auto const config = controller()->serviceProvider()->config();

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
        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_deleteWorkerConfig(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const worker = req->params.at("name");
        controller()->serviceProvider()->config()->deleteWorker(worker);
        resp->send(_configToJson().dump(), "application/json");
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " no such worker");
        resp->sendStatus(404);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_addWorkerConfig(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        // All parameters must be provided
        
        WorkerInfo info;
        info.name       = ::getRequiredQueryParamStr   (req->query, "name");
        info.svcHost    = ::getRequiredQueryParamStr   (req->query, "svc_host");
        info.svcPort    = ::getRequiredQueryParamUInt16(req->query, "svc_port");
        info.fsHost     = ::getRequiredQueryParamStr   (req->query, "fs_host");
        info.fsPort     = ::getRequiredQueryParamUInt16(req->query, "fs_port");
        info.dataDir    = ::getRequiredQueryParamStr   (req->query, "data_dir");
        info.isEnabled  = ::getRequiredQueryParamBool  (req->query, "is_enabled");
        info.isReadOnly = ::getRequiredQueryParamBool  (req->query, "is_read_only");

        _debug(string(__func__) + " name="         +           info.name);
        _debug(string(__func__) + " svc_host="     +           info.svcHost);
        _debug(string(__func__) + " svc_port="     + to_string(info.svcPort));
        _debug(string(__func__) + " fs_host="      +           info.fsHost);
        _debug(string(__func__) + " fs_port="      + to_string(info.fsPort));
        _debug(string(__func__) + " data_dir="     +           info.dataDir);        
        _debug(string(__func__) + " is_enabled="   + to_string(info.isEnabled  ? 1 : 0));
        _debug(string(__func__) + " is_read_only=" + to_string(info.isReadOnly ? 1 : 0));

        controller()->serviceProvider()->config()->addWorker(info);
        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_deleteFamilyConfig(qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const family = req->params.at("name");
        controller()->serviceProvider()->config()->deleteDatabaseFamily(family);
        resp->send(_configToJson().dump(), "application/json");
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_addFamilyConfig(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        // All parameters must be provided
        
        DatabaseFamilyInfo info;
        info.name             = ::getRequiredQueryParamStr( req->query, "name");
        info.replicationLevel = ::getRequiredQueryParam(    req->query, "replication_level");
        info.numStripes       = ::getRequiredQueryParamUInt(req->query, "num_stripes");
        info.numSubStripes    = ::getRequiredQueryParamUInt(req->query, "num_sub_stripes");

        _debug(string(__func__) + " name="              +           info.name);
        _debug(string(__func__) + " replication_level=" + to_string(info.replicationLevel));
        _debug(string(__func__) + " num_stripes="       + to_string(info.numStripes));
        _debug(string(__func__) + " num_sub_stripes="   + to_string(info.numSubStripes));

        if (0 == info.replicationLevel) throw invalid_argument("'replication_level' can't be equal to 0");
        if (0 == info.numStripes)       throw invalid_argument("'num_stripes' can't be equal to 0");
        if (0 == info.numSubStripes)    throw invalid_argument("'num_sub_stripes' can't be equal to 0");

        controller()->serviceProvider()->config()->addDatabaseFamily(info);
        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request, ex: " + ex.what());
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_deleteDatabaseConfig(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const database = req->params.at("name");
        controller()->serviceProvider()->config()->deleteDatabase(database);
        resp->send(_configToJson().dump(), "application/json");
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_addDatabaseConfig(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        // All parameters must be provided
        
        DatabaseInfo info;
        info.name   = ::getRequiredQueryParamStr(req->query, "name");
        info.family = ::getRequiredQueryParamStr(req->query, "family");

        _debug(string(__func__) + " name="   + info.name);
        _debug(string(__func__) + " family=" + info.family);

 
        controller()->serviceProvider()->config()->addDatabase(info);
        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request, ex: " + ex.what());
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_deleteTableConfig(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const table = req->params.at("name");
        auto const database = ::getRequiredQueryParamStr(req->query, "database");
        controller()->serviceProvider()->config()->deleteTable(database, table);
        resp->send(_configToJson().dump(), "application/json");
    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_addTableConfig(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        // All parameters must be provided
        
        auto const table         = ::getRequiredQueryParamStr( req->query, "name");
        auto const database      = ::getRequiredQueryParamStr( req->query, "database");
        auto const isPartitioned = ::getRequiredQueryParamBool(req->query, "is_partitioned");

        _debug(string(__func__) + " name="           + table);
        _debug(string(__func__) + " database="       + database);
        _debug(string(__func__) + " is_partitioned=" + to_string(isPartitioned ? 1 : 0));

        controller()->serviceProvider()->config()->addTable(database, table, isPartitioned);
        resp->send(_configToJson().dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request, ex: " + ex.what());
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_sqlQuery(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        // All parameters must be provided via the body of the request

        ::HttpRequestBody body(req);

        auto const worker   = body.required("worker");
        auto const query    = body.required("query");
        auto const user     = body.required("user");
        auto const password = body.required("password");
        auto const maxRows  = stoull(body.optional("max_rows", "0"));

        _debug(string(__func__) + " worker="   + worker);
        _debug(string(__func__) + " query="    + query);
        _debug(string(__func__) + " user="     + user);
        _debug(string(__func__) + " maxRows="  + to_string(maxRows));

        auto const request = controller()->sqlQuery(
            worker,
            query,
            user,
            password,
            maxRows
        );
        request->wait();

        json result;
        result["success"]    = request->extendedState() == Request::SUCCESS ? 1 : 0;
        result["result_set"] = request->responseData().toJson();

        resp->send(result.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request, ex: " + ex.what());
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}

void HttpProcessor::_getQservManyWorkersStatus(qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        unsigned int const timeoutSec =
            getQueryParamUInt(req->query, "timeout_sec", _workerResponseTimeoutSec);

        _debug(string(__func__) + " timeout_sec=" + to_string(timeoutSec));

        bool const allWorkers = true;
        auto const job = QservStatusJob::create(timeoutSec, allWorkers, controller());
        job->start();
        job->wait();

        json result;
        auto&& status = job->qservStatus();
        for (auto&& entry: status.workers) {
            auto&& worker = entry.first;
            bool success = entry.second;
            if (success) {
                auto info = status.info.at(worker);
                result["status"][worker]["success"] = 1;
                result["status"][worker]["info"] = info;
                result["status"][worker]["queries"] = _getQueries(info);
            } else {
                result["status"][worker]["success"] = 0;
            }        
        }
        resp->send(result.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getQservWorkerStatus(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        unsigned int const timeoutSec =
            getQueryParamUInt(req->query, "timeout_sec", _workerResponseTimeoutSec);

        auto const worker = req->params.at("name");

        _debug(string(__func__) + " timeout_sec=" + to_string(timeoutSec));
        _debug(string(__func__) + " worker=" + worker);

        string const noParentJobId;
        GetStatusQservMgtRequest::CallbackType const onFinish = nullptr;

        auto const request =
            controller()->serviceProvider()->qservMgtServices()->status(
                worker,
                noParentJobId,
                onFinish,
                timeoutSec);
        request->wait();

        json result;
        if (request->extendedState() == QservMgtRequest::ExtendedState::SUCCESS) {
            auto info = request->info();
            result["status"][worker]["success"] = 1;
            result["status"][worker]["info"] = info;
            result["status"][worker]["queries"] = _getQueries(info);
        } else {
            result["status"][worker]["success"] = 0;
        }        
        resp->send(result.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getQservManyUserQuery(qhttp::Request::Ptr const& req,
                                           qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        json result;
        result["status"]["success"] = 0;

        resp->send(result.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
        resp->sendStatus(500);
    }
}


void HttpProcessor::_getQservUserQuery(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        auto const id = stoull(req->params.at("id"));

        _debug(string(__func__) + " id="   + to_string(id));

        json result;
        result["status"]["success"] = 0;

        resp->send(result.dump(), "application/json");

    } catch (invalid_argument const& ex) {
        _error(string(__func__) + " invalid parameters of the request");
        resp->sendStatus(400);
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


json HttpProcessor::_getQueries(json& workerInfo) const {

    json result;
    try {

        // Find identifiers of all queries in the wait queues of all schedulers
        set<QueryId> qids;
        for (auto&& scheduler: workerInfo.at("processor").at("queries").at("blend_scheduler").at("schedulers")) {
            for (auto&& entry: scheduler.at("query_id_to_count")) {
                qids.insert(entry[0].get<QueryId>());
            }
        }

        // Connect to the database service of the Qserv Master
        auto const config = controller()->serviceProvider()->config();
        database::mysql::ConnectionParams const connectionParams(
            config->qservMasterDatabaseHost(),
            config->qservMasterDatabasePort(),
            config->qservMasterDatabaseUser(),
            config->qservMasterDatabasePassword(),
            lsst::qserv::SEC_INDEX_DB
        );
        auto const conn = database::mysql::Connection::open(connectionParams);
        
        // Extract descriptions of those queries from qservMeta
        if (not qids.empty()) {
            conn->execute(
                "SELECT * FROM " + conn->sqlId("QInfo") +
                "  WHERE "       + conn->sqlIn("queryId", qids)
            );
            if (conn->hasResult()) {

                database::mysql::Row row;
                while (conn->next(row)) {

                    QueryId queryId;
                    if (not row.get("queryId", queryId)) continue;

                    string query;
                    string status;
                    string submitted;
                    string completed;

                    row.get("query",     query);
                    row.get("status",    status);
                    row.get("submitted", submitted);
                    row.get("completed", completed);

                    string queryIdStr = to_string(queryId);
                    result[queryIdStr]["query"]     = query;
                    result[queryIdStr]["status"]    = status;
                    result[queryIdStr]["submitted"] = submitted;
                    result[queryIdStr]["completed"] = completed;
                }
            }
        }

    } catch (exception const& ex) {
        _error(string(__func__) + " operation failed due to: " + string(ex.what()));
    }
    return result;
}

}}} // namespace lsst::qserv::replica
