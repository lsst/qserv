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
#include <limits>
#include <map>
#include <iomanip>
#include <set>
#include <stdexcept>
#include <sstream>

// Third party headers
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/intTypes.h"
#include "replica/AbortTransactionJob.h"
#include "replica/ChunkNumber.h"
#include "replica/ConfigurationTypes.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"
#include "replica/QservMgtServices.h"
#include "replica/QservStatusJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/SqlJob.h"
#include "replica/SqlRequest.h"


using namespace std;
using namespace std::placeholders;
namespace fs = boost::filesystem;
using json = nlohmann::json;
namespace qhttp = lsst::qserv::qhttp;
using namespace lsst::qserv::replica;

namespace {

string const taskName = "HTTP-PROCESSOR";

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

    /// parsed body of the request
    json objJson;

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
        req->content >> objJson;
        if (objJson.is_null() or objJson.is_object()) return;
        throw invalid_argument(
                "invalid format of the request body. A simple JSON object was expected");
    }

    template <typename T>
    T required(string const& name) const {
        if (objJson.find(name) != objJson.end()) return objJson[name];
        throw invalid_argument(
                "HttpRequestBody::" + string(__func__) + "<T> required parameter " + name +
                " is missing in the request body");
    }

    template <typename T>
    T optional(string const& name, T const& defaultValue) const {
        if (objJson.find(name) != objJson.end()) return objJson[name];
        return defaultValue;
    }
};

/**
 * @return the name of a worker which has the least number of replicas
 * among workers mentioned in the input collection of workrs.
 */
template<typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool   const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker: workers) {
        size_t const num =
            databaseServices->numWorkerReplicas(candidateWorker,
                                                noSpecificDatabase,
                                                allDatabases);
        if (num < numReplicas) {
            numReplicas = num;
            worker = candidateWorker;
        }
    }
    return worker;
}

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
    :   EventLogger(controller,
                    taskName),
        _onWorkerEvict(onWorkerEvict),
        _workerResponseTimeoutSec(workerResponseTimeoutSec),
        _healthMonitorTask(healthMonitorTask),
        _log(LOG_GET("lsst.qserv.replica.HttpProcessor")) {
}


HttpProcessor::~HttpProcessor() {
    logOnStopEvent();
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    logOnStartEvent();

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
        {"GET",    "/replication/v1/qserv/master/query/:id", bind(&HttpProcessor::_getQservUserQuery, self, _1, _2)},
        {"GET",    "/ingest/v1/trans", bind(&HttpProcessor::_getTransactions, self, _1, _2)},
        {"GET",    "/ingest/v1/trans/:id", bind(&HttpProcessor::_getTransaction, self, _1, _2)},
        {"POST",   "/ingest/v1/trans", bind(&HttpProcessor::_beginTransaction, self, _1, _2)},
        {"PUT",    "/ingest/v1/trans/:id", bind(&HttpProcessor::_endTransaction, self, _1, _2)},
        {"POST",   "/ingest/v1/database", bind(&HttpProcessor::_addDatabase, self, _1, _2)},
        {"PUT",    "/ingest/v1/database/:name", bind(&HttpProcessor::_publishDatabase, self, _1, _2)},
        {"POST",   "/ingest/v1/table", bind(&HttpProcessor::_addTable, self, _1, _2)},
        {"POST",   "/ingest/v1/chunk", bind(&HttpProcessor::_addChunk, self, _1, _2)},
        {"POST",   "/ingest/v1/chunk/empty", bind(&HttpProcessor::_buildEmptyChunksList, self, _1, _2)}
    });
    controller()->serviceProvider()->httpServer()->start();
}


string HttpProcessor::_context() const {
    return taskName + " ";
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

    if (not _replicationLevelReport.is_null()) {
        uint64_t lastReportAgeMs = PerformanceUtils::now() - _replicationLevelReportTimeMs;
        if (lastReportAgeMs < 240 * 1000) {
            _sendData(resp, _replicationLevelReport);
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

        json result;
        for (auto&& family: config->databaseFamilies()) {

            size_t const replicationLevel = config->databaseFamilyInfo(family).replicationLevel;
            result["families"][family]["level"] = replicationLevel;

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
                result["families"][family]["databases"][database] = databaseJson;
            }
        }

        // Update the cache
        _replicationLevelReport = result;
        _replicationLevelReportTimeMs = PerformanceUtils::now();

        _sendData(resp, _replicationLevelReport);

    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_listWorkerStatuses(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        HealthMonitorTask::WorkerResponseDelay delays =
            _healthMonitorTask->workerResponseDelay();

        json workersJson = json::array();
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
            workersJson.push_back(workerJson);
        }
        json result;
        result["workers"] = workersJson;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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

        json result;
        result["controllers"] = controllersJson;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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

        // General description of the Controller

        auto const dbSvc = controller()->serviceProvider()->databaseServices();
        auto const controllerInfo = dbSvc->controller(id);

        bool const isCurrent = controllerInfo.id == controller()->identity().id;

        json result;
        result["controller"] = controllerInfo.toJson(isCurrent);

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
        result["log"] = jsonLog;
        _sendData(resp, result);

    } catch (DatabaseServicesNotFound const& ex) {
        _sendError(resp, __func__, "no such controller found");
    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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

        auto const requests =
            controller()->serviceProvider()->databaseServices()->requests(
                jobId,
                fromTimeStamp,
                toTimeStamp,
                maxEntries);

        json requestsJson;
        for (auto&& info: requests) {
            requestsJson.push_back(info.toJson());
        }
        json result;
        result["requests"] = requestsJson;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getRequestInfo(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = req->params.at("id");

        json result;
        result["request"] = controller()->serviceProvider()->databaseServices()->request(id).toJson();

        _sendData(resp, result);

    } catch (DatabaseServicesNotFound const& ex) {
        _sendError(resp, __func__, "no such request found");
    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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


        auto const jobs =
            controller()->serviceProvider()->databaseServices()->jobs(
                controllerId,
                parentJobId,
                fromTimeStamp,
                toTimeStamp,
                maxEntries);

        json jobsJson;
        for (auto&& info: jobs) {
            jobsJson.push_back(info.toJson());
        }
        json result;
        result["jobs"] = jobsJson;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getJobInfo(qhttp::Request::Ptr const& req,
                                qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = req->params.at("id");

        json result;
        result["job"] = controller()->serviceProvider()->databaseServices()->job(id).toJson();

        _sendData(resp, result);

    } catch (DatabaseServicesNotFound const& ex) {
        _sendError(resp, __func__, "no such job found");
    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getConfig(qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config  = controller()->serviceProvider()->config();

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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

        json result;
        result["config"] = Configuration::toJson(config);
        _sendData(resp, result);

    } catch (boost::bad_lexical_cast const& ex) {
        _sendError(resp, __func__, "invalid value of a configuration parameter: " + string(ex.what()));
    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_updateWorkerConfig(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
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

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_deleteWorkerConfig(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const worker = req->params.at("name");

        config->deleteWorker(worker);
        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addWorkerConfig(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        
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

        config->addWorker(info);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_deleteFamilyConfig(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const family = req->params.at("name");

        config->deleteDatabaseFamily(family);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addFamilyConfig(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        
        DatabaseFamilyInfo info;
        info.name             = ::getRequiredQueryParamStr( req->query, "name");
        info.replicationLevel = ::getRequiredQueryParam(    req->query, "replication_level");
        info.numStripes       = ::getRequiredQueryParamUInt(req->query, "num_stripes");
        info.numSubStripes    = ::getRequiredQueryParamUInt(req->query, "num_sub_stripes");

        _debug(string(__func__) + " name="              +           info.name);
        _debug(string(__func__) + " replication_level=" + to_string(info.replicationLevel));
        _debug(string(__func__) + " num_stripes="       + to_string(info.numStripes));
        _debug(string(__func__) + " num_sub_stripes="   + to_string(info.numSubStripes));

        if (0 == info.replicationLevel) {
            _sendError(resp, __func__, "'replication_level' can't be equal to 0");
            return;
        }
        if (0 == info.numStripes) {
            _sendError(resp, __func__, "'num_stripes' can't be equal to 0");
            return;
        }
        if (0 == info.numSubStripes) {
            _sendError(resp, __func__, "'num_sub_stripes' can't be equal to 0");
            return;
        }
        config->addDatabaseFamily(info);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_deleteDatabaseConfig(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const database = req->params.at("name");

        config->deleteDatabase(database);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addDatabaseConfig(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        
        DatabaseInfo info;
        info.name   = ::getRequiredQueryParamStr(req->query, "name");
        info.family = ::getRequiredQueryParamStr(req->query, "family");

        _debug(string(__func__) + " name="   + info.name);
        _debug(string(__func__) + " family=" + info.family);

        config->addDatabase(info);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_deleteTableConfig(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const table = req->params.at("name");
        auto const database = ::getRequiredQueryParamStr(req->query, "database");

        config->deleteTable(database, table);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addTableConfig(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        
        auto const table         = ::getRequiredQueryParamStr( req->query, "name");
        auto const database      = ::getRequiredQueryParamStr( req->query, "database");
        auto const isPartitioned = ::getRequiredQueryParamBool(req->query, "is_partitioned");

        _debug(string(__func__) + " name="           + table);
        _debug(string(__func__) + " database="       + database);
        _debug(string(__func__) + " is_partitioned=" + to_string(isPartitioned ? 1 : 0));

        config->addTable(database, table, isPartitioned);

        json result;
        result["config"] = Configuration::toJson(config);

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_sqlQuery(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        // All parameters must be provided via the body of the request

        ::HttpRequestBody body(req);

        auto const worker   = body.required<string>("worker");
        auto const query    = body.required<string>("query");
        auto const user     = body.required<string>("user");
        auto const password = body.required<string>("password");
        auto const maxRows  = body.optional<uint64_t>("max_rows", 0);

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
        result["result_set"] = request->responseData().toJson();

        bool const success = request->extendedState() == Request::SUCCESS ? 1 : 0;
        _sendData(resp, result, success);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
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
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getQservManyUserQuery(qhttp::Request::Ptr const& req,
                                           qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        json result;
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getQservUserQuery(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const id = stoull(req->params.at("id"));

        _debug(string(__func__) + " id=" + to_string(id));

        json result;
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


json HttpProcessor::_getQueries(json& workerInfo) const {

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
        "qservMeta"
    );
    auto const conn = database::mysql::Connection::open(connectionParams);

    // Extract descriptions of those queries from qservMeta

    json result;

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
    return result;
}


void HttpProcessor::_getTransactions(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);


    try {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const database = ::getQueryParamStr(req->query, "database");

        _debug(string(__func__) + " database=" + database);

        json result;
        result["transactions"] = json::array();
        for (auto&& t: databaseServices->transactions(database)) {
            result["transactions"].push_back(t.toJson());
        }    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_getTransaction(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const id = stoul(req->params.at("id"));

        _debug(__func__, "id=" + to_string(id));

        json result;
        result["transaction"] = databaseServices->transaction(id).toJson();
    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_beginTransaction(qhttp::Request::Ptr const& req,
                                      qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    uint32_t id = 0;
    string database;

    auto const logBeginTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "BEGIN TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        ::HttpRequestBody body(req);

        auto const database = body.required<string>("database");

        _debug(__func__, "database=" + database);

        if (config->databaseInfo(database).isPublished) {
            _sendError(resp, __func__, "the database is already published");
            return;
        }
        auto const trans = databaseServices->beginTransaction(database);

        json result;
        result["transaction"] = trans.toJson();

        _sendData(resp, result);
        logBeginTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        auto const msg = "invalid parameters of the request, ex: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logBeginTransaction("FAILED", msg);
    } catch (exception const& ex) {
        auto const msg = "operation failed due to: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logBeginTransaction("FAILED", msg);
    }
}


void HttpProcessor::_endTransaction(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    uint32_t id = 0;
    string database;
    bool abort = false;

    auto const logEndTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "END TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        event.kvInfo.emplace_back("abort", abort ? "true" : "false");
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };
    try {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const config = controller()->serviceProvider()->config();

        id = stoul(req->params.at("id"));
        abort = ::getRequiredQueryParamBool(req->query, "abort");

        _debug(__func__, "id="    + to_string(id));
        _debug(__func__, "abort=" + to_string(abort ? 1 : 0));

        auto const trans = databaseServices->endTransaction(id, abort);
        auto const databaseInfo = config->databaseInfo(trans.database);
        database = trans.database;

        json result;
        result["transaction"] = trans.toJson();

        if (abort) {
            // Drop the transaction-specific MySQL partition from the relevant tables
            bool const allWorkers = true;
            auto const job = AbortTransactionJob::create(id, allWorkers, controller());
            logJobStartedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->start();
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->wait();
            result["data"] = job->getResultData().toJson();
        } else {
            // TODO: replicate MySQL partition associated with the transaction
            _error(__func__, "replication stage is not implemented");
        }

        _sendData(resp, result);
        logEndTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        auto const msg = "invalid parameters of the request, ex: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logEndTransaction("FAILED", msg);
    } catch (exception const& ex) {
        auto const msg = "operation failed due to: " + string(ex.what());
        _sendError(resp, __func__, msg);
        logEndTransaction("FAILED", msg);
    }
}


void HttpProcessor::_addDatabase(qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();

        ::HttpRequestBody body(req);

        DatabaseInfo databaseInfo;
        databaseInfo.name = body.required<string>("database");

        auto const numStripes    = body.required<unsigned int>("num_stripes");
        auto const numSubStripes = body.required<unsigned int>("num_sub_stripes");

        _debug(string(__func__) + " database="      + databaseInfo.name);
        _debug(string(__func__) + " numStripes="    + to_string(numStripes));
        _debug(string(__func__) + " numSubStripes=" + to_string(numSubStripes));
        
        // Find an appropriate database family for the database. If none
        // found then create a new one named after the database.

        string familyName;
        for (auto&& candidateFamilyName: config->databaseFamilies()) {
            auto const familyInfo = config->databaseFamilyInfo(candidateFamilyName);
            if ((familyInfo.numStripes == numStripes) and (familyInfo.numSubStripes == numSubStripes)) {
                familyName = candidateFamilyName;
            }
        }
        if (familyName.empty()) {

            // When creating the family use partitioning attributes as the name of the family
            // as shown below:
            //
            //   layout_<numStripes>_<numSubStripes>

            familyName = "layout_" + to_string(numStripes) + "_" + to_string(numSubStripes);
            DatabaseFamilyInfo familyInfo;
            familyInfo.name = familyName;
            familyInfo.replicationLevel = 1;
            familyInfo.numStripes = numStripes;
            familyInfo.numSubStripes = numSubStripes;
            config->addDatabaseFamily(familyInfo);
        }
        
        // Create the database at all QServ workers

        bool const allWorkers = true;
        auto const job = SqlCreateDbJob::create(
            databaseInfo.name,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlCreateDbJob::typeName(), job, familyName);
        job->wait();
        logJobFinishedEvent(SqlCreateDbJob::typeName(), job, familyName);

        string error;
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& resultSet = itr.second;
            bool const succeeded = resultData.workers.at(worker);
            if (not succeeded) {
                error += "database creation failed on worker: " + worker + ",  error: " +
                         resultSet.error + " ";
            }
        }
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Register the new database in the Configuration.
        // Note, this operation will fail if the database with the name
        // already exists. Also, the new database won't have any tables
        // until they will be added as a separate step.

        databaseInfo.family = familyName;
        databaseInfo.isPublished = false;

        databaseInfo = config->addDatabase(databaseInfo);

        json result;
        result["database"] = databaseInfo.toJson();
    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_publishDatabase(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();
        auto const database = ::getRequiredQueryParamStr(req->query, "database");

        _debug(string(__func__) + " database=" + database);

        if (config->databaseInfo(database).isPublished) {
            _sendError(resp, __func__, "the database is already published");
            return;
        }
           
        // TODO: (re-)build the secondary index. Should we rather do this as
        // in a separate REST call?

        // TODO: create database & tables entries in the Qserv master database

        // TODO: grant SELECT authorizations for the new database to Qserv
        // MySQL account(s) at all workers and the master(s)

        // TODO: register the database at CSS

        // TODO: enable this database in Qserv workers by adding an entry
        // to table 'qservw_worker.Dbs'

        // TODO: ask Replication workers to reload their Configurations so that
        // they recognized the new database as the published one. This step should
        // be probably done after publishing the database.
        //
        // NOTE: the rest should be taken care of by the Replication system.
        // This includes registering chunks in the persistent store of the Replication
        // system, synchronizing with Qserv workers, fixing, re-balancing,
        // replicating, etc.

        ControllerEvent event;
        event.status = "PUBLISH DATABASE";
        event.kvInfo.emplace_back("database", database);
        logEvent(event);

        json result;
        result["database"] = config->publishDatabase(database).toJson();
    
        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addTable(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const config = controller()->serviceProvider()->config();

        ::HttpRequestBody body(req);

        auto const database      = body.required<string>("database");
        auto const table         = body.required<string>("table");
        auto const isPartitioned = (bool)body.required<int>("is_partitioned");
        auto const schema        = body.required<json>("schema");
        auto const isDirector    = (bool)body.required<int>("is_director");
        auto const directorKey   = body.optional<string>("director_key", "");
        auto const chunkIdKey    = body.optional<string>("chunk_id_key", "");
        auto const subChunkIdKey = body.optional<string>("sub_chunk_id_key", "");

        _debug(string(__func__) + " database="      + database);
        _debug(string(__func__) + " table="         + table);
        _debug(string(__func__) + " isPartitioned=" + (isPartitioned ? "1" : "0"));
        _debug(string(__func__) + " schema="        + schema.dump());
        _debug(string(__func__) + " isDirector="    + (isDirector ? "1" : "0"));
        _debug(string(__func__) + " directorKey="   + directorKey);
        _debug(string(__func__) + " chunkIdKey="    + chunkIdKey);
        _debug(string(__func__) + " subChunkIdKey=" + subChunkIdKey);

        // Make sure the database is known and it's not PUBLISHED yet

        auto databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
           _sendError(resp, __func__, "the database is already published");
            return;
        }

        // Make sure the table doesn't exist in the Configuration

        for (auto&& existingTable: databaseInfo.tables()) {
            if (table == existingTable) {
                _sendError(resp, __func__, "table already exists");
                return;
            }
        }

        // Translate table schema

        if (schema.is_null()) {
            _sendError(resp, __func__, "table schema is empty");
            return;
        }
        if (not schema.is_array()) {
            _sendError(resp, __func__, "table schema is not defined as an array");
            return;
        }

        list<pair<string,string>> columns;

        // The name of a special column for the super-transaction-based ingest.
        // Always insert this column as the very first one into the schema.
        string const partitionByColumn = "qserv_trans_id";
        columns.emplace_front(partitionByColumn, "INT NOT NULL");

        for (auto&& coldef: schema) {
            if (not coldef.is_object()) {
                _sendError(resp, __func__,
                        "columns definitions in table schema are not JSON objects");
                return;
            }
            if (0 == coldef.count("name")) {
                _sendError(resp, __func__,
                        "column attribute 'name' is missing in table schema for "
                        "column number: " + to_string(columns.size() + 1));
                return;
            }
            string colName = coldef["name"];
            if (0 == coldef.count("type")) {
                _sendError(resp, __func__,
                        "column attribute 'type' is missing in table schema for "
                        "column number: " + to_string(columns.size() + 1));
                return;
            }
            string colType = coldef["type"];
            
            if (partitionByColumn == colName) {
                _sendError(resp, __func__,
                        "reserved column '" + partitionByColumn + "' is not allowed");
                return;
            }
            columns.emplace_back(colName, colType);
        }

        // TODO: if this is a partitioned table then add columns for
        //       chunk and sub-chunk numbers provided with the request.
        //       Check if these columns aren't present in the schema.
        //       Make sure they're provided for the partitioned table.

        // Create template tables on all workers. These tables will be used
        // to create chunk-specific tables before loading data.

        bool const allWorkers = true;
        string const engine = "MyISAM";

        auto const job = SqlCreateTableJob::create(
            database,
            table,
            engine,
            partitionByColumn,
            columns,
            allWorkers,
            controller()
        );
        logJobStartedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
        job->start();
        logJobFinishedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
        job->wait();

        string error;
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            auto&& resultSet = itr.second;

            bool const succeeded = resultData.workers.at(worker);
            if (not succeeded) {
                error += "table creation failed on worker: " + worker + ",  error: " +
                         resultSet.error + " ";
            }
        }
        if (not error.empty()) {
            _sendError(resp, __func__, error);
            return;
        }

        // Register table in the Configuration

        json result;
        result["database"] = config->addTable(
            database, table, isPartitioned, columns, isDirector,
            directorKey, chunkIdKey, subChunkIdKey
        ).toJson();

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__,  "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_addChunk(qhttp::Request::Ptr const& req,
                              qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {

        ::HttpRequestBody body(req);

        uint32_t const transactionId = body.required<uint32_t>("transaction_id");
        unsigned int const chunk = body.required<unsigned int>("chunk");

        _debug(string(__func__) + " transactionId=" + to_string(transactionId));
        _debug(string(__func__) + " chunk=" + to_string(chunk));

        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const config = controller()->serviceProvider()->config();

        auto const transactionInfo = databaseServices->transaction(transactionId);
        if (transactionInfo.state != "STARTED") {
            _sendError(resp, __func__, "this transaction is already over");
            return;
        }
        auto const databaseInfo = config->databaseInfo(transactionInfo.database);
        auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

        ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                                  databaseFamilyInfo.numSubStripes);
        if (not validator.valid(chunk)) {
            _sendError(resp, __func__, "this chunk number is not valid");
            return;
        }

        // This locks prevents other invocations of the method from making different
        // decisions on a chunk placement.
        util::Lock lock(_ingestManagementMtx, "HttpProcessor::" + string(__func__));

        // Decide on a worker where the chunk is best to be located.
        // If the chunk is already there then use it. Otherwise register an empty chunk
        // at some least loaded worker.
        //
        // ATTENTION: the current implementation of the algorithm assumes that
        // newly ingested chunks won't have replicas. This will change later
        // when the Replication system will be enhanced to allow creating replicas
        // of chunks within UNPUBLISHED databases.
        
        string worker;

        vector<ReplicaInfo> replicas;
        databaseServices->findReplicas(replicas, chunk, transactionInfo.database);
        if (replicas.size() > 1) {
            _sendError(resp, __func__, "this chunk has too many replicas");
            return;
        }
        if (replicas.size() == 1) {
            worker = replicas[0].worker();
        } else {

            // Search chunk in all databases of the same family to see
            // which workers may have replicas of the same chunk.
            // The idea here is to ensure the 'chunk colocation' requirements
            // is met, so that no unnecessary replica migration will be needed
            // when the database will be being published.
 
            bool const allDatabases = true;

            set<string> candidateWorkers;
            for (auto&& database: config->databases(databaseInfo.family, allDatabases)) {
                vector<ReplicaInfo> replicas;
                databaseServices->findReplicas(replicas, chunk, database);
                for (auto&& replica: replicas) {
                    candidateWorkers.insert(replica.worker());
                }
            }
            if (not candidateWorkers.empty()) {

                // Among those workers which have been found to have replicas with
                // the same chunk pick the one which has the least number of replicas
                // (of any chunks in any databases). The goal here is to ensure all
                // workers are equally loaded with data.
                //
                // NOTE: a decision of which worker is 'least loaded' is based
                // purely on the replica count, not on the amount of data residing
                // in the workers databases.

                worker = ::leastLoadedWorker(databaseServices, candidateWorkers);

            } else {

                // We got here because no database within the family has a chunk
                // with this number. Hence we need to pick some least loaded worker
                // among all known workers. 

                worker = ::leastLoadedWorker(databaseServices, config->workers());
            }

            // Register the new chunk
            //
            // TODO: Use status COMPLETE for now. Consider extending schema
            // of table 'replica' to store the status as well. This will allow
            // to differentiate between the 'INGEST_PRIMARY' and 'INGEST_SECONDARY' replicas,
            // which will be used for making the second replica of a chunk and selecting
            // the right version for further ingests.

            auto const verifyTime = PerformanceUtils::now();
            ReplicaInfo const newReplica(ReplicaInfo::Status::COMPLETE,
                                         worker,
                                         transactionInfo.database,
                                         chunk,
                                         verifyTime);
            databaseServices->saveReplicaInfo(newReplica);
        }
        
        // The sanity check, just to make sure we've found a worker
        if (worker.empty()) {
            _sendError(resp, __func__, "no suitable worker found");
            return;
        }
        ControllerEvent event;
        event.status = "ADD CHUNK";
        event.kvInfo.emplace_back("transaction", to_string(transactionInfo.id));
        event.kvInfo.emplace_back("database", transactionInfo.database);
        event.kvInfo.emplace_back("worker", worker);
        event.kvInfo.emplace_back("chunk", to_string(chunk));
        logEvent(event);

        // Pull connection parameters of the loader for the worker

        auto const workerInfo = config->workerInfo(worker);

        json result;
        result["location"]["worker"] = workerInfo.name;
        result["location"]["host"]   = workerInfo.loaderHost;
        result["location"]["port"]   = workerInfo.loaderPort;

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_buildEmptyChunksList(qhttp::Request::Ptr const& req,
                                          qhttp::Response::Ptr const& resp) {
    _debug(__func__);

    try {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const config = controller()->serviceProvider()->config();

        ::HttpRequestBody body(req);

        string const database = body.required<string>("database");
        bool const force = (bool)body.optional<int>("force", 0);

        _debug(string(__func__) + " database=" + database);
        _debug(string(__func__) + " force=" + string(force ? "1" : "0"));

        auto const databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
            throw invalid_argument("database is already published");
        }

        bool const enabledWorkersOnly = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database, enabledWorkersOnly);

        set<unsigned int> uniqueChunks;
        for (auto chunk: chunks) uniqueChunks.insert(chunk);

        auto const file = "empty_" + database + ".txt";
        auto const filePath = fs::path(config->controllerEmptyChunksDir()) / file;

        if (not force) {
            boost::system::error_code ec;
            fs::file_status const stat = fs::status(filePath, ec);
            if (stat.type() == fs::status_error) {
                throw runtime_error("failed to check the status of file: " + filePath.string());
            }
            if (fs::exists(stat)) {
                throw runtime_error("'force' is required to overwrite existing file: " + filePath.string());
            }
        }

        _debug(__func__, "creating/opening file: " + filePath.string());
        ofstream ofs(filePath.string());
        if (not ofs.good()) {
            throw runtime_error("failed to create/open file: " + filePath.string());
        }
        unsigned int const maxChunkAllowed = 1000000;
        for (unsigned int chunk = 0; chunk < maxChunkAllowed; ++chunk) {
            if (not uniqueChunks.count(chunk)) {
                ofs << chunk << "\n";
            }
        }
        ofs.flush();
        ofs.close();

        json result;
        result["file"] = file;
        result["num_chunks"] = chunks.size();

        _sendData(resp, result);

    } catch (invalid_argument const& ex) {
        _sendError(resp, __func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        _sendError(resp, __func__, "operation failed due to: " + string(ex.what()));
    }
}


void HttpProcessor::_sendError(qhttp::Response::Ptr const& resp,
                               string const& func,
                               string const& error) const {
    _error(func, error);

    json result;
    result["success"] = 0;
    result["error"] = error;

    resp->send(result.dump(), "application/json");
}


void HttpProcessor::_sendData(qhttp::Response::Ptr const& resp,
                              json& result,
                              bool success) {
    result["success"] = success ? 1 : 0;
    result["error"] = "";

    resp->send(result.dump(), "application/json");
}

}}} // namespace lsst::qserv::replica
