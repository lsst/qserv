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
#include "replica/HttpIngestModule.h"

// System headers
#include <limits>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "css/CssAccess.h"
#include "replica/AbortTransactionJob.h"
#include "replica/ChunkNumber.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/FindAllJob.h"
#include "replica/IndexJob.h"
#include "replica/HttpRequestBody.h"
#include "replica/HttpRequestQuery.h"
#include "replica/QservSyncJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceManagementJob.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlCreateDbJob.h"
#include "replica/SqlCreateTableJob.h"
#include "replica/SqlDeleteDbJob.h"
#include "replica/SqlGrantAccessJob.h"
#include "replica/SqlEnableDbJob.h"
#include "replica/SqlRemoveTablePartitionsJob.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace fs = boost::filesystem;

namespace {

/**
 * @return the name of a worker which has the least number of replicas
 * among workers mentioned in the input collection of workers.
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

/**
 * The optimized version of the previously defined function will
 * re-use and update the transient replica disposition cache when
 * selecting a candidate worker. Each worker's entry in the cache
 * will be populated from the database first time 
 *
 * @param worker2replicasCache  the cache of replica disposition for workers
 * @param databaseServices  the service used for pulling the initial replica
 *   disposition counters for workers.
 * @param workers  a collection of workers to be evaluated as candidates.
 * @return the name of a worker which has the least number of replicas
 */
template<typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(map<string,size_t>& worker2replicasCache,
                         DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool   const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker: workers) {
        if (worker2replicasCache.count(candidateWorker) == 0) {
            worker2replicasCache[candidateWorker] =
                    databaseServices->numWorkerReplicas(candidateWorker,
                                                        noSpecificDatabase,
                                                        allDatabases);
        } 
        size_t const num = worker2replicasCache[candidateWorker];
        if (num < numReplicas) {
            numReplicas = num;
            worker = candidateWorker;
        }
    }
    if (not worker.empty()) worker2replicasCache[worker]++;
    return worker;
}


/**
 * The optimized version of the previously defined function will
 * re-use and update the transient replica disposition cache when
 * selecting a candidate worker. Each worker's entry in the cache
 * will be populated from the database first time 
 *
 * @param worker2replicasCache  the cache of replica disposition for workers
 * @param databaseServices  the service used for pulling the initial replica
 *   disposition counters for workers.
 * @param workers  a collection of workers to be evaluated as candidates.
 * @return the name of a worker which has the least number of replicas,
 *   or the empty string in case if the empty collection of workers was
 *   passed into the function, or if the maximum allowed number of replicas
 *   has been exceeded in all workers. Note that the empty string value
 *   needs to be treated as an abnormal condition.
 */
template<typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(map<string,size_t>& worker2replicasCache,
                         DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker: workers) {
        auto itr = worker2replicasCache.find(candidateWorker);
        if (worker2replicasCache.end() == itr) {
            pair<decltype(itr),bool> itrSuccess = worker2replicasCache.insert({
                candidateWorker,
                databaseServices->numWorkerReplicas(
                    candidateWorker,
                    noSpecificDatabase,
                    allDatabases
                )
            });
            itr = itrSuccess.first;
        }
        if (itr->second < numReplicas) {
            numReplicas = itr->second;
            worker = itr->first;
        }
    }

    // Update the replica counter for the found worker, to ensure the new
    // replica will be taken into account by subsequent invocations of
    // the function.
    //
    // Note that a safeguard below addresses two potential problems:
    // 1) an empty collection of candidate workers passed into the function, or
    // 2) the database query reports the biggest number for the given number type.
    // In either case a result of the function needs to be evaluated by a caller
    // to see if the empty string returned.
    if (worker.empty() or (numReplicas == numeric_limits<size_t>::max())) {
        return string();
    }
    worker2replicasCache[worker]++;
    return worker;
}


string jobCompletionErrorIfAny(SqlJob::Ptr const& job,
                               string const& prefix) {
    string error;
    if (job->extendedState() != Job::ExtendedState::SUCCESS) {
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            for (auto&& result: itr.second) {
                if (result.hasErrors()) {
                    error += prefix + ", worker: " + worker + ",  error: " +
                             result.firstError() + " ";
                }
            }
        }
    }
    return error;
}

}

namespace lsst {
namespace qserv {
namespace replica {

string const HttpIngestModule::_partitionByColumn = "qserv_trans_id";
string const HttpIngestModule::_partitionByColumnType = "INT NOT NULL";


HttpIngestModule::Ptr HttpIngestModule::create(Controller::Ptr const& controller,
                                               string const& taskName,
                                               HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpIngestModule(
        controller, taskName, processorConfig));
}


HttpIngestModule::HttpIngestModule(Controller::Ptr const& controller,
                                   string const& taskName,
                                   HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpIngestModule::executeImpl(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp,
                                   string const& subModuleName) {

    if (subModuleName == "TRANSACTIONS") {
        _getTransactions(req, resp);
    } else if (subModuleName == "SELECT-TRANSACTION-BY-ID") {
        _getTransaction(req, resp);
    } else if (subModuleName == "BEGIN-TRANSACTION") {
        _beginTransaction(req, resp);
    } else if (subModuleName == "END-TRANSACTION") {
        _endTransaction(req, resp);
    } else if (subModuleName == "ADD-DATABASE") {
        _addDatabase(req, resp);
    } else if (subModuleName == "PUBLISH-DATABASE") {
         _publishDatabase(req, resp);
    } else if (subModuleName == "DELETE-DATABASE") {
         _deleteDatabase(req, resp);
    } else if (subModuleName == "ADD-TABLE") {
         _addTable(req, resp);
    } else if (subModuleName == "ADD-CHUNK") {
         _addChunk(req, resp);
    } else if (subModuleName == "ADD-CHUNK-LIST") {
         _addChunks(req, resp);
    } else if (subModuleName == "BUILD-CHUNK-LIST") {
         _buildEmptyChunksList(req, resp);
    } else if (subModuleName == "REGULAR") {
         _getRegular(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpIngestModule::_getTransactions(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    HttpRequestQuery const query(req->query);
    auto const database     = query.optionalString("database");
    auto const family       = query.optionalString("family");
    auto const allDatabases = query.optionalUInt64("all_databases", 0) != 0;
    auto const isPublished  = query.optionalUInt64("is_published",  0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "family=" + family);
    debug(__func__, "all_databases=" + string(allDatabases ? "1": "0"));
    debug(__func__, "is_published=" + string(isPublished ? "1": "0"));

    vector<string> databases;
    if (database.empty()) {
        databases = config->databases(family, allDatabases, isPublished);
    } else {
        databases.push_back(database);
    }

    json result;
    result["databases"] = json::object();
    for (auto&& database: databases) {

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database, allWorkers);

        result["databases"][database]["info"] = config->databaseInfo(database).toJson();
        result["databases"][database]["num_chunks"] = chunks.size();

        result["databases"][database]["transactions"] = json::array();
        for (auto&& transaction: databaseServices->transactions(database)) {
            result["databases"][database]["transactions"].push_back(transaction.toJson());
        }
    }
    sendData(resp, result);
}


void HttpIngestModule::_getTransaction(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const id = stoul(req->params.at("id"));

    debug(__func__, "id=" + to_string(id));

    auto const transaction = databaseServices->transaction(id);

    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

    json result;
    result["databases"][transaction.database]["info"] = config->databaseInfo(transaction.database).toJson();
    result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
    result["databases"][transaction.database]["num_chunks"] = chunks.size();

    sendData(resp, result);
}


void HttpIngestModule::_beginTransaction(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp) {
    debug(__func__);

    TransactionId id = 0;
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
    
    // Intercept standard exceptions just to report the failure, then
    // let a caller to do the rest (post error messages into the Logger,
    // return errors to clients).
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        HttpRequestBody body(req);
        auto const database = body.required<string>("database");

        debug(__func__, "database=" + database);

        auto const databaseInfo = config->databaseInfo(database);
        if (databaseInfo.isPublished) {
            sendError(resp, __func__, "the database is already published");
            return;
        }
        auto const transaction = databaseServices->beginTransaction(databaseInfo.name);

        _addPartitionToSecondaryIndex(databaseInfo, transaction.id);

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, databaseInfo.name, allWorkers);

        json result;
        result["databases"][transaction.database]["info"] = config->databaseInfo(databaseInfo.name).toJson();
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();

        sendData(resp, result);
        logBeginTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        logBeginTransaction("FAILED", "invalid parameters of the request, ex: " + string(ex.what()));
        throw;
    } catch (exception const& ex) {
        logBeginTransaction("FAILED", "operation failed due to: " + string(ex.what()));
        throw;
    }
}


void HttpIngestModule::_endTransaction(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    debug(__func__);

    TransactionId id = 0;
    string database;
    bool abort = false;
    bool buildSecondaryIndex = false;

    auto const logEndTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "END TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        event.kvInfo.emplace_back("abort", abort ? "true" : "false");
        event.kvInfo.emplace_back("build-secondary-index", buildSecondaryIndex ? "true" : "false");
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };

    // Intercept standard exceptions just to report the failure, then
    // let a caller to do the rest (post error messages into the Logger,
    // return errors to clients).
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        id = stoul(req->params.at("id"));

        HttpRequestQuery const query(req->query);
        abort               = query.requiredBool("abort");
        buildSecondaryIndex = query.optionalBool("build-secondary-index");

        debug(__func__, "id="    + to_string(id));
        debug(__func__, "abort=" + to_string(abort ? 1 : 0));
        debug(__func__, "build-secondary-index=" + to_string(abort ? 1 : 0));

        auto const transaction = databaseServices->endTransaction(id, abort);
        auto const databaseInfo = config->databaseInfo(transaction.database);
        database = transaction.database;

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

        json result;
        result["databases"][transaction.database]["info"] = config->databaseInfo(transaction.database).toJson();
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();
        result["secondary-index-build-success"] = 0;

        if (abort) {

            // Drop the transaction-specific MySQL partition from the relevant tables
            auto const job = AbortTransactionJob::create(transaction.id, allWorkers, controller());
            job->start();
            logJobStartedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->wait();
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            result["data"] = job->getResultData().toJson();

            _removePartitionFromSecondaryIndex(databaseInfo, transaction.id);

        } else {

            // Make the best attempt to build a layer at the "secondary index"
            // if requested.
            if (buildSecondaryIndex) {
                bool const hasTransactions = true;
                string const destinationPath = transaction.database + "__" + databaseInfo.directorTable;
                auto const job = IndexJob::create(
                    transaction.database,
                    hasTransactions,
                    transaction.id,
                    allWorkers,
                    IndexJob::TABLE,
                    destinationPath,
                    controller()
                );
                job->start();
                logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
                job->wait();
                logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);
                result["secondary-index-build-success"] = job->extendedState() == Job::SUCCESS ? 1 : 0;
            }

            // TODO: replicate MySQL partition associated with the transaction
            error(__func__, "replication stage is not implemented");
        }
        sendData(resp, result);
        logEndTransaction("SUCCESS");

    } catch (invalid_argument const& ex) {
        logEndTransaction("FAILED", "invalid parameters of the request, ex: " + string(ex.what()));
        throw;
    } catch (exception const& ex) {
        logEndTransaction("FAILED", "operation failed due to: " + string(ex.what()));
        throw;
    }
}


void HttpIngestModule::_addDatabase(qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestBody body(req);
    DatabaseInfo databaseInfo;
    databaseInfo.name = body.required<string>("database");

    auto const numStripes    = body.required<unsigned int>("num_stripes");
    auto const numSubStripes = body.required<unsigned int>("num_sub_stripes");
    auto const overlap       = body.required<double>("overlap");

    debug(__func__, "database="      + databaseInfo.name);
    debug(__func__, "numStripes="    + to_string(numStripes));
    debug(__func__, "numSubStripes=" + to_string(numSubStripes));
    debug(__func__, "overlap="       + to_string(overlap));

    if (overlap < 0) {
        sendError(resp, __func__, "overlap can't have a negative value");
        return;
    }

    // Find an appropriate database family for the database. If none
    // found then create a new one named after the database.

    string familyName;
    for (auto&& candidateFamilyName: config->databaseFamilies()) {
        auto const familyInfo = config->databaseFamilyInfo(candidateFamilyName);
        if ((familyInfo.numStripes == numStripes) and (familyInfo.numSubStripes == numSubStripes)
            and (abs(familyInfo.overlap - overlap) <= numeric_limits<double>::epsilon())) {
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
        familyInfo.overlap = overlap;
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

    string error = ::jobCompletionErrorIfAny(job, "database creation failed");
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    // Register the new database in the Configuration.
    // Note, this operation will fail if the database with the name
    // already exists. Also, the new database won't have any tables
    // until they will be added as a separate step.

    databaseInfo.family = familyName;
    databaseInfo.isPublished = false;

    databaseInfo = config->addDatabase(databaseInfo);

    // Tell workers to reload their configurations
    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    json result;
    result["database"] = databaseInfo.toJson();

    sendData(resp, result);
}


void HttpIngestModule::_publishDatabase(qhttp::Request::Ptr const& req,
                                        qhttp::Response::Ptr const& resp) {
    debug(__func__);

    bool const allWorkers = true;
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const database = req->params.at("name");

    HttpRequestQuery const query(req->query);
    bool const consolidateSecondayIndex = query.optionalBool("consolidate_secondary_index", false);

    debug(__func__, "database=" + database);
    debug(__func__, "consolidate_secondary_index=" + to_string(consolidateSecondayIndex ? 1 : 0));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        sendError(resp, __func__, "the database is already published");
        return;
    }

    // Scan super-transactions to make sure none is still open
    for (auto&& t: databaseServices->transactions(databaseInfo.name)) {
        if (t.state == TransactionInfo::STARTED) {
            sendError(resp, __func__, "database has uncommitted transactions");
            return;
        }
    }

    // ATTENTION: this operation may take a while if the table has
    // a large number of entries
    if (consolidateSecondayIndex) _consolidateSecondaryIndex(databaseInfo);

    if (not _grantDatabaseAccess(resp, databaseInfo, allWorkers)) return;
    if (not _enableDatabase(resp, databaseInfo, allWorkers)) return;
    if (not _removeMySQLPartitions(resp, databaseInfo, allWorkers)) return;

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    auto const error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    // Run the chunks scanner to ensure new chunks are registered in the persistent
    // store of the Replication system and synchronized with the Qserv workers.
    // The (fixing, re-balancing, replicating, etc.) will be taken care of by
    // the Replication system.
    if (not _qservSync(resp, databaseInfo, allWorkers)) return;

    // Finalize setting the database in Qserv master to make the new catalog
    // visible to Qserv users.
    _publishDatabaseInMaster(databaseInfo);

    ControllerEvent event;
    event.status = "PUBLISH DATABASE";
    event.kvInfo.emplace_back("database", database);
    logEvent(event);

    json result;
    result["database"] = config->publishDatabase(database).toJson();

    sendData(resp, result);
}


void HttpIngestModule::_deleteDatabase(qhttp::Request::Ptr const& req,
                                       qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    bool const allWorkers = true;
    auto const database = req->params.at("name");

    HttpRequestQuery const query(req->query);
    bool const deleteSecondaryIndex = query.optionalBool("delete_secondary_index", false);

    debug(__func__, "database=" + database);
    debug(__func__, "delete_secondary_index=" + to_string(deleteSecondaryIndex ? 1 : 0));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        sendError(resp, __func__, "unable to delete the database which is already published");
        return;
    }

    if (deleteSecondaryIndex) _deleteSecondaryIndex(databaseInfo);

    // Delete database entries at workers
    auto const job = SqlDeleteDbJob::create(databaseInfo.name, allWorkers, controller());
    job->start();
    logJobStartedEvent(SqlDeleteDbJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlDeleteDbJob::typeName(), job, databaseInfo.family);

    string error = ::jobCompletionErrorIfAny(job, "database deletion failed");
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    // Remove database entry from the Configuration. This will also eliminate all
    // dependent metadata, such as replicas info
    config->deleteDatabase(databaseInfo.name);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    json result;
    sendData(resp, result);
}


void HttpIngestModule::_addTable(qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    HttpRequestBody body(req);
    auto const database      = body.required<string>("database");
    auto const table         = body.required<string>("table");
    auto const isPartitioned = (bool)body.required<int>("is_partitioned");
    auto const schema        = body.required<json>("schema");
    auto const isDirector    = (bool)body.required<int>("is_director");
    auto const directorKey   = body.optional<string>("director_key", "");
    auto const chunkIdColName    = body.optional<string>("chunk_id_key", "");
    auto const subChunkIdColName = body.optional<string>("sub_chunk_id_key", "");
    auto const latitudeColName  = body.optional<string>("latitude_key",  "");
    auto const longitudeColName = body.optional<string>("longitude_key", "");

    debug(__func__, "database="      + database);
    debug(__func__, "table="         + table);
    debug(__func__, "isPartitioned=" + string(isPartitioned ? "1" : "0"));
    debug(__func__, "schema="        + schema.dump());
    debug(__func__, "isDirector="    + string(isDirector ? "1" : "0"));
    debug(__func__, "directorKey="   + directorKey);
    debug(__func__, "chunkIdColName="    + chunkIdColName);
    debug(__func__, "subChunkIdColName=" + subChunkIdColName);
    debug(__func__, "latitudeColName="  + latitudeColName);
    debug(__func__, "longitudeColName=" + longitudeColName);

    // Make sure the database is known and it's not PUBLISHED yet

    auto databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
       sendError(resp, __func__, "the database is already published");
        return;
    }

    // Make sure the table doesn't exist in the Configuration

    for (auto&& existingTable: databaseInfo.tables()) {
        if (table == existingTable) {
            sendError(resp, __func__, "table already exists");
            return;
        }
    }

    // Translate table schema

    if (schema.is_null()) {
        sendError(resp, __func__, "table schema is empty");
        return;
    }
    if (not schema.is_array()) {
        sendError(resp, __func__, "table schema is not defined as an array");
        return;
    }

    list<SqlColDef> columns;

    // The name of a special column for the super-transaction-based ingest.
    // Always insert this column as the very first one into the schema.
    columns.emplace_front(_partitionByColumn, _partitionByColumnType);

    for (auto&& coldef: schema) {
        if (not coldef.is_object()) {
            sendError(resp, __func__,
                    "columns definitions in table schema are not JSON objects");
            return;
        }
        if (0 == coldef.count("name")) {
            sendError(resp, __func__,
                    "column attribute 'name' is missing in table schema for "
                    "column number: " + to_string(columns.size() + 1));
            return;
        }
        string colName = coldef["name"];
        if (0 == coldef.count("type")) {
            sendError(resp, __func__,
                    "column attribute 'type' is missing in table schema for "
                    "column number: " + to_string(columns.size() + 1));
            return;
        }
        string colType = coldef["type"];

        if (_partitionByColumn == colName) {
            sendError(resp, __func__,
                    "reserved column '" + _partitionByColumn + "' is not allowed");
            return;
        }
        columns.emplace_back(colName, colType);
    }

    // Create template tables on all workers. These tables will be used
    // to create chunk-specific tables before loading data.

    bool const allWorkers = true;
    string const engine = "MyISAM";

    auto const job = SqlCreateTableJob::create(
        databaseInfo.name,
        table,
        engine,
        _partitionByColumn,
        columns,
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);

    string error = ::jobCompletionErrorIfAny(job, "table creation failed");
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }

    // Register table in the Configuration

    json result;
    result["database"] = config->addTable(
        databaseInfo.name, table, isPartitioned, columns, isDirector,
        directorKey, chunkIdColName, subChunkIdColName,
        latitudeColName, longitudeColName
    ).toJson();

    // Create the secondary index table using an updated version of
    // the database descriptor.

    if (isPartitioned and isDirector) _createSecondaryIndex(config->databaseInfo(databaseInfo.name));

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.

    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return;
    }
    sendData(resp, result);
}


void HttpIngestModule::_addChunk(qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp) {
    debug(__func__);

    HttpRequestBody body(req);
    TransactionId const transactionId = body.required<TransactionId>("transaction_id");
    unsigned int const chunk = body.required<unsigned int>("chunk");

    debug(__func__, "transactionId=" + to_string(transactionId));
    debug(__func__, "chunk=" + to_string(chunk));

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const transactionInfo = databaseServices->transaction(transactionId);
    if (transactionInfo.state != TransactionInfo::STARTED) {
        sendError(resp, __func__, "this transaction is already over");
        return;
    }
    auto const databaseInfo = config->databaseInfo(transactionInfo.database);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    if (not validator.valid(chunk)) {
        sendError(resp, __func__, "this chunk number is not valid");
        return;
    }

    // This locks prevents other invocations of the method from making different
    // decisions on a chunk placement.
    util::Lock lock(_ingestManagementMtx, "HttpIngestModule::" + string(__func__));

    // Decide on a worker where the chunk is best to be located.
    // If the chunk is already there then use it. Otherwise register an empty chunk
    // at some least loaded worker.
    //
    // ATTENTION: the current implementation of the algorithm assumes that
    // newly ingested chunks won't have replicas. This will change later
    // when the Replication system will be enhanced to allow creating replicas
    // of chunks within UNPUBLISHED databases.

    string worker;

    bool const enabledWorkersOnly = true;
    bool const includeFileInfo = false;
    vector<ReplicaInfo> replicas;
    databaseServices->findReplicas(replicas, chunk, transactionInfo.database,
                                   enabledWorkersOnly, includeFileInfo);
    if (replicas.size() > 1) {
        sendError(resp, __func__, "this chunk has too many replicas");
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
            databaseServices->findReplicas(replicas, chunk, database,
                                           enabledWorkersOnly, includeFileInfo);
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
        sendError(resp, __func__, "no suitable worker found");
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

    sendData(resp, result);
}


void HttpIngestModule::_addChunks(qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp) {
    debug(__func__);

    HttpRequestBody body(req);
    TransactionId const transactionId = body.required<TransactionId>("transaction_id");
    auto const chunks = body.requiredColl<unsigned int>("chunks");

    debug(__func__, "transactionId=" + to_string(transactionId));
    debug(__func__, "chunks.size()=" + chunks.size());

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const transactionInfo = databaseServices->transaction(transactionId);
    if (transactionInfo.state != TransactionInfo::STARTED) {
        sendError(resp, __func__, "this transaction is already over");
        return;
    }
    auto const databaseInfo = config->databaseInfo(transactionInfo.database);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Make sure chunk numbers are valid for the given
    // partitioning scheme.
    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    for (auto const chunk: chunks) {
        if (not validator.valid(chunk)) {
            sendError(resp, __func__, "chunk " + to_string(chunk) + " is not valid");
            return;
        }
    }

    // Locate replicas (if any) for all chunks. Then regroup them into
    // a dictionary in which all input chunk numbers are used as keys.
    bool const enabledWorkersOnly = true;
    bool const includeFileInfo = false;
    vector<ReplicaInfo> replicas;
    databaseServices->findReplicas(replicas, chunks, transactionInfo.database,
                                   enabledWorkersOnly, includeFileInfo);

    map<unsigned int, vector<ReplicaInfo>> chunk2replicas;
    for (auto&& replica: replicas) {
        auto const chunk = replica.chunk();
        chunk2replicas[chunk].push_back(move(replica));     // enforcing moving semantics in order
                                                            // to avoid memory bloating, etc.
    }

    // This locks prevents other invocations of the method from making different
    // decisions on chunk placements.
    util::Lock lock(_ingestManagementMtx, "HttpIngestModule::" + string(__func__));

    // (For each input chunk) decide on a worker where the chunk is best to be located.
    // If the chunk is already there then use it. Otherwise register an empty chunk
    // at some least loaded worker.
    //
    // ATTENTION: the current implementation of the algorithm assumes that
    // newly ingested chunks won't have replicas. This will change later
    // when the Replication system will be enhanced to allow creating replicas
    // of chunks within UNPUBLISHED databases.

    bool const allDatabases = true;
    auto const databases = config->databases(databaseInfo.family, allDatabases);

    map<string,size_t> worker2replicasCache;

    map<unsigned int, string> chunk2worker;
    for (auto const chunk: chunks) {

        vector<ReplicaInfo> const& replicas = chunk2replicas[chunk];

        if (replicas.size() > 1) {
            sendError(resp, __func__, "chunk " + to_string(chunk) + " has too many replicas");
            return;
        }
        if (replicas.size() == 1) {
            chunk2worker[chunk] = replicas[0].worker();
        } else {

            // Search chunk in all databases of the same family to see
            // which workers may have replicas of the same chunk.
            // The idea here is to ensure the 'chunk colocation' requirements
            // is met, so that no unnecessary replica migration will be needed
            // when the database will be being published.
            //
            // NOTE: the replica lookup operations have to be performed
            // at each iteration since previously registered replicas will
            // change replica disposition across workers. The replica lookup
            // algorithm uses and updates the transient replica disposition
            // cache to avoid making expensive queries against the persistent
            // store.

            set<string> candidateWorkers;
            for (auto&& database: databases) {
                vector<ReplicaInfo> replicas;
                databaseServices->findReplicas(replicas, chunk, database,
                                               enabledWorkersOnly, includeFileInfo);
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

                chunk2worker[chunk] = ::leastLoadedWorker(
                        worker2replicasCache, databaseServices, candidateWorkers);

            } else {

                // We got here because no database within the family has a chunk
                // with this number. Hence we need to pick some least loaded worker
                // among all known workers. 

                chunk2worker[chunk] = ::leastLoadedWorker(
                        worker2replicasCache, databaseServices, config->workers());
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
                                         chunk2worker[chunk],
                                         transactionInfo.database,
                                         chunk,
                                         verifyTime);
            databaseServices->saveReplicaInfo(newReplica);
        }

        // The sanity check, just to make sure we've found a worker
        if (chunk2worker[chunk].empty()) {
            sendError(resp, __func__, "no suitable worker found for chunk " + to_string(chunk));
            return;
        }
    }

    // Note, that the group operation for chunks will report the total
    // number of chunks allocated by the service rather than individual chunks.
    // This is done to avoid flooding the log with too many specific details on
    // the operation which (the details) could be found in the replica disposition
    // table.

    ControllerEvent event;
    event.status = "ADD CHUNKS";
    event.kvInfo.emplace_back("transaction", to_string(transactionInfo.id));
    event.kvInfo.emplace_back("database", transactionInfo.database);
    event.kvInfo.emplace_back("num_chunks", to_string(chunks.size()));
    logEvent(event);

    // Process the chunk-to-worker map into a result object to be
    // returned to a client.

    json result;
    result["location"] = json::array();

    for (auto const chunk: chunks) {

        // Pull connection parameters of the loader for the worker
        auto const workerInfo = config->workerInfo(chunk2worker[chunk]);

        json workerResult;
        workerResult["chunk"]  = chunk;
        workerResult["worker"] = workerInfo.name;
        workerResult["host"]   = workerInfo.loaderHost;
        workerResult["port"]   = workerInfo.loaderPort;

        result["location"].push_back(workerResult);
    }
    sendData(resp, result);
}


void HttpIngestModule::_buildEmptyChunksList(qhttp::Request::Ptr const& req,
                                             qhttp::Response::Ptr const& resp) {
    debug(__func__);

    HttpRequestBody body(req);
    string const database = body.required<string>("database");
    bool const force = (bool)body.optional<int>("force", 0);

    debug(__func__, "database=" + database);
    debug(__func__, "force=" + string(force ? "1" : "0"));

    auto const emptyListInfo = _buildEmptyChunksListImpl(database, force);

    json result;
    result["file"] = emptyListInfo.first;
    result["num_chunks"] = emptyListInfo.second;

    sendData(resp, result);
}


void HttpIngestModule::_getRegular(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const id = stoul(req->params.at("id"));
    debug(__func__, "id="    + to_string(id));

    auto const transaction = databaseServices->transaction(id);
    if (transaction.state != TransactionInfo::STARTED) {
        throw invalid_argument("transaction has already ended");
    }

    json resultLocations = json::array();
    for (auto&& worker: config->workers()) {
        auto&& workerInfo = config->workerInfo(worker);
        json resultLocation;
        resultLocation["worker"] = workerInfo.name;
        resultLocation["host"]   = workerInfo.loaderHost;
        resultLocation["port"]   = workerInfo.loaderPort;
        resultLocations.push_back(resultLocation);
    }

    json result;
    result["locations"] = resultLocations;
    sendData(resp, result);
}


bool HttpIngestModule::_grantDatabaseAccess(qhttp::Response::Ptr const& resp,
                                            DatabaseInfo const& databaseInfo,
                                            bool allWorkers) const {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlGrantAccessJob::create(
        databaseInfo.name,
        config->qservMasterDatabaseUser(),
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "grant access to a database failed");
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return false;
    }
    return true;
}


bool HttpIngestModule::_enableDatabase(qhttp::Response::Ptr const& resp,
                                       DatabaseInfo const& databaseInfo,
                                       bool allWorkers) const {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlEnableDbJob::create(
        databaseInfo.name,
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "enabling database failed");
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return false;
    }
    return true;
}


bool HttpIngestModule::_removeMySQLPartitions(qhttp::Response::Ptr const& resp,
                                              DatabaseInfo const& databaseInfo,
                                              bool allWorkers) const {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    // Ignore tables which may have already been processed at a previous attempt
    // of running this algorithm.
    bool const ignoreNonPartitioned = true;

    string error;
    for (auto const table: databaseInfo.tables()) {
        auto const job = SqlRemoveTablePartitionsJob::create(
            databaseInfo.name,
            table,
            allWorkers,
            ignoreNonPartitioned,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);

        error += ::jobCompletionErrorIfAny(
                        job,
                        "MySQL partitions removal failed for database: " +
                        databaseInfo.name + ", table: " + table);
    }
    if (not error.empty()) {
        sendError(resp, __func__, error);
        return false;
    }
    return true;
}


void HttpIngestModule::_publishDatabaseInMaster(DatabaseInfo const& databaseInfo) const {

    auto const config = controller()->serviceProvider()->config();
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Connect to the master database as user "root".
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    {
        database::mysql::ConnectionHandler const h(_qservMasterDbConnection());

        // SQL statements to be executed
        vector<string> statements;

        // Statements for creating the database & table entries

        statements.push_back(
            "CREATE DATABASE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name)
        );
        for (auto const& table: databaseInfo.tables()) {
            string sql = "CREATE TABLE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name) +
                    "." + h.conn->sqlId(table) + " (";
            bool first = true;
            for (auto const& coldef: databaseInfo.columns.at(table)) {
                if (first) {
                    first = false;
                } else {
                    sql += ",";
                }
                sql += h.conn->sqlId(coldef.name) + " " + coldef.type;
            }
            sql += ") ENGINE=InnoDB";
            statements.push_back(sql);
        }

        // Statements for granting SELECT authorizations for the new database
        // to the Qserv account.

        statements.push_back(
            "GRANT ALL ON " + h.conn->sqlId(databaseInfo.name) + ".* TO " +
            h.conn->sqlValue(config->qservMasterDatabaseUser()) + "@" +
            h.conn->sqlValue(config->qservMasterDatabaseHost()));

        h.conn->execute([&statements](decltype(h.conn) conn) {
            conn->begin();
            for (auto const& query: statements) {
                conn->execute(query);
            }
            conn->commit();
        });
    }

    // Register the database, tables and the partitioning scheme at CSS

    map<string, string> cssConfig;
    cssConfig["technology"] = "mysql";
    // FIXME: Address translation because CSS MySQL connector doesn't set the TCP protocol
    // option for 'localhost' and tries to connect via UNIX socket.
    cssConfig["hostname"] =
            config->qservMasterDatabaseHost() == "localhost" ?
                "127.0.0.1" :
                config->qservMasterDatabaseHost(),
    cssConfig["port"] = to_string(config->qservMasterDatabasePort());
    cssConfig["username"] = "root";
    cssConfig["password"] = Configuration::qservMasterDatabasePassword();
    cssConfig["database"] = "qservCssData";

    auto const cssAccess =
            css::CssAccess::createFromConfig(cssConfig, config->controllerEmptyChunksDir());
    if (not cssAccess->containsDb(databaseInfo.name)) {

        // First, try to find another database within the same family which
        // has already been published, and the one is found then use it
        // as a template when registering the database in CSS.
        //
        // Otherwise, create a new database using an extended CSS API which
        // will allocate a new partitioning identifier.

        bool const allDatabases = false;
        bool const isPublished = true;
        auto const databases = config->databases(databaseFamilyInfo.name, allDatabases, isPublished);
        if (not databases.empty()) {
            auto const templateDatabase = databases.front();
            cssAccess->createDbLike(databaseInfo.name, templateDatabase);
        } else {

            // This parameter is not used by the current implementation of the CSS API.
            // However, we should give it some meaningless value in case of the implementation
            // will change. (Hopefully) this would trigger an exception.
            int const unusedPartitioningId = -1;

            css::StripingParams const stripingParams(
                    databaseFamilyInfo.numStripes,
                    databaseFamilyInfo.numSubStripes,
                    unusedPartitioningId,
                    databaseFamilyInfo.overlap
            );
            string const storageClass = "L2";
            string const releaseStatus = "RELEASED";
            cssAccess->createDb(databaseInfo.name, stripingParams, storageClass, releaseStatus);
        }
    }

    // Register tables which still hasn't been registered in CSS
    
    for (auto const& table: databaseInfo.regularTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            // Neither of those groups of parameters apply to the regular tables,
            // so we're leaving them default constructed. 
            css::PartTableParams const partParams;
            css::ScanTableParams const scanParams;

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    for (auto const& table: databaseInfo.partitionedTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            bool const isPartitioned = true;
            bool const hasSubChunks = true;
            css::PartTableParams const partParams(
                databaseInfo.name,
                databaseInfo.directorTable,
                databaseInfo.directorTableKey,
                databaseInfo.latitudeColName.at(table),
                databaseInfo.longitudeColName.at(table),
                databaseFamilyInfo.overlap,     /* same as for other tables of the database family*/
                isPartitioned,
                hasSubChunks
            );

            bool const lockInMem = true;
            int const scanRating = 1;
            css::ScanTableParams const scanParams(lockInMem, scanRating);

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    
    bool const forceRebuild = true;
    _buildEmptyChunksListImpl(databaseInfo.name, forceRebuild);
}


pair<string,size_t> HttpIngestModule::_buildEmptyChunksListImpl(string const& database,
                                                                bool force) const {
    debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        throw invalid_argument("database is already published");
    }

    bool const enabledWorkersOnly = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database, enabledWorkersOnly);

    // Sanitize the collection of chunks to ensure it only has unique
    // chunk numbers.
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

    debug(__func__, "creating/opening file: " + filePath.string());
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
    
    return make_pair(file, uniqueChunks.size());
}


string HttpIngestModule::_reconfigureWorkers(DatabaseInfo const& databaseInfo,
                                             bool allWorkers,
                                             unsigned int workerResponseTimeoutSec) const {

    auto const job = ServiceReconfigJob::create(
        allWorkers,
        workerResponseTimeoutSec,
        controller()
    );
    job->start();
    logJobStartedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr: resultData.workers) {
        auto&& worker = itr.first;
        auto&& success = itr.second;
        if (not success) {
            error += "reconfiguration failed on worker: " + worker + " ";
        }
    }
    return error;
}


void HttpIngestModule::_createSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty() or databaseInfo.directorTableKey.empty() or
        databaseInfo.chunkIdColName.empty() or databaseInfo.subChunkIdColName.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }
    if (0 == databaseInfo.columns.count(databaseInfo.directorTable)) {
        throw logic_error(
                "no schema found for director table '" + databaseInfo.directorTable +
                "' of database '" + databaseInfo.name + "'");
    }

    // Find types of the secondary index table's columns

    string directorTableKeyType;
    string chunkIdColNameType;
    string subChunkIdColNameType;

    for (auto&& coldef: databaseInfo.columns.at(databaseInfo.directorTable)) {
        if      (coldef.name == databaseInfo.directorTableKey)  directorTableKeyType  = coldef.type;
        else if (coldef.name == databaseInfo.chunkIdColName)    chunkIdColNameType    = coldef.type;
        else if (coldef.name == databaseInfo.subChunkIdColName) subChunkIdColNameType = coldef.type;
    }
    if (directorTableKeyType.empty() or chunkIdColNameType.empty() or subChunkIdColNameType.empty()) {
        throw logic_error(
                "column definitions for the Object identifier or chunk/sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                databaseInfo.directorTable + "' of database '" + databaseInfo.name + "'");
    }
    
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(_qservMasterDbConnection());
    auto const escapedTableName = h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable);

    vector<string> queries;
    queries.push_back(
        "DROP TABLE IF EXISTS " + escapedTableName
    );
    queries.push_back(
        "CREATE TABLE IF NOT EXISTS " + escapedTableName +
        " (" + h.conn->sqlId(_partitionByColumn)            + " " + _partitionByColumnType + "," +
               h.conn->sqlId(databaseInfo.directorTableKey) + " " + directorTableKeyType   + "," +
               h.conn->sqlId(databaseInfo.chunkIdColName)       + " " + chunkIdColNameType         + "," +
               h.conn->sqlId(databaseInfo.subChunkIdColName)    + " " + subChunkIdColNameType      + ","
               " UNIQUE KEY (" + h.conn->sqlId(_partitionByColumn) + "," + h.conn->sqlId(databaseInfo.directorTableKey) + "),"
               " KEY (" + h.conn->sqlId(databaseInfo.directorTableKey) + ")"
        ") ENGINE=InnoDB PARTITION BY LIST (" + h.conn->sqlId(_partitionByColumn) +
        ") (PARTITION `p0` VALUES IN (0) ENGINE=InnoDB)"
    );

    h.conn->execute([&queries](decltype(h.conn) conn) {
        conn->begin();
        for (auto&& query: queries) {
            conn->execute(query);
        }
        conn->commit();
    });
}


void HttpIngestModule::_addPartitionToSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                     TransactionId transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(_qservMasterDbConnection());
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " ADD PARTITION (PARTITION `p" + to_string(transactionId) + "` VALUES IN (" + to_string(transactionId) +
        ") ENGINE=InnoDB)";

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


void HttpIngestModule::_removePartitionFromSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                          TransactionId transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(_qservMasterDbConnection());
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " DROP PARTITION `p" + to_string(transactionId) + "`";

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


void HttpIngestModule::_consolidateSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(_qservMasterDbConnection());
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " REMOVE PARTITIONING";

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


void HttpIngestModule::_deleteSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(_qservMasterDbConnection());
    string const query =
        "DROP TABLE IF EXISTS " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable);

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


bool HttpIngestModule::_qservSync(qhttp::Response::Ptr const& resp,
                                  DatabaseInfo const& databaseInfo,
                                  bool allWorkers) const {
    debug(__func__);

    bool const saveReplicaInfo = true;
    auto const findAlljob = FindAllJob::create(
        databaseInfo.family,
        saveReplicaInfo,
        allWorkers,
        controller()
    );
    findAlljob->start();
    logJobStartedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);
    findAlljob->wait();
    logJobFinishedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);

    if (findAlljob->extendedState() != Job::SUCCESS) {
        sendError(resp, __func__, "replica lookup stage failed");
        return false;
    }

    bool const force = false;
    auto const qservSyncJob = QservSyncJob::create(
        databaseInfo.family,
        force,
        qservSyncTimeoutSec(),
        controller()
    );
    qservSyncJob->start();
    logJobStartedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);
    qservSyncJob->wait();
    logJobFinishedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);

    if (qservSyncJob->extendedState() != Job::SUCCESS) {
        sendError(resp, __func__, "Qserv synchronization failed");
        return false;
    }
    return true;
}


database::mysql::Connection::Ptr HttpIngestModule::_qservMasterDbConnection() const {
    auto const config = controller()->serviceProvider()->config();
    return database::mysql::Connection::open(
        database::mysql::ConnectionParams(
            config->qservMasterDatabaseHost(),
            config->qservMasterDatabasePort(),
            "root",
            Configuration::qservMasterDatabasePassword(),
            "qservMeta"
        )
    );
}

}}}  // namespace lsst::qserv::replica

