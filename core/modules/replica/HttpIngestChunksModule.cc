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
#include "replica/HttpIngestChunksModule.h"

// System headers
#include <limits>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/ChunkNumber.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpRequestBody.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

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
}


namespace lsst {
namespace qserv {
namespace replica {

HttpIngestChunksModule::Ptr HttpIngestChunksModule::create(
            Controller::Ptr const& controller,
            string const& taskName,
            HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpIngestChunksModule(controller, taskName, processorConfig));
}


HttpIngestChunksModule::HttpIngestChunksModule(
            Controller::Ptr const& controller,
            string const& taskName,
            HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpIngestChunksModule::executeImpl(qhttp::Request::Ptr const& req,
                                         qhttp::Response::Ptr const& resp,
                                         string const& subModuleName) {

    if (subModuleName == "ADD-CHUNK") {
         _addChunk(req, resp);
    } else if (subModuleName == "ADD-CHUNK-LIST") {
         _addChunks(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpIngestChunksModule::_addChunk(qhttp::Request::Ptr const& req,
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
    util::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

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


void HttpIngestChunksModule::_addChunks(qhttp::Request::Ptr const& req,
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
    util::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

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

}}}  // namespace lsst::qserv::replica
