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
#include "replica/contr/HttpIngestChunksModule.h"

// System headers
#include <limits>
#include <fstream>
#include <set>
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "replica/config/Configuration.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ChunkNumber.h"
#include "replica/util/ReplicaInfo.h"
#include "util/TimeUtils.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * @return the name of a worker which has the least number of replicas
 * among workers mentioned in the input collection of workers.
 */
template <typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker : workers) {
        size_t const num =
                databaseServices->numWorkerReplicas(candidateWorker, noSpecificDatabase, allDatabases);
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
template <typename COLLECTION_OF_WORKERS>
string leastLoadedWorker(map<string, size_t>& worker2replicasCache,
                         DatabaseServices::Ptr const& databaseServices,
                         COLLECTION_OF_WORKERS const& workers) {
    string worker;
    string const noSpecificDatabase;
    bool const allDatabases = true;
    size_t numReplicas = numeric_limits<size_t>::max();
    for (auto&& candidateWorker : workers) {
        auto itr = worker2replicasCache.find(candidateWorker);
        if (worker2replicasCache.end() == itr) {
            pair<decltype(itr), bool> itrSuccess = worker2replicasCache.insert(
                    {candidateWorker,
                     databaseServices->numWorkerReplicas(candidateWorker, noSpecificDatabase, allDatabases)});
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
}  // namespace

namespace lsst::qserv::replica {

replica::Mutex HttpIngestChunksModule::_ingestManagementMtx;

void HttpIngestChunksModule::process(Controller::Ptr const& controller, string const& taskName,
                                     HttpProcessorConfig const& processorConfig,
                                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                     string const& subModuleName, http::AuthType const authType) {
    HttpIngestChunksModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpIngestChunksModule::HttpIngestChunksModule(Controller::Ptr const& controller, string const& taskName,
                                               HttpProcessorConfig const& processorConfig,
                                               qhttp::Request::Ptr const& req,
                                               qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpIngestChunksModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "ADD-CHUNK")
        return _addChunk();
    else if (subModuleName == "ADD-CHUNK-MULTI")
        return _addChunkMulti();
    else if (subModuleName == "ADD-CHUNK-LIST")
        return _addChunks();
    else if (subModuleName == "ADD-CHUNK-LIST-MULTI")
        return _addChunksMulti();
    else if (subModuleName == "GET-CHUNK-LIST")
        return _getChunks();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpIngestChunksModule::_addChunk() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    unsigned int const chunk = body().required<unsigned int>("chunk");
    debug(__func__, "chunk=" + to_string(chunk));

    auto const databaseInfo = getDatabaseInfo(__func__);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    if (not validator.valid(chunk)) {
        throw http::Error(__func__, "this chunk number is not valid");
    }

    // This locks prevents other invocations of the method from making different
    // decisions on a chunk placement.
    replica::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

    // Decide on a worker where the chunk is best to be located.
    // If the chunk is already there then use it. Otherwise register an empty chunk
    // at some least loaded worker.
    //
    // ATTENTION: the current implementation of the algorithm assumes that
    // newly ingested chunks won't have replicas. This will change later
    // when the Replication system will be enhanced to allow creating replicas
    // of chunks within UNPUBLISHED databases.

    string workerName;

    bool const enabledWorkersOnly = true;
    bool const includeFileInfo = false;
    vector<ReplicaInfo> replicas;
    databaseServices->findReplicas(replicas, chunk, databaseInfo.name, enabledWorkersOnly, includeFileInfo);
    if (replicas.size() > 1) {
        json replicasJson = json::array();
        for (auto&& replica : replicas) {
            replicasJson.push_back(replica.toJson());
        }
        json extendedError;
        extendedError["replicas"] = replicasJson;
        throw http::Error(__func__, "this chunk has too many replicas", extendedError);
    }
    if (replicas.size() == 1) {
        workerName = replicas[0].worker();
    } else {
        // Search chunk in all databases of the same family to see
        // which workers may have replicas of the same chunk.
        // The idea here is to ensure the 'chunk colocation' requirements
        // is met, so that no unnecessary replica migration will be needed
        // when the database will be being published.

        bool const allDatabases = true;

        set<string> candidateWorkers;
        for (auto&& database : config->databases(databaseInfo.family, allDatabases)) {
            vector<ReplicaInfo> replicas;
            databaseServices->findReplicas(replicas, chunk, database, enabledWorkersOnly, includeFileInfo);
            for (auto&& replica : replicas) {
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

            workerName = ::leastLoadedWorker(databaseServices, candidateWorkers);

        } else {
            // We got here because no database within the family has a chunk
            // with this number. Hence we need to pick some least loaded worker
            // among all known workers.

            workerName = ::leastLoadedWorker(databaseServices, config->workers());
        }
        _registerNewReplica(workerName, databaseInfo.name, chunk);
        ControllerEvent event;
        event.status = "ADD CHUNK";
        event.kvInfo.emplace_back("database", databaseInfo.name);
        event.kvInfo.emplace_back("worker", workerName);
        event.kvInfo.emplace_back("chunk", to_string(chunk));
        logEvent(event);
    }

    // The sanity check, just to make sure we've found a worker
    if (workerName.empty()) {
        throw http::Error(__func__, "no suitable worker found");
    }

    auto const worker = config->worker(workerName);

    json result = json::object();
    result["location"] = json::object({{"chunk", chunk},
                                       {"worker", worker.name},
                                       {"host", worker.loaderHost.addr},
                                       {"host_name", worker.loaderHost.name},
                                       {"port", worker.loaderPort},
                                       {"http_host", worker.httpLoaderHost.addr},
                                       {"http_host_name", worker.httpLoaderHost.name},
                                       {"http_port", worker.httpLoaderPort}});
    return result;
}

json HttpIngestChunksModule::_addChunkMulti() {
    debug(__func__);
    checkApiVersion(__func__, 54);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    unsigned int const chunk = body().required<unsigned int>("chunk");
    debug(__func__, "chunk=" + to_string(chunk));

    auto const databaseInfo = getDatabaseInfo(__func__);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    if (not validator.valid(chunk)) {
        throw http::Error(__func__, "this chunk number is not valid");
    }

    // This locks prevents other invocations of the method from making different
    // decisions on a chunk placement.
    replica::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

    // Preload the existing replicas (if any) for this chunk.
    bool const enabledWorkersOnly = true;
    bool const includeFileInfo = false;
    vector<ReplicaInfo> existingReplicas;
    databaseServices->findReplicas(existingReplicas, chunk, databaseInfo.name, enabledWorkersOnly,
                                   includeFileInfo);

    // The transient cache of replica disposition for workers. This will be used
    // for optimizing the selection of workers for chunk placements. Otheriwise
    // relatively expensive database queries will be needed for each chunk.
    map<string, size_t> worker2replicasCache;

    json result = json::object({{"locations", json::array()}});
    auto const numReplicasRegistered =
            _addChunk(worker2replicasCache, result["locations"], chunk, databaseInfo, existingReplicas);
    ControllerEvent event;
    event.status = "ADD CHUNKS";
    event.kvInfo.emplace_back("database", databaseInfo.name);
    event.kvInfo.emplace_back("num_replicas_registered", to_string(numReplicasRegistered));
    logEvent(event);
    return result;
}

json HttpIngestChunksModule::_addChunks() {
    debug(__func__);
    checkApiVersion(__func__, 54);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const chunks = body().requiredColl<unsigned int>("chunks");
    debug(__func__, "chunks.size()=" + chunks.size());

    auto const databaseInfo = getDatabaseInfo(__func__);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Make sure chunk numbers are valid for the given
    // partitioning scheme.
    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    for (auto const chunk : chunks) {
        if (not validator.valid(chunk)) {
            throw http::Error(__func__, "chunk " + to_string(chunk) + " is not valid");
        }
    }

    // This locks prevents other invocations of the method from making different
    // decisions on chunk placements.
    replica::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

    // The transient cache of replica disposition for workers. This will be used
    // for optimizing the selection of workers for chunk placements. Otherwise
    // relatively expensive database queries will be needed for each chunk.
    map<unsigned int, vector<ReplicaInfo>> chunks2replicas = _chunks2Replicas(chunks, databaseInfo);

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
    map<string, size_t> worker2replicasCache;
    map<unsigned int, string> chunk2worker;
    size_t numReplicasRegistered = 0;
    for (auto const chunk : chunks) {
        vector<ReplicaInfo> const& existingReplicas = chunks2replicas.at(chunk);
        if (existingReplicas.size() > 1) {
            throw http::Error(__func__, "chunk " + to_string(chunk) + " has too many replicas");
        }
        if (existingReplicas.size() == 1) {
            chunk2worker[chunk] = existingReplicas[0].worker();
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
            for (auto&& database : databases) {
                bool const enabledWorkersOnly = true;
                bool const includeFileInfo = false;
                vector<ReplicaInfo> replicas;
                databaseServices->findReplicas(replicas, chunk, database, enabledWorkersOnly,
                                               includeFileInfo);
                for (auto&& replica : replicas) {
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
                chunk2worker[chunk] =
                        ::leastLoadedWorker(worker2replicasCache, databaseServices, candidateWorkers);
            } else {
                // We got here because no database within the family has a chunk
                // with this number. Hence we need to pick some least loaded worker
                // among all known workers.
                chunk2worker[chunk] =
                        ::leastLoadedWorker(worker2replicasCache, databaseServices, config->workers());
            }
            _registerNewReplica(chunk2worker[chunk], databaseInfo.name, chunk);
            numReplicasRegistered++;
        }

        // The sanity check, just to make sure we've found a worker
        if (chunk2worker[chunk].empty()) {
            throw http::Error(__func__, "no suitable worker found for chunk " + to_string(chunk));
        }
    }

    // Note, that the group operation for chunks will report the total
    // number of chunks allocated by the service rather than individual chunks.
    // This is done to avoid flooding the log with too many specific details on
    // the operation which (the details) could be found in the replica disposition
    // table.
    ControllerEvent event;
    event.status = "ADD CHUNKS";
    event.kvInfo.emplace_back("database", databaseInfo.name);
    event.kvInfo.emplace_back("num_replicas_registered", to_string(numReplicasRegistered));
    logEvent(event);

    // Process the chunk-to-worker map into a result object to be
    // returned to a client.
    json result = json::object({{"locations", json::array()}});
    for (auto const chunk : chunks) {
        auto const worker = config->worker(chunk2worker[chunk]);
        result["locations"].push_back(json::object({
                {"chunk", chunk},
                {"worker", worker.name},
                {"host", worker.loaderHost.addr},
                {"host_name", worker.loaderHost.name},
                {"port", worker.loaderPort},
                {"http_host", worker.httpLoaderHost.addr},
                {"http_host_name", worker.httpLoaderHost.name},
                {"http_port", worker.httpLoaderPort},
        }));
    }
    return result;
}

json HttpIngestChunksModule::_addChunksMulti() {
    debug(__func__);
    checkApiVersion(__func__, 54);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const chunks = body().requiredColl<unsigned int>("chunks");
    debug(__func__, "chunks.size()=" + chunks.size());

    auto const databaseInfo = getDatabaseInfo(__func__);
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Make sure chunk numbers are valid for the given
    // partitioning scheme.
    ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                              databaseFamilyInfo.numSubStripes);
    for (auto const chunk : chunks) {
        if (not validator.valid(chunk)) {
            throw http::Error(__func__, "chunk " + to_string(chunk) + " is not valid");
        }
    }

    // This locks prevents other invocations of the method from making different
    // decisions on a chunk placement.
    replica::Lock lock(_ingestManagementMtx, "HttpIngestChunksModule::" + string(__func__));

    // The transient cache of replica disposition for workers. This will be used
    // for optimizing the selection of workers for chunk placements. Otherwise
    // relatively expensive database queries will be needed for each chunk.
    map<unsigned int, vector<ReplicaInfo>> chunks2replicas = _chunks2Replicas(chunks, databaseInfo);

    // The transient cache of replica disposition for workers. This will be used
    // for optimizing the selection of workers for chunk placements. Otheriwise
    // relatively expensive database queries will be needed for each chunk.
    map<string, size_t> worker2replicasCache;

    json result = json::object({{"locations", json::array()}});
    size_t numReplicasRegistered = 0;
    for (auto const chunk : chunks) {
        vector<ReplicaInfo> const& existingReplicas = chunks2replicas.at(chunk);
        numReplicasRegistered +=
                _addChunk(worker2replicasCache, result["locations"], chunk, databaseInfo, existingReplicas);
    }
    ControllerEvent event;
    event.status = "ADD CHUNKS";
    event.kvInfo.emplace_back("database", databaseInfo.name);
    event.kvInfo.emplace_back("num_replicas_registered", to_string(numReplicasRegistered));
    logEvent(event);
    return result;
}

map<unsigned int, vector<ReplicaInfo>> HttpIngestChunksModule::_chunks2Replicas(
        vector<unsigned int> const& chunks, DatabaseInfo const& databaseInfo) const {
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    bool const enabledWorkersOnly = true;
    bool const includeFileInfo = false;
    vector<ReplicaInfo> replicas;
    databaseServices->findReplicas(replicas, chunks, databaseInfo.name, enabledWorkersOnly, includeFileInfo);
    map<unsigned int, vector<ReplicaInfo>> chunks2replicas;
    for (auto&& replica : replicas) {
        chunks2replicas[replica.chunk()].push_back(replica);
    }

    // Create empty entries for chunks which don't have replicas.
    for (auto const chunk : chunks) {
        auto itr = chunks2replicas.find(chunk);
        if (chunks2replicas.end() == itr) {
            chunks2replicas[chunk] = vector<ReplicaInfo>();
        }
    }
    return chunks2replicas;
}

size_t HttpIngestChunksModule::_addChunk(map<string, size_t>& worker2replicasCache, json& locations,
                                         unsigned int const chunk, DatabaseInfo const& databaseInfo,
                                         vector<ReplicaInfo> const& existingReplicas) const {
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    size_t numReplicasRegistered = 0;

    // Workers that currently host this chunk or are eligible to host it.
    set<string> workerNames;

    if (existingReplicas.size() > 0) {
        for (auto const& replica : existingReplicas) {
            workerNames.insert(replica.worker());
        }
    } else {
        // Search chunk in all databases of the same family to see which workers may have replicas of
        // the same chunk. The idea here is to ensure the 'chunk colocation' requirements are met, so
        // that no unnecessary replica migration will be needed when the database is published.
        bool const allDatabases = true;
        for (auto&& database : config->databases(databaseInfo.family, allDatabases)) {
            bool const enabledWorkersOnly = true;
            bool const includeFileInfo = false;
            vector<ReplicaInfo> replicas;
            databaseServices->findReplicas(replicas, chunk, database, enabledWorkersOnly, includeFileInfo);
            for (auto const& replica : replicas) {
                workerNames.insert(replica.worker());
            }
        }
        if (workerNames.empty()) {
            // We got here because no database within the family has a chunk with this number.
            // Hence we need to pick some least loaded worker among all known workers.
            workerNames.insert(
                    ::leastLoadedWorker(worker2replicasCache, databaseServices, config->workers()));
        }

        // At this state candidate workers are used for registering chunk replicas in a context
        // of the current database.
        for (auto&& workerName : workerNames) {
            _registerNewReplica(workerName, databaseInfo.name, chunk);
            ++numReplicasRegistered;
        }
    }
    if (workerNames.empty()) {
        throw http::Error(__func__, "no suitable workers found for chunk=" + to_string(chunk));
    }
    for (auto&& workerName : workerNames) {
        auto const worker = config->worker(workerName);
        locations.push_back(json::object({{"chunk", chunk},
                                          {"worker", worker.name},
                                          {"host", worker.loaderHost.addr},
                                          {"host_name", worker.loaderHost.name},
                                          {"port", worker.loaderPort},
                                          {"http_host", worker.httpLoaderHost.addr},
                                          {"http_host_name", worker.httpLoaderHost.name},
                                          {"http_port", worker.httpLoaderPort}}));
    }
    return numReplicasRegistered;
}

json HttpIngestChunksModule::_getChunks() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    string const database = query().requiredString("database");
    debug(__func__, "database=" + database);

    auto const databaseInfo = config->databaseInfo(database);

    // Locate replicas (if any) for all chunks of the database.
    bool const enabledWorkersOnly = true;
    vector<ReplicaInfo> replicas;
    databaseServices->findDatabaseReplicas(replicas, database, enabledWorkersOnly);

    // Build the chunk-to-worker map to be returned to a client in the result
    // object. Note that published databases may have more than 1 replica of a chunk.

    json result;
    result["replica"] = json::array();

    for (auto const& replica : replicas) {
        json replicaResult;
        replicaResult["chunk"] = replica.chunk();
        replicaResult["worker"] = replica.worker();

        // Initialize required attributes to the default values (all 0) for all
        // relevant tables.
        for (auto&& table : databaseInfo.partitionedTables()) {
            replicaResult[table]["overlap_rows"] = 0;  // TBC when available
            replicaResult[table]["overlap_data_size"] = 0;
            replicaResult[table]["overlap_index_size"] = 0;

            replicaResult[table]["rows"] = 0;  // TBC when available
            replicaResult[table]["data_size"] = 0;
            replicaResult[table]["index_size"] = 0;
        }

        // fetch and report actual values of the attributes.
        for (auto&& file : replica.fileInfo()) {
            auto& replicaTableResult = replicaResult[file.baseTable()];
            if (file.isOverlap()) {
                if (file.isData()) {
                    replicaTableResult["overlap_data_size"] = file.size;
                } else if (file.isIndex()) {
                    replicaTableResult["overlap_index_size"] = file.size;
                }
            } else {
                if (file.isData()) {
                    replicaTableResult["data_size"] = file.size;
                } else if (file.isIndex()) {
                    replicaTableResult["index_size"] = file.size;
                }
            }
        }
        result["replica"].push_back(replicaResult);
    }
    return result;
}

void HttpIngestChunksModule::_registerNewReplica(string const& worker, string const& database,
                                                 unsigned int chunk) const {
    auto const verifyTime = util::TimeUtils::now();
    ReplicaInfo const newReplica(ReplicaInfo::Status::COMPLETE, worker, database, chunk, verifyTime);
    controller()->serviceProvider()->databaseServices()->saveReplicaInfo(newReplica);
}

}  // namespace lsst::qserv::replica
