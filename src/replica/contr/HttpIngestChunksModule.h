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
#ifndef LSST_QSERV_HTTPINGESTCHUNKSMODULE_H
#define LSST_QSERV_HTTPINGESTCHUNKSMODULE_H

// System headers
#include <map>
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/HttpModule.h"
#include "replica/util/Common.h"
#include "replica/util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class DatabaseInfo;
class ReplicaInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpIngestChunksModule provides a support for registering new chunks
 * (or querying chunk disposition) in the Replication system as needed during
 * catalog ingest.
 */
class HttpIngestChunksModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestChunksModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   ADD-CHUNK              - for registering (or requesting a status of) of a new chunk
     *   ADD-CHUNK-MULTI        - for registering (or requesting a status of) of a new chunk
     *                            at (possibly) multiple workers
     *   ADD-CHUNK-LIST         - for registering (or requesting a status of) of many new chunks
     *   ADD-CHUNK-LIST-MULTI   - for registering (or requesting a status of) of many new chunks
     *                            at (possibly) multiple workers for each chunk
     *   GET-CHUNK-LIST         - return the chunk allocation map for a database
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpIngestChunksModule() = delete;
    HttpIngestChunksModule(HttpIngestChunksModule const&) = delete;
    HttpIngestChunksModule& operator=(HttpIngestChunksModule const&) = delete;

    ~HttpIngestChunksModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestChunksModule(Controller::Ptr const& controller, std::string const& taskName,
                           HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                           qhttp::Response::Ptr const& resp);

    /**
     * Register (if it's not register yet) a chunk for ingest.
     * @return connection parameters to an end-point service where chunk data will need to be ingested.
     */
    nlohmann::json _addChunk();

    /**
     * Register (if it's not register yet) a chunk for ingest at (possibly) multiple workers.
     * @return connection parameters to an end-point service where chunk data will need to be ingested.
     */
    nlohmann::json _addChunkMulti();

    /**
     * Register (if it's not register yet) a list of chunks for ingest.
     * @return connection parameters to an end-point services (may differ
     *  from chunk to chunk) where data of each chunk will need to be ingested.
     */
    nlohmann::json _addChunks();

    /**
     * Register (if it's not register yet) a list of chunks for ingest at (possibly) multiple workers.
     * @return connection parameters to an end-point services (may differ
     *  from chunk to chunk) where data of each chunk will need to be ingested.
     */
    nlohmann::json _addChunksMulti();

    /**
     * @param chunks A list of chunk numbers to be inspected.
     * @param databaseInfo The database the chunks belong to.
     * @return A chunk allocation map for a database.
     * @note The output collection is guaranteed to have entries for input chunks including
     *   those that are not registered yet.
     */
    std::map<unsigned int, std::vector<ReplicaInfo>> _chunks2Replicas(std::vector<unsigned int> const& chunks,
                                                                      DatabaseInfo const& databaseInfo) const;

    /**
     * Figure out and fill a chunks allocation map for a database. This method is used
     * in implementations of the above-defined methods _addChunkMulti and _addChunksMulti.
     * @param worker2replicasCache A transient cache of replica disposition for workers. This will
     *  be used for optimizing the selection of workers for chunk placements. Otheriwise relatively
     *  expensive database queries will be needed for each chunk.
     * @param locations A JSON array to be filled with the chunk allocation map.
     * @param chunk The number of a chunk to be registered.
     * @param databaseInfo The database the chunk belongs to.
     * @param existingReplicas The list of existing replicas for the chunk in the given database.
     *  This is used to optimize the selection of workers for chunk placements.
     * @return The number of new replicas registered for the chunk.
     */
    size_t _addChunk(std::map<std::string, size_t>& worker2replicasCache, nlohmann::json& locations,
                     unsigned int const chunk, DatabaseInfo const& databaseInfo,
                     std::vector<ReplicaInfo> const& existingReplicas) const;

    /**
     * Register new replica of a chunk.
     *
     * @note In the current version of the operation, the chunk will be registered
     *   with status COMPLETE. This decision will be reconsidered later after
     *   extending schema of table 'replica' to store the status as well. This would
     *   make it possible to differentiate between the 'INGEST_PRIMARY' and 'INGEST_SECONDARY'
     *   replicas for selecting the right version of the replica for further ingests.
     *
     * @param worker    The name of a worker this chunk will be placed at.
     * @param database  The name of the database the chunk belongs to.
     * @param chunk     The number of a chunk to be registered.
     */
    void _registerNewReplica(std::string const& worker, std::string const& database,
                             unsigned int chunk) const;

    /**
     * Return a chunks allocation map for a database.
     */
    nlohmann::json _getChunks();

    static replica::Mutex _ingestManagementMtx;  /// Synchronized access to the chunk management operations
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPINGESTCHUNKSMODULE_H
