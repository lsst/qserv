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
#include "replica/contr/HttpExportModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigWorker.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ChunkedTable.h"
#include "replica/util/ReplicaInfo.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * Structure TableSpec represents a specification for a single table to
 * be exported.
 */
struct TableSpec {
    std::string tableName;     ///< The base name of a table to be exported.
    bool partitioned = false;  ///< Is 'true' for the partitioned tables.
    unsigned int chunk = 0;    ///< The chunk number (partitioned tables).
    bool overlap = false;      ///< Is 'true' for the 'overlap' tables (partitioned tables).
    ConfigHost workerHost;     ///< The host name and an IP address of a worker.
    uint16_t workerPort = 0;   ///< The port number of the Export Service.

    json toJson() const {
        json spec;
        spec["baseName"] = tableName;
        spec["fullName"] = partitioned ? ChunkedTable(tableName, chunk, overlap).name() : tableName;
        spec["partitioned"] = partitioned ? 1 : 0;
        spec["chunk"] = chunk;
        spec["overlap"] = overlap ? 1 : 0;
        spec["worker_host"] = workerHost.addr;
        spec["worker_host_name"] = workerHost.name;
        spec["port"] = workerPort;
        return spec;
    }
};

}  // namespace

namespace lsst::qserv::replica {

void HttpExportModule::process(Controller::Ptr const& controller, string const& taskName,
                               HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp, string const& subModuleName,
                               http::AuthType const authType) {
    HttpExportModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpExportModule::HttpExportModule(Controller::Ptr const& controller, string const& taskName,
                                   HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpExportModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "TABLES") return _getTables();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpExportModule::_getTables() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const databaseName = params().at("database");
    auto const tablesJson = body().requiredColl<json>("tables");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "tables.size()=" + to_string(tablesJson.size()));

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    // This operation will throw an exception if the database name is not valid
    auto const database = config->databaseInfo(databaseName);
    if (not database.isPublished) {
        throw http::Error(__func__, "database '" + database.name + "' is not PUBLISHED");
    }

    // Get a collection of known workers which are in the 'ENABLED' state
    vector<ConfigWorker> allConfigWorkers;
    for (auto&& worker : config->workers()) {
        allConfigWorkers.push_back(config->worker(worker));
    }
    if (allConfigWorkers.empty()) {
        throw http::Error(__func__, "no workers found in the Configuration of the system.");
    }

    /**
     * The helper function is defined here to reduce code duplication along
     * in the rest of the current method. The function will locate the first
     * available (among 'ENABLED') worker hosting a replica of the specified
     * chunk. The ConfigWorker object will be returned upon successful completion
     * o f the function.
     *
     * @param chunk  The chunk number for
     * @return The ConfigWorker for the found worker
     * @throws invalid_argument in case if no replica was found for the specified
     *   chunk. For the databases in the 'PUBLISHED' state it means that the chunk
     *   doesn't exist.
     */
    auto const findWorkerForChunk = [&databaseServices, &config, &database](unsigned int chunk) {
        bool const enabledWorkersOnly = true;
        bool const includeFileInfo = false;
        vector<ReplicaInfo> replicas;
        databaseServices->findReplicas(replicas, chunk, database.name, enabledWorkersOnly, includeFileInfo);
        if (replicas.empty()) {
            throw invalid_argument("no replica found for chunk " + to_string(chunk) +
                                   " in a scope of database '" + database.name + "'.");
        }
        return config->worker(replicas[0].worker());
    };

    // The following algorithm has two modes of operation, depending on the content
    // of a collection of tables provided by a client. In either case, the algorithm
    // will make the following decisions on which worker to report if a table has
    // more than one replica. The choices are made based on a type of a table:
    //
    //   regular:     the first known worker since these tables are guaranteed to be
    //                fully replicated in the 'PUBLISHED' catalogs.
    //   partitioned: the first available replica.
    //
    // NOTE: The choice will be made among the so called 'ENABLED' workers.
    //       See a definition of the worker status in a design documentation
    //       of the Replication system.
    //
    // TODO: consider load balancing workers.
    try {
        json result;
        result["location"] = json::array();

        if (tablesJson.empty()) {
            // Report locations for all regular tables in the database
            for (auto&& tableName : database.regularTables()) {
                TableSpec spec;
                spec.tableName = tableName;
                spec.workerHost = allConfigWorkers[0].exporterHost;
                spec.workerPort = allConfigWorkers[0].exporterPort;
                result["location"].push_back(spec.toJson());
            }

            // The rest is for the partitioned tables
            bool const enabledWorkersOnly = true;
            vector<unsigned int> chunks;
            databaseServices->findDatabaseChunks(chunks, database.name, enabledWorkersOnly);
            for (auto chunk : chunks) {
                auto const workerInfo = findWorkerForChunk(chunk);

                for (auto&& tableName : database.partitionedTables()) {
                    TableSpec spec;

                    // One entry for the main chunk table itself
                    spec.tableName = tableName;
                    spec.partitioned = true;
                    spec.chunk = chunk;
                    spec.overlap = false;
                    spec.workerHost = workerInfo.exporterHost;
                    spec.workerPort = workerInfo.exporterPort;
                    result["location"].push_back(spec.toJson());

                    // Another (slightly modified) entry for the 'overlap' chunk table
                    spec.overlap = true;
                    result["location"].push_back(spec.toJson());
                }
            }
        } else {
            // Validate input collection of tables and produce an extended collection
            // with table specifications to be returned back (to a caller).

            for (auto&& tableJson : tablesJson) {
                TableSpec spec;
                spec.tableName = http::RequestBodyJSON::required<string>(tableJson, "table");
                spec.partitioned = database.findTable(spec.tableName).isPartitioned;
                if (spec.partitioned) {
                    spec.overlap = http::RequestBodyJSON::required<unsigned int>(tableJson, "overlap");
                    spec.chunk = http::RequestBodyJSON::required<unsigned int>(tableJson, "chunk");
                }
                ConfigWorker const worker =
                        spec.partitioned ? findWorkerForChunk(spec.chunk) : allConfigWorkers[0];
                spec.workerHost = worker.exporterHost;
                spec.workerPort = worker.exporterPort;

                result["location"].push_back(spec.toJson());
            }
        }
        return result;

    } catch (invalid_argument const& ex) {
        throw http::Error(__func__, ex.what());
    }
}

}  // namespace lsst::qserv::replica
