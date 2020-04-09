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
#include "replica/HttpExportModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

/**
 * Structure TableSpec represents a specification for a single table to
 * be exported.
 */
struct TableSpec {
    std::string   tableName;            /// The base name of a table to be exported
    bool          partitioned = false;  /// Is 'true' for the partitioned tables
    unsigned int  chunk = 0;            /// The chunk number (partitioned tables)
    bool          overlap = false;      /// Is 'true' for the 'overlap' tables (partitioned tables)
    std::string   workerHost;           /// The host name or an IP address of a worker
    uint16_t      workerPort = 0;       /// The port number of the Export Service

    json toJson() const {
        json spec;
        spec["tableName"]   = tableName;
        spec["partitioned"] = partitioned ? 1 : 0;
        spec["chunk"]       = chunk;
        spec["overlap"]     = overlap ? 1 : 0;
        spec["worker"]      = workerHost;
        spec["port"]        = workerPort;
        return spec;
    }
};
    
}

namespace lsst {
namespace qserv {
namespace replica {

HttpExportModule::Ptr HttpExportModule::create(Controller::Ptr const& controller,
                                               string const& taskName,
                                               HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpExportModule(controller, taskName, processorConfig));
}


HttpExportModule::HttpExportModule(Controller::Ptr const& controller,
                                   string const& taskName,
                                   HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpExportModule::executeImpl(string const& subModuleName) {

    if (subModuleName == "TABLES") {
        _getTables();
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpExportModule::_getTables() {
    debug(__func__);

    auto const database = req()->params.at("database");
    auto const tables = body().requiredColl<json>("tables");

    debug(__func__, "database=" + database);
    debug(__func__, "tables.size()=" + to_string(tables.size()));

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    // This operation will throw an exception if the database name is not valid
    auto const databaseInfo = config->databaseInfo(database);
    if (not databaseInfo.isPublished) {
        sendError(__func__, "database '" + databaseInfo.name + "' is not PUBLISHED");
        return;
    }

    // Get a collection of known workers which are in the 'ENABLED' state
    vector<WorkerInfo> allWorkerInfos;
    for (auto&& worker: config->workers()) {
        allWorkerInfos.push_back(config->workerInfo(worker));
    }
    if (allWorkerInfos.empty()) {
        sendError(__func__, "no workers found in the Configuration of the system.");
        return;
    }

    /**
     * The helper function is defined here to reduce code duplication along
     * in the rest of the current method. The function will locate the first
     * available (among 'ENABLED') worker hosting a replica of the specified
     * chunk. The WorkerInfo object will be returned upon successful completion
     * o f the function.
     *
     * @param chunk  The chunk number for 
     * @return The WorkerInfo for the found worker
     * @throws invalid_argument in case if no replica was found for the specified
     *   chunk. For the databases in the 'PUBLISHED' state it means that the chunk
     *   doesn't exist.
     */
    auto const findWorkerForChunk =
            [&databaseServices, &config, &databaseInfo](unsigned int chunk) {
        bool const enabledWorkersOnly = true;
        bool const includeFileInfo = false;
        vector<ReplicaInfo> replicas;
        databaseServices->findReplicas(
            replicas,
            chunk,
            databaseInfo.name,
            enabledWorkersOnly,
            includeFileInfo
        );
        if (replicas.empty()) {
            throw invalid_argument(
                    "no replica found for chunk " + to_string(chunk) +
                    " in a scope of database '" + databaseInfo.name + "'.");
        }
        return config->workerInfo(replicas[0].worker());
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

    json result;
    result["location"] = json::array();

    try {
        if (tables.empty()) {

            // Report locations for all regular tables in the database
            for (auto&& table: databaseInfo.regularTables) {
                TableSpec spec;
                spec.tableName = table;
                spec.workerHost = allWorkerInfos[0].exporterHost;
                spec.workerPort = allWorkerInfos[0].exporterPort;
                result["location"].push_back(spec.toJson());
            }

            // The rest is for the partitioned tabled
            bool const enabledWorkersOnly = true;
            vector<unsigned int> chunks;
            databaseServices->findDatabaseChunks(chunks, databaseInfo.name, enabledWorkersOnly);
            for(auto chunk: chunks) {
                auto const workerInfo = findWorkerForChunk(chunk);

                for (auto&& table: databaseInfo.partitionedTables) {
                    TableSpec spec;

                    // One entry for the main chunk table itself
                    spec.tableName = table;
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

            for (auto&& table: tables) {

                TableSpec spec;
                spec.tableName = HttpRequestBody::required<string>(table, "table");
                spec.partitioned = databaseInfo.isPartitioned(spec.tableName);
                if (spec.partitioned) {
                    spec.overlap = HttpRequestBody::required<unsigned int>(table, "overlap");
                    spec.chunk   = HttpRequestBody::required<unsigned int>(table, "chunk");
                }
                WorkerInfo const workerInfo =
                        spec.partitioned ? findWorkerForChunk(spec.chunk) : allWorkerInfos[0];
                spec.workerHost = workerInfo.exporterHost;
                spec.workerPort = workerInfo.exporterPort;

                result["location"].push_back(spec.toJson());
            }
        }
    } catch (invalid_argument const& ex) {
        sendError(__func__, ex.what());
        return;
    }
    sendData(result);
}

}}}  // namespace lsst::qserv::replica
