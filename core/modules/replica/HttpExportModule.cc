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
 * Structure TableSpec represents a specification for a single table to
 * be exported.
 */
struct TableSpec {
    std::string   databaseName;         /// The name of a database which has the desired table
    std::string   tableName;            /// The base name of a table to be exported
    bool          partitioned = false;  /// Is 'true' for the partitioned tables
    unsigned int  chunk = 0;            /// The chunk number (partitioned tables)
    bool          overlap = false;      /// Is 'true' for the 'overlap' tables (partitioned tables)
    std::string   workerHost;           /// The host name or an IP address of a worker
    uint16_t      workerPort = 0;       /// The port number of the Export Service

    json toJson() const {
        json spec;
        spec["database"]    = databaseName;
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


void HttpExportModule::executeImpl(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp,
                                   string const& subModuleName) {

    if (subModuleName == "TABLES") {
        _getTables(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpExportModule::_getTables(qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp) {
    debug(__func__);

    HttpRequestBody body(req);
    auto const tables = body.requiredColl<json>("tables");

    debug(__func__, "tables.size()=" + tables.size());

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    // Get a collection of known workers which are in the 'ENABLED' state
    vector<WorkerInfo> allWorkerInfos;
    for (auto&& worker: config->workers()) {
        allWorkerInfos.push_back(config->workerInfo(worker));
    }
    if (allWorkerInfos.empty()) {
        sendError(resp, __func__, "no workers found in the Configuration of the system.");
        return;
    }

    // Validate input collection of tables and produce an extended collection
    // with table specifications to be returned back (to a caller).
    json result;
    for (auto&& table: tables) {

        TableSpec spec;

        spec.databaseName = table["database"];
        spec.tableName = table["table"];

        // This operation will throw an exception if the database name is not valid

        auto const databaseInfo = config->databaseInfo(spec.databaseName);
        if (not databaseInfo.isPublished) {
            sendError(resp, __func__, "database '" + databaseInfo.name + "' is not PUBLISHED");
            return;
        }

        // Additional verifications for the partitioned tables
        auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);
        spec.partitioned = databaseInfo.partitionedTables.end() != find(
                databaseInfo.partitionedTables.begin(),
                databaseInfo.partitionedTables.end(),
                spec.tableName);
        if (not spec.partitioned) {
            if (databaseInfo.regularTables.end() == find(
                    databaseInfo.regularTables.begin(),
                    databaseInfo.regularTables.end(),
                    spec.tableName)) {
                sendError(resp, __func__,
                        "no such table '" + spec.tableName + "' in a scope of database '" +
                        databaseInfo.name + "'");
                return;
            }
        }
        if (spec.partitioned) {
            spec.overlap = table["overlap"];
            spec.chunk = table["chunk"];
            // Make sure the chunk is valid for the given partitioning scheme.
            ChunkNumberQservValidator const validator(databaseFamilyInfo.numStripes,
                                                      databaseFamilyInfo.numSubStripes);
            if (not validator.valid(spec.chunk)) {
                sendError(resp, __func__, "chunk " + to_string(spec.chunk) + " is not valid");
                return;
            }
        }

        // Figure out a worker which has the table/chunk. Pick any worker for
        // the regular table because these tables are guaranteed to be fully replicated
        // in the 'PUBLISHED' catalogs. Pick the first worker which has the specific chunk
        // for the partitioned tables.
        //
        // TODO: consider load balancing workers if the number of table requests
        //       exceeds some limit.
        WorkerInfo workerInfo;
        if (spec.partitioned) {
            bool const enabledWorkersOnly = true;
            bool const includeFileInfo = false;
            vector<ReplicaInfo> replicas;
            databaseServices->findReplicas(
                replicas,
                spec.chunk,
                spec.databaseName,
                enabledWorkersOnly,
                includeFileInfo
            );
            if (replicas.empty()) {
                sendError(resp, __func__,
                        "no replica found for chunk " + to_string(spec.chunk) +
                        " of table '" + spec.tableName +
                        "' in a scope of database '" + spec.databaseName + "'");
                return;
            }
            workerInfo = config->workerInfo(replicas[0].worker());
        } else {
            workerInfo = allWorkerInfos[0];
        }
        spec.workerHost = workerInfo.exporterHost;
        spec.workerPort = workerInfo.exporterPort;

        result.push_back(spec.toJson());
    }
    sendData(resp, result);
}

}}}  // namespace lsst::qserv::replica
