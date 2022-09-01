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
#include "replica/HttpCatalogsModule.h"

// System headers
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

// Initialize static members

json HttpCatalogsModule::_catalogsReport = json::object();
uint64_t HttpCatalogsModule::_catalogsReportTimeMs = 0;
util::Mutex HttpCatalogsModule::_catalogsMtx;

void HttpCatalogsModule::process(Controller::Ptr const& controller, string const& taskName,
                                 HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp, string const& subModuleName,
                                 HttpAuthType const authType) {
    HttpCatalogsModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpCatalogsModule::HttpCatalogsModule(Controller::Ptr const& controller, string const& taskName,
                                       HttpProcessorConfig const& processorConfig,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpCatalogsModule::executeImpl(string const& subModuleName) {
    debug(__func__);

    util::Lock lock(_catalogsMtx, "HttpCatalogsModule::" + string(__func__));

    // Check if a valid cached report is still available
    if (not _catalogsReport.is_null()) {
        // TODO: add a cache control parameter to the class's constructor,
        // or (even better) extract it from an optional parameter of the request
        // to let a client decide on how "stale" the result is expected to be.
        uint64_t const lastReportAgeMs = PerformanceUtils::now() - _catalogsReportTimeMs;
        if (lastReportAgeMs < 60 * 60 * 1000) return _catalogsReport;
    }

    // Otherwise, get the fresh snapshot of the replica distributions
    json result;
    result["databases"] = json::object();

    for (auto&& database : controller()->serviceProvider()->config()->databases()) {
        result["databases"][database] = _databaseStats(database);
    }

    // Update the cache
    _catalogsReport = result;
    _catalogsReportTimeMs = PerformanceUtils::now();

    return _catalogsReport;
}

json HttpCatalogsModule::_databaseStats(string const& database) const {
    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database);

    vector<ReplicaInfo> replicas;
    databaseServices->findDatabaseReplicas(replicas, database);

    json result;
    result["chunks"]["unique"] = chunks.size();
    result["chunks"]["with_replicas"] = replicas.size();

    map<string, map<string, size_t>> stats;
    for (auto&& table : config->databaseInfo(database).partitionedTables()) {
        auto&& statsTable = stats[table];
        statsTable["data_unique_in_chunks_data"] = 0;
        statsTable["data_unique_in_chunks_index"] = 0;
        statsTable["data_unique_in_overlaps_data"] = 0;
        statsTable["data_unique_in_overlaps_index"] = 0;
        statsTable["data_with_replicas_in_chunks_data"] = 0;
        statsTable["data_with_replicas_in_chunks_index"] = 0;
        statsTable["data_with_replicas_in_overlaps_data"] = 0;
        statsTable["data_with_replicas_in_overlaps_index"] = 0;
    }
    set<unsigned int> uniqueChunks;
    for (auto&& replica : replicas) {
        bool isUniqueChunk = false;
        if (uniqueChunks.count(replica.chunk()) == 0) {
            uniqueChunks.insert(replica.chunk());
            isUniqueChunk = true;
        }
        for (auto&& f : replica.fileInfo()) {
            auto const table = f.baseTable();
            auto const isData = f.isData();
            auto const isIndex = f.isIndex();
            auto const isOverlap = f.isOverlap();
            auto const size = f.size;
            auto&& statsTable = stats[table];
            if (isUniqueChunk) {
                if (isData) {
                    if (isOverlap)
                        statsTable["data_unique_in_overlaps_data"] += size;
                    else
                        statsTable["data_unique_in_chunks_data"] += size;
                }
                if (isIndex) {
                    if (isOverlap)
                        statsTable["data_unique_in_overlaps_index"] += size;
                    else
                        statsTable["data_unique_in_chunks_index"] += size;
                }
            }
            if (isData) {
                if (isOverlap)
                    statsTable["data_with_replica_in_overlaps_data"] += size;
                else
                    statsTable["data_with_replica_in_chunks_data"] += size;
            }
            if (isIndex) {
                if (isOverlap)
                    statsTable["data_with_replica_in_overlaps_index"] += size;
                else
                    statsTable["data_with_replica_in_chunks_index"] += size;
            }
        }
    }
    for (auto&& table : config->databaseInfo(database).partitionedTables()) {
        size_t numRowsInChunks = 0;
        size_t numRowsInOverlaps = 0;
        auto const tableRowStats = databaseServices->tableRowStats(database, table);
        for (auto&& e : tableRowStats.entries) {
            if (e.isOverlap)
                numRowsInOverlaps += e.numRows;
            else
                numRowsInChunks += e.numRows;
        }
        auto&& statsTable = stats[table];
        auto&& resultTable = result["tables"][table];
        resultTable["is_partitioned"] = 1;
        resultTable["rows"]["in_chunks"] = numRowsInChunks;
        resultTable["rows"]["in_overlaps"] = numRowsInOverlaps;
        resultTable["data"]["unique"]["in_chunks"]["data"] = statsTable["data_unique_in_chunks_data"];
        resultTable["data"]["unique"]["in_chunks"]["index"] = statsTable["data_unique_in_chunks_index"];
        resultTable["data"]["unique"]["in_overlaps"]["data"] = statsTable["data_unique_in_overlaps_data"];
        resultTable["data"]["unique"]["in_overlaps"]["index"] = statsTable["data_unique_in_overlaps_index"];
        resultTable["data"]["with_replicas"]["in_chunks"]["data"] =
                statsTable["data_with_replica_in_chunks_data"];
        resultTable["data"]["with_replicas"]["in_chunks"]["index"] =
                statsTable["data_with_replica_in_chunks_index"];
        resultTable["data"]["with_replicas"]["in_overlaps"]["data"] =
                statsTable["data_with_replica_in_overlaps_data"];
        resultTable["data"]["with_replicas"]["in_overlaps"]["index"] =
                statsTable["data_with_replica_in_overlaps_index"];
    }
    for (auto&& table : config->databaseInfo(database).regularTables()) {
        // TODO: implement this when the Replication system will support regular tables

        size_t data_unique_data = 0;
        size_t data_unique_index = 0;
        size_t data_with_replicas_data = 0;
        size_t data_with_replicas_index = 0;

        size_t numRows = 0;
        auto const tableRowStats = databaseServices->tableRowStats(database, table);
        for (auto&& e : tableRowStats.entries) {
            numRows += e.numRows;
        }
        auto&& resultTable = result["tables"][table];
        resultTable["is_partitioned"] = 0;
        resultTable["rows"] = numRows;
        resultTable["data"]["unique"]["data"] = data_unique_data;
        resultTable["data"]["unique"]["index"] = data_unique_index;
        resultTable["data"]["with_replicas"]["data"] = data_with_replicas_data;
        resultTable["data"]["with_replicas"]["index"] = data_with_replicas_index;
    }
    return result;
}

}  // namespace lsst::qserv::replica
