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

namespace lsst {
namespace qserv {
namespace replica {

HttpCatalogsModule::Ptr HttpCatalogsModule::create(Controller::Ptr const& controller,
                                                   string const& taskName,
                                                   unsigned int workerResponseTimeoutSec) {
    return Ptr(new HttpCatalogsModule(controller, taskName, workerResponseTimeoutSec));
}


HttpCatalogsModule::HttpCatalogsModule(Controller::Ptr const& controller,
                                       string const& taskName,
                                       unsigned int workerResponseTimeoutSec)
    :   HttpModule(controller,
                   taskName,
                   workerResponseTimeoutSec) {
}


void HttpCatalogsModule::executeImpl(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp,
                                     string const& subModuleName) {

    debug(__func__);

    util::Lock lock(_catalogsMtx, "HttpCatalogsModule::" + string(__func__));

    // Check if a cached report can be used
    if (not _catalogsReport.is_null()) {

        // Send what's available so far before evaluating the age of the cache
        // to see if it needs to be upgraded in the background.
        sendData(resp, _catalogsReport);

        uint64_t lastReportAgeMs = PerformanceUtils::now() - _catalogsReportTimeMs;
        if (lastReportAgeMs < 60 * 60 * 1000) return;

    } else {

        // Send a dummy report for now, then upgrade the cache
        bool const dummyReport = true;
        json result;
        for (auto&& database: controller()->serviceProvider()->config()->databases()) {
            result["databases"][database] = _databaseStats(database, dummyReport);
        }
        sendData(resp, result);
    }

    // Otherwise, get the fresh snapshot of the replica distributions
    json result;
    result["databases"] = json::object();

    for (auto&& database: controller()->serviceProvider()->config()->databases()) {
        result["databases"][database] = _databaseStats(database);
    }

    // Update the cache
    _catalogsReport = result;
    _catalogsReportTimeMs = PerformanceUtils::now();

    sendData(resp, _catalogsReport);
}


json HttpCatalogsModule::_databaseStats(string const& database, bool dummyReport) const {

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    vector<unsigned int> chunks;
    if (not dummyReport) databaseServices->findDatabaseChunks(chunks, database);

    vector<ReplicaInfo> replicas;
    if (not dummyReport) databaseServices->findDatabaseReplicas(replicas, database);

    json result;
    result["chunks"]["unique"] = chunks.size();
    result["chunks"]["with_replicas"] = replicas.size();

    map<string, map<string, size_t>> stats;
    for (auto&& table: config->databaseInfo(database).partitionedTables) {
        stats[table]["data_unique_in_chunks_data"] = 0;
        stats[table]["data_unique_in_chunks_index"] = 0;
        stats[table]["data_unique_in_overlaps_data"] = 0;
        stats[table]["data_unique_in_overlaps_index"] = 0;
        stats[table]["data_with_replicas_in_chunks_data"] = 0;
        stats[table]["data_with_replicas_in_chunks_index"] = 0;
        stats[table]["data_with_replicas_in_overlaps_data"] = 0;
        stats[table]["data_with_replicas_in_overlaps_index"] = 0;
    }
    set<unsigned int> uniqueChunks;
    for (auto&& replica: replicas) {
        bool isUniqueChunk = false;
        if (uniqueChunks.count(replica.chunk()) == 0) {
            uniqueChunks.insert(replica.chunk());
            isUniqueChunk = true;
        }
        for (auto&& f: replica.fileInfo()) {
            auto const table     = f.baseTable();
            auto const isData    = f.isData();
            auto const isIndex   = f.isIndex();
            auto const isOverlap = f.isOverlap();
            auto const size      = f.size;
            if (isUniqueChunk) {
                if (isData) {
                    if (isOverlap) stats[table]["data_unique_in_overlaps_data"] += size;
                    else           stats[table]["data_unique_in_chunks_data"]   += size;
                }
                if (isIndex) {
                    if (isOverlap) stats[table]["data_unique_in_overlaps_index"] += size;
                    else           stats[table]["data_unique_in_chunks_index"]   += size;
                }
            }
            if (isData) {
                if (isOverlap) stats[table]["data_with_replica_in_overlaps_data"] += size;
                else           stats[table]["data_with_replica_in_chunks_data"]   += size;
            }
            if (isIndex) {
                if (isOverlap) stats[table]["data_with_replica_in_overlaps_index"] += size;
                else           stats[table]["data_with_replica_in_chunks_index"]   += size;
            }            
        }
    }
    for (auto&& table: config->databaseInfo(database).partitionedTables) {
        result["tables"][table]["is_partitioned"] = 1;
        result["tables"][table]["rows"]["in_chunks"]   = 0;
        result["tables"][table]["rows"]["in_overlaps"] = 0;
        result["tables"][table]["data"]["unique"]["in_chunks"]["data"]    = stats[table]["data_unique_in_chunks_data"];
        result["tables"][table]["data"]["unique"]["in_chunks"]["index"]   = stats[table]["data_unique_in_chunks_index"];
        result["tables"][table]["data"]["unique"]["in_overlaps"]["data"]  = stats[table]["data_unique_in_overlaps_data"];
        result["tables"][table]["data"]["unique"]["in_overlaps"]["index"] = stats[table]["data_unique_in_overlaps_index"];
        result["tables"][table]["data"]["with_replicas"]["in_chunks"]["data"]    = stats[table]["data_with_replica_in_chunks_data"];
        result["tables"][table]["data"]["with_replicas"]["in_chunks"]["index"]   = stats[table]["data_with_replica_in_chunks_index"];
        result["tables"][table]["data"]["with_replicas"]["in_overlaps"]["data"]  = stats[table]["data_with_replica_in_overlaps_data"];
        result["tables"][table]["data"]["with_replicas"]["in_overlaps"]["index"] = stats[table]["data_with_replica_in_overlaps_index"];
    }
    for (auto&& table: config->databaseInfo(database).regularTables) {

        // TODO: implement this when the Replication system will support regular tables

        size_t data_unique_data  = 0;
        size_t data_unique_index = 0;
        size_t data_with_replicas_data  = 0;
        size_t data_with_replicas_index = 0;

        result["tables"][table]["is_partitioned"] = 0;
        result["tables"][table]["rows"] = 0;
        result["tables"][table]["data"]["unique"]["data"]  = data_unique_data;
        result["tables"][table]["data"]["unique"]["index"] = data_unique_index;
        result["tables"][table]["data"]["with_replicas"]["data"]  = data_with_replicas_data;
        result["tables"][table]["data"]["with_replicas"]["index"] = data_with_replicas_index;
    }
    return result;
}

}}}  // namespace lsst::qserv::replica

