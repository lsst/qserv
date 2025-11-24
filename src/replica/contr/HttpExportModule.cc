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
#include "global/stringUtil.h"  // for qserv::stoui
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigWorker.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ChunkedTable.h"
#include "replica/util/ReplicaInfo.h"

using namespace std;
using json = nlohmann::json;

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
    if (subModuleName == "CONFIG-DATABASE") return _getDatabaseConfig();
    if (subModuleName == "CONFIG-TABLE") return _getTableConfig();
    if (subModuleName == "TABLE-LOCATIONS") return _getTableLocations();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpExportModule::_getDatabaseConfig() {
    debug(__func__);
    checkApiVersion(__func__, 53);

    auto const databaseName = params().at("database");
    debug(__func__, "database=" + databaseName);

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    if (!database.isPublished) {
        throw http::Error(__func__, "database '" + database.name + "' is not PUBLISHED");
    }
    auto const family = config->databaseFamilyInfo(database.family);

    // Note the version number of the API coresponds to the actual version of the database
    // registration service that existed at a time this generator was written. It is not related
    // to the version of the generator. The version number could be further adjusted
    // by the ingest workflow if needed.
    auto const result = json::object({{"version", 12},
                                      {"database", database.name},
                                      {"auto_build_secondary_index", 0},
                                      {"num_stripes", family.numStripes},
                                      {"num_sub_stripes", family.numSubStripes},
                                      {"overlap", family.overlap}});
    return json::object({{"config", result}});
}

json HttpExportModule::_getTableConfig() {
    debug(__func__);
    checkApiVersion(__func__, 53);

    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");
    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    if (!database.isPublished) {
        throw http::Error(__func__, "database '" + database.name + "' is not PUBLISHED");
    }
    auto const table = database.findTable(tableName);
    if (!table.isPublished) {
        throw http::Error(__func__, "table '" + tableName + "' of " + database.name + " is not PUBLISHED");
    }

    // Note the version number of the API coresponds to the actual version of the table
    // registration service that existed at a time this generator was written. It is not related
    // to the version of the generator. The version number could be further adjusted
    // by the ingest workflow if needed.
    json result = json::object({{"version", 49},
                                {"database", database.name},
                                {"table", table.name},
                                {"charset_name", table.charsetName},
                                {"collation_name", table.collationName},
                                {"is_partitioned", table.isPartitioned}});

    // The optional attributes for the partitioned tables only.
    if (table.isPartitioned) {
        result["director_table"] = table.directorTable.tableName();
        result["director_key"] = table.directorTable.primaryKeyColumn();
        if (table.isDirector()) {
            result["unique_primary_key"] = table.uniquePrimaryKey ? 1 : 0;
        }
        if (table.isRefMatch()) {
            result["director_table2"] = table.directorTable2.tableName();
            result["director_key2"] = table.directorTable2.primaryKeyColumn();
            result["ang_sep"] = table.angSep;
            result["flag"] = table.flagColName;
        }
        result["latitude_key"] = table.latitudeColName;
        result["longitude_key"] = table.longitudeColName;
    }

    // Extract schema from czar's MySQL database.
    set<string> const columnsToExclude = {"qserv_trans_id"};
    database::mysql::ConnectionHandler const h(qservMasterDbConnection(database.name));
    result["schema"] =
            database::mysql::tableSchemaForCreate(h.conn, database.name, table.name, columnsToExclude);

    return json::object({{"config", result}});
}

json HttpExportModule::_getTableLocations() {
    debug(__func__);
    checkApiVersion(__func__, 53);

    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");
    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    if (!database.isPublished) {
        throw http::Error(__func__, "database '" + database.name + "' is not PUBLISHED");
    }
    auto const table = database.findTable(tableName);
    if (!table.isPublished) {
        throw http::Error(__func__, "table '" + tableName + "' of " + database.name + " is not PUBLISHED");
    }
    auto workerLocation = [&config](string const& workerName) -> json {
        auto const worker = config->worker(workerName);
        return json::object({{"worker", worker.name},
                             {"host", worker.exporterHost.toJson()},
                             {"port", worker.exporterPort}});
    };
    if (table.isPartitioned) {
        // The first step of the algorithm is to build a mapping from chunk numbers
        // to locations (workers) hosting replicas of the specified table.
        //
        // Note that the first phase of the algorithm can be a bit slow since it
        // needs to query the database for each chunk of the specified table. It may take
        // many seconds (or a few minutes) to finish depending on the number of chunks
        // in the table and on a performance of the underlying database server.
        // Unfortunately there is no easy way to optimize this process in the current
        // implementation of the Replication database Services (and the underlying schema)
        json chunk2locations = json::object();
        bool const enabledWorkersOnly = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database.name, enabledWorkersOnly);
        for (auto const chunk : chunks) {
            bool const includeFileInfo = true;  // to see the names of the base tables
            vector<ReplicaInfo> replicas;
            databaseServices->findReplicas(replicas, chunk, database.name, enabledWorkersOnly,
                                           includeFileInfo);
            for (auto const& replica : replicas) {
                // Incomplete replicas are ignored since they may not have the full set
                // of files for the specified table.
                if (replica.status() != ReplicaInfo::Status::COMPLETE) continue;

                for (auto const& file : replica.fileInfo()) {
                    // In the current implementation of Qserv, tables are allowed not to participate
                    // in all partitions (chunks). Different tables of the same catalog may have
                    // different spatial coverage.
                    if (file.baseTable() != table.name) continue;

                    // Stop iterating here to prevent adding multiple entries (locations) of a chunk
                    // for the same worker. Note that the JSON object doesn't allow numeric keys,
                    // so we need to convert the chunk number to a string.
                    chunk2locations[to_string(chunk)].push_back(workerLocation(replica.worker()));
                    break;
                }
            }
        }

        // The second step is to populate the result set.
        json result = json::object({{"chunks", json::array()}});
        for (auto const& item : chunk2locations.items()) {
            result["chunks"].push_back(
                    json::object({{"chunk", qserv::stoui(item.key())}, {"locations", item.value()}}));
        }
        return result;
    } else {
        json result = json::object({{"locations", json::array()}});
        bool const isEnabled = true;
        auto const workerNames = config->workers(isEnabled);
        if (workerNames.empty()) {
            throw http::Error(__func__, "no workers found in the Configuration of the system.");
        }
        for (auto const& workerName : workerNames) {
            result["locations"].push_back(workerLocation(workerName));
        }
        return result;
    }
}

}  // namespace lsst::qserv::replica
