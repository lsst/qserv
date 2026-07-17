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
#include "replica/services/ChunkMap.h"

// System headers
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLGenerator.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ChunkMap");
}

namespace lsst::qserv::replica {

using namespace database::mysql;

shared_ptr<ChunkMap> ChunkMap::create(shared_ptr<ServiceProvider> const& serviceProvider) {
    return shared_ptr<ChunkMap>(new ChunkMap(serviceProvider));
}

ChunkMap::ChunkMap(shared_ptr<ServiceProvider> const& serviceProvider)
        : _serviceProvider(serviceProvider), _chunks(make_shared<Chunks>()) {}

bool ChunkMap::update() {
    string const context = "ChunkMap::" + string(__func__) + " ";
    lock_guard<mutex> lock(_mtx);
    if (!_updateTransientState(lock) || _chunks->empty()) {
        // No changes in the chunk map, or the map is still empty so there's
        // nothing to do.
        return false;
    }

    // Open MySQL connection using the RAII-style handler that would automatically
    // abort the transaction should any problem occurred when loading data into the table.
    ConnectionHandler h;
    try {
        h.conn = Connection::open(Configuration::qservCzarDbParams("qservMeta"));
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             context << "failed to connect to the czar's database server, ex: " << ex.what());
        return false;
    }
    QueryGenerator const g(h.conn);

    // Pack the map into ready-to-ingest data.
    vector<string> rows;
    for (auto const& [workerName, databases] : *_chunks) {
        for (auto const& [databaseName, tables] : databases) {
            for (auto const& [tableName, chunks] : tables) {
                for (auto const [chunkId, size] : chunks) {
                    rows.push_back(g.packVals(workerName, databaseName, tableName, chunkId, size));
                }
            }
        }
    }

    // Get the limit for the length of the bulk insert queries. The limit is needed
    // to run the query generation.
    size_t maxQueryLength = 0;
    string const globalVariableName = "max_allowed_packet";
    try {
        string const query = g.showVars(SqlVarScope::GLOBAL, globalVariableName);
        h.conn->executeInOwnTransaction([&context, &query, &maxQueryLength](auto conn) {
            bool const noMoreThanOne = true;
            if (!selectSingleValue(conn, query, maxQueryLength, "Value", noMoreThanOne)) {
                throw runtime_error(context + "no such variable found");
            }
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             context << "failed to get a value of GLOBAL '" << globalVariableName << "', ex: " << ex.what());
        return false;
    }

    // Execute a sequence of queries atomically
    vector<string> const deleteQueries = {g.delete_("chunkMap"), g.delete_("chunkMapStatus")};
    vector<string> insertQueries = g.insertPacked(
            "chunkMap", g.packIds("worker", "database", "table", "chunk", "size"), rows, maxQueryLength);
    insertQueries.push_back(g.insert("chunkMapStatus", Sql::NOW));
    try {
        h.conn->executeInOwnTransaction([&deleteQueries, &insertQueries](auto conn) {
            for (auto const& query : deleteQueries) conn->execute(query);
            for (auto const& query : insertQueries) conn->execute(query);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR,
             context << "failed to update chunk map in the Czar database, ex: " << ex.what());
        return false;
    }
    LOGS(_log, LOG_LVL_INFO, context << "chunk map has been updated in the Czar database");
    return true;
}

json ChunkMap::toJson() const {
    lock_guard<mutex> lock(_mtx);
    return *_chunks;
}

bool ChunkMap::_updateTransientState(lock_guard<mutex> const& lock) {
    // Get info on known chunk replicas from the persistent store of the Replication system
    // and package those into the new chunk disposition map.
    bool const allDatabases = true;
    string const emptyDatabaseFilter;
    bool const isPublished = true;
    bool const includeFileInfo = true;  // need this to access tables sizes
    shared_ptr<Chunks> newChunks = make_shared<Chunks>();
    for (auto const& workerName : _serviceProvider->config()->workers()) {
        vector<ReplicaInfo> replicas;
        _serviceProvider->databaseServices()->findWorkerReplicas(replicas, workerName, emptyDatabaseFilter,
                                                                 allDatabases, isPublished, includeFileInfo);
        for (auto const& replica : replicas) {
            // Incomplete replicas should not be used by Czar for query processing.
            if (replica.status() != ReplicaInfo::Status::COMPLETE) continue;
            for (auto const& fileInfo : replica.fileInfo()) {
                if (fileInfo.isData() && !fileInfo.isOverlap()) {
                    (*newChunks)[workerName][replica.database()][fileInfo.baseTable()][replica.chunk()] =
                            fileInfo.size;
                }
            }
        }
    }

    // Update the current map if the new one is different from the current one.
    if (*_chunks != *newChunks) {
        _chunks = newChunks;
        return true;
    }
    return false;
}

}  // namespace lsst::qserv::replica