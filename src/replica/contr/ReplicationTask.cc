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
#include "replica/contr/ReplicationTask.h"

// System headers
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/jobs/FindAllJob.h"
#include "replica/jobs/FixUpJob.h"
#include "replica/jobs/ReplicateJob.h"
#include "replica/jobs/RebalanceJob.h"
#include "replica/jobs/PurgeJob.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/services/DatabaseServices.h"
#include "replica/util/ReplicaInfo.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

using namespace database::mysql;

ReplicationTask::Ptr ReplicationTask::create(Controller::Ptr const& controller,
                                             Task::AbnormalTerminationCallbackType const& onTerminated,
                                             unsigned int qservSyncTimeoutSec, bool forceQservSync,
                                             bool qservChunkMapUpdate, unsigned int replicationIntervalSec,
                                             bool purge) {
    return Ptr(new ReplicationTask(controller, onTerminated, qservSyncTimeoutSec, forceQservSync,
                                   qservChunkMapUpdate, replicationIntervalSec, purge));
}

bool ReplicationTask::onRun() {
    bool const saveReplicaInfo = true;
    bool const allWorkers = false;
    unsigned int const numReplicas = 0;  // Always assume the curently configured level for each family
    int const priority =
            serviceProvider()->config()->get<int>("controller", "catalog-management-priority-level");

    launch<FindAllJob>(priority, saveReplicaInfo, allWorkers);
    sync(_qservSyncTimeoutSec, _forceQservSync);

    if (_qservChunkMapUpdate) _updateChunkMap();

    launch<FixUpJob>(priority);
    sync(_qservSyncTimeoutSec, _forceQservSync);

    launch<ReplicateJob>(priority, numReplicas);
    sync(_qservSyncTimeoutSec, _forceQservSync);

    bool const estimateOnly = false;
    launch<RebalanceJob>(priority, estimateOnly);
    sync(_qservSyncTimeoutSec, _forceQservSync);

    if (_purge) {
        launch<PurgeJob>(priority, numReplicas);
        sync(_qservSyncTimeoutSec, _forceQservSync);
    }

    // Keep on getting calls on this method after a wait time
    return true;
}

ReplicationTask::ReplicationTask(Controller::Ptr const& controller,
                                 Task::AbnormalTerminationCallbackType const& onTerminated,
                                 unsigned int qservSyncTimeoutSec, bool forceQservSync,
                                 bool qservChunkMapUpdate, unsigned int replicationIntervalSec, bool purge)
        : Task(controller, "REPLICATION-THREAD  ", onTerminated, replicationIntervalSec),
          _qservSyncTimeoutSec(qservSyncTimeoutSec),
          _forceQservSync(forceQservSync),
          _qservChunkMapUpdate(qservChunkMapUpdate),
          _purge(purge) {}

void ReplicationTask::_updateChunkMap() {
    // Get the current status of all known chunk replica across all enabled workers,
    // published databases and tables. Package this info into the JSON object of
    // the following schema:
    //
    //   {<worker>:{
    //     <database>:{
    //       <table>:[
    //         [<chunk>,<size>],
    //          ...
    //       ]
    //     }
    //  }

    bool const allDatabases = true;
    string const emptyDatabaseFilter;
    bool const isPublished = true;
    bool const includeFileInfo = true;  // need this to access tables sizes
    json chunkMap = json::object();
    for (auto const& workerName : serviceProvider()->config()->workers()) {
        vector<ReplicaInfo> replicas;
        serviceProvider()->databaseServices()->findWorkerReplicas(replicas, workerName, emptyDatabaseFilter,
                                                                  allDatabases, isPublished, includeFileInfo);
        chunkMap[workerName] = json::object();
        json& chunkMapWorker = chunkMap[workerName];
        for (auto const& replica : replicas) {
            for (auto const& fileInfo : replica.fileInfo()) {
                if (fileInfo.isData() && !fileInfo.isOverlap()) {
                    chunkMapWorker[replica.database()][fileInfo.baseTable()].push_back(
                            vector<uint64_t>({replica.chunk(), fileInfo.size}));
                }
            }
        }
    }

    // Open MySQL connection using the RAII-style handler that would automatically
    // abort the transaction should any problem occured when loading data into the table.
    ConnectionHandler h;
    try {
        h.conn = Connection::open(Configuration::qservCzarDbParams("qservMeta"));
    } catch (exception const& ex) {
        error("failed to connect to the czar's database server, ex: " + string(ex.what()));
        return;
    }

    // Load the map into the table and purge any previous maps (if any)
    QueryGenerator const g(h.conn);
    string const tableName = "chunkMap";
    auto const insertQuery = g.insert(tableName, chunkMap.dump(), Sql::NOW);
    auto const maxUpdateTimeSubQuery =
            g.subQuery(g.select(Sql::MAX_(g.id("update_time"))) + g.from(tableName));
    auto const deleteQuery =
            g.delete_(tableName) + g.where(g.notInSubQuery("update_time", maxUpdateTimeSubQuery));
    try {
        h.conn->executeInOwnTransaction([&insertQuery, &deleteQuery](auto conn) {
            conn->execute(insertQuery);
            conn->execute(deleteQuery);
        });
    } catch (exception const& ex) {
        error("failed to update chunk map in the Czar database, ex: " + string(ex.what()));
        return;
    }
}

}  // namespace lsst::qserv::replica
