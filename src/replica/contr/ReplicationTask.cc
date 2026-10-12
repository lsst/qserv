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
#include <stdexcept>
#include <vector>

// Qserv headers
#include "cconfig/DataManagementEvent.h"
#include "replica/jobs/FindAllJob.h"
#include "replica/jobs/FixUpJob.h"
#include "replica/jobs/ReplicateJob.h"
#include "replica/jobs/RebalanceJob.h"
#include "replica/jobs/PurgeJob.h"
#include "replica/registry/Registry.h"
#include "replica/qserv/PostEventQservCzarMgtRequest.h"
#include "replica/services/ChunkMap.h"

using namespace std;

namespace lsst::qserv::replica {

ReplicationTask::Ptr ReplicationTask::create(Controller::Ptr const& controller,
                                             Task::AbnormalTerminationCallbackType const& onTerminated) {
    return Ptr(new ReplicationTask(controller, onTerminated));
}

bool ReplicationTask::onRun() {
    auto const config = serviceProvider()->config();
    bool const saveReplicaInfo = true;
    bool const allWorkers = false;
    unsigned int const numReplicas = 0;  // Always assume the curently configured level for each family
    int const priority = config->get<int>("controller", "catalog-management-priority-level");

    launch<FindAllJob>(priority, saveReplicaInfo, allWorkers);
    sync();

    _updateChunkMap();

    launch<FixUpJob>(priority);
    sync();

    launch<ReplicateJob>(priority, numReplicas);
    sync();

    bool const estimateOnly = false;
    launch<RebalanceJob>(priority, estimateOnly);
    sync();

    if (config->get<unsigned int>("controller", "purge-excess-replicas") != 0) {
        launch<PurgeJob>(priority, numReplicas);
        sync();
    }

    // Keep on getting calls on this method after a wait time
    return true;
}

ReplicationTask::ReplicationTask(Controller::Ptr const& controller,
                                 Task::AbnormalTerminationCallbackType const& onTerminated)
        : Task(controller, "REPLICATION-THREAD  ", onTerminated,
               controller->serviceProvider()->config()->get<unsigned int>("controller",
                                                                          "replication-interval")) {}

void ReplicationTask::_updateChunkMap() {
    auto const config = serviceProvider()->config();
    if (config->get<unsigned int>("controller", "chunk-map-update") == 0) return;

    bool updated = serviceProvider()->chunkMap()->update();
    if (!updated) return;

    cconfig::DataManagementEvent event;
    event.type = cconfig::DataManagementEvent::Type::CHUNK_MAP_REBUILT;

    // This test prevents the Controller from exiting if the Registry is not available.
    // This solution deals with the startup ordering of services in the Qserv deployments.
    vector<ConfigCzar> czars;
    try {
        czars = serviceProvider()->registry()->czars();
    } catch (exception const& ex) {
        error("ReplicationTask::" + string(__func__) +
              ": failed to get a list of czars from the Registry service, ex: " + string(ex.what()));
        return;
    }
    for (auto const& czar : czars) {
        // Do not wait before the notification is sent, the chunk disposition map is already
        // updated in the database.
        PostEventQservCzarMgtRequest::create(serviceProvider(), czar.name, event)->start();
    }
}

}  // namespace lsst::qserv::replica
