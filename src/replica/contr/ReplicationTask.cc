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

// Qserv headers
#include "replica/jobs/FindAllJob.h"
#include "replica/jobs/FixUpJob.h"
#include "replica/jobs/ReplicateJob.h"
#include "replica/jobs/RebalanceJob.h"
#include "replica/jobs/PurgeJob.h"

using namespace std;

namespace lsst::qserv::replica {

ReplicationTask::Ptr ReplicationTask::create(Controller::Ptr const& controller,
                                             Task::AbnormalTerminationCallbackType const& onTerminated,
                                             unsigned int qservSyncTimeoutSec, bool forceQservSync,
                                             unsigned int replicationIntervalSec, bool purge) {
    return Ptr(new ReplicationTask(controller, onTerminated, qservSyncTimeoutSec, forceQservSync,
                                   replicationIntervalSec, purge));
}

bool ReplicationTask::onRun() {
    bool const saveReplicaInfo = true;
    bool const allWorkers = false;
    unsigned int const numReplicas = 0;  // Always assume the curently configured level for each family
    int const priority =
            serviceProvider()->config()->get<int>("controller", "catalog-management-priority-level");

    launch<FindAllJob>(priority, saveReplicaInfo, allWorkers);
    sync(_qservSyncTimeoutSec, _forceQservSync);

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
                                 unsigned int replicationIntervalSec, bool purge)
        : Task(controller, "REPLICATION-THREAD  ", onTerminated, replicationIntervalSec),
          _qservSyncTimeoutSec(qservSyncTimeoutSec),
          _forceQservSync(forceQservSync),
          _purge(purge) {}

}  // namespace lsst::qserv::replica
