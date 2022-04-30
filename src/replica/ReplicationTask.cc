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
#include "replica/ReplicationTask.h"

// Qserv headers
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/ReplicateJob.h"
#include "replica/RebalanceJob.h"
#include "replica/PurgeJob.h"

using namespace std;

namespace lsst::qserv::replica {

ReplicationTask::Ptr ReplicationTask::create(Controller::Ptr const& controller,
                                             Task::AbnormalTerminationCallbackType const& onTerminated,
                                             unsigned int qservSyncTimeoutSec,
                                             unsigned int replicationIntervalSec, unsigned int numReplicas,
                                             bool purge) {
    return Ptr(new ReplicationTask(controller, onTerminated, qservSyncTimeoutSec, replicationIntervalSec,
                                   numReplicas, purge));
}

bool ReplicationTask::onRun() {
    bool const saveReplicaInfo = true;
    bool const allWorkers = false;
    int const priority =
            serviceProvider()->config()->get<int>("controller", "catalog-management-priority-level");

    launch<FindAllJob>(priority, saveReplicaInfo, allWorkers);
    sync(_qservSyncTimeoutSec);

    launch<FixUpJob>(priority);
    sync(_qservSyncTimeoutSec);

    launch<ReplicateJob>(priority, _numReplicas);
    sync(_qservSyncTimeoutSec);

    bool const estimateOnly = false;
    launch<RebalanceJob>(priority, estimateOnly);
    sync(_qservSyncTimeoutSec);

    if (_purge) {
        launch<PurgeJob>(priority, _numReplicas);
        sync(_qservSyncTimeoutSec);
    }

    // Keep on getting calls on this method after a wait time
    return true;
}

ReplicationTask::ReplicationTask(Controller::Ptr const& controller,
                                 Task::AbnormalTerminationCallbackType const& onTerminated,
                                 unsigned int qservSyncTimeoutSec, unsigned int replicationIntervalSec,
                                 unsigned int numReplicas, bool purge)
        : Task(controller, "REPLICATION-THREAD  ", onTerminated, replicationIntervalSec),
          _qservSyncTimeoutSec(qservSyncTimeoutSec),
          _numReplicas(numReplicas),
          _purge(purge) {}

}  // namespace lsst::qserv::replica
