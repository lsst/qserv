/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/ReplicationThread.h"

// Qserv headers
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/ReplicateJob.h"
#include "replica/RebalanceJob.h"
#include "replica/PurgeJob.h"
#include "util/BlockPost.h"

namespace lsst {
namespace qserv {
namespace replica {

ReplicationThread::Ptr ReplicationThread::create(Controller::Ptr const& controller,
                                                 ControlThread::CallbackType const& onTerminated,
                                                 unsigned int qservSyncTimeoutSec,
                                                 unsigned int replicationIntervalSec,
                                                 unsigned int numReplicas,
                                                 unsigned int numIter,
                                                 bool purge) {
    return Ptr(
        new ReplicationThread(
            controller,
            onTerminated,
            qservSyncTimeoutSec,
            replicationIntervalSec,
            numReplicas,
            numIter,
            purge
        )
    );
}

void ReplicationThread::run() {

    unsigned int numIterCompleted = 0;

    while (not stopRequested()) {

        bool const saveReplicaInfo = true;

        launch<FindAllJob>("FindAllJob", saveReplicaInfo);
        sync(_qservSyncTimeoutSec);

        launch<FixUpJob>("FixUpJob");
        sync(_qservSyncTimeoutSec);

        launch<ReplicateJob>("ReplicateJob", _numReplicas);
        sync(_qservSyncTimeoutSec);

        bool const estimateOnly = false;
        launch<RebalanceJob>("RebalanceJob", estimateOnly);
        sync(_qservSyncTimeoutSec);

        if (_purge) {
            launch<PurgeJob>("PurgeJob", _numReplicas);
            sync(_qservSyncTimeoutSec);
        }

        // Wait before going for another iteration

        util::BlockPost blockPost(1000 * _replicationIntervalSec,
                                  1000 * _replicationIntervalSec + 1);
        blockPost.wait();

        // Stop the application if running in the iteration restricted mode
        // and a desired number of iterations has been reached.

        ++numIterCompleted;
        if (0 != _numIter) {
            if (numIterCompleted >= _numIter) {
                info("desired number of iterations has been reached");
                break;
            }
        }
    }
}

ReplicationThread::ReplicationThread(Controller::Ptr const& controller,
                                     CallbackType const& onTerminated,
                                     unsigned int qservSyncTimeoutSec,
                                     unsigned int replicationIntervalSec,
                                     unsigned int numReplicas,
                                     unsigned int numIter,
                                     bool purge)
    :   ControlThread(controller,
                      "REPLICATION-THREAD  ",
                      onTerminated),
        _qservSyncTimeoutSec(qservSyncTimeoutSec),
        _replicationIntervalSec(replicationIntervalSec),
        _numReplicas(numReplicas),
        _numIter(numIter),
        _purge(purge) {
}

}}} // namespace lsst::qserv::replica
