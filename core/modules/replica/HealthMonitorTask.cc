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
#include "replica/HealthMonitorTask.h"

// System headers
#include <map>

// Qserv headers
#include "replica/ClusterHealthJob.h"
#include "replica/Performance.h"

namespace lsst {
namespace qserv {
namespace replica {

HealthMonitorTask::Ptr HealthMonitorTask::create(
        Controller::Ptr const& controller,
        Task::AbnormalTerminationCallbackType const& onTerminated,
        WorkerEvictCallbackType const& onWorkerEvictTimeout,
        unsigned int workerEvictTimeoutSec,
        unsigned int workerResponseTimeoutSec,
        unsigned int healthProbeIntervalSec) {
    return Ptr(
        new HealthMonitorTask(
            controller,
            onTerminated,
            onWorkerEvictTimeout,
            workerEvictTimeoutSec,
            workerResponseTimeoutSec,
            healthProbeIntervalSec
        )
    );
}

HealthMonitorTask::WorkerResponseDelay HealthMonitorTask::workerResponseDelay() const {
    util::Lock lock(_mtx, "HealthMonitorTask::workerResponseDelay()");
    return _workerServiceNoResponseSec;
}

void HealthMonitorTask::onStart() {
 
    std::string const context = "HealthMonitorTask::onStart()";

    util::Lock lock(_mtx, context);

    for (auto&& worker: serviceProvider()->config()->allWorkers()) {
        _workerServiceNoResponseSec[worker]["qserv"] = 0;
        _workerServiceNoResponseSec[worker]["replication"] = 0;
    }
    _prevUpdateTimeMs = PerformanceUtils::now();
}


bool HealthMonitorTask::onRun() {
 
    std::string const context = "HealthMonitorTask::onRun()";

    std::string const parentJobId;  // no parent jobs

    // Probe hosts. Wait for completion or expiration of the job
    // before analyzing its findings.

    info("ClusterHealthJob");

    _numFinishedJobs = 0;

    auto self = shared_from_base<HealthMonitorTask>();

    std::vector<ClusterHealthJob::Ptr> jobs;
    jobs.emplace_back(
        ClusterHealthJob::create(
            _workerResponseTimeoutSec,
            true, /* allWorkers */
            controller(),
            parentJobId,
            [self](ClusterHealthJob::Ptr const& job) {
                self->_numFinishedJobs++;
            }
        )
    );
    jobs[0]->start();

    track<ClusterHealthJob>(ClusterHealthJob::typeName(), jobs, _numFinishedJobs);

    // Compute the actual delay which will also include the wait time since
    // the previous invocation of this method (onRun()).

    auto newUpdateTimeMs = PerformanceUtils::now();
    unsigned int workerResponseDelaySec = (newUpdateTimeMs - _prevUpdateTimeMs) / 1000;
    _prevUpdateTimeMs = newUpdateTimeMs;

    // Update non-response intervals for both services
    {
        util::Lock lock(_mtx, context);

        for (auto&& entry: jobs[0]->clusterHealth().qserv()) {

            auto worker = entry.first;
            auto responded = entry.second;

            if (responded) {
                _workerServiceNoResponseSec[worker]["qserv"] = 0;
            } else {
                _workerServiceNoResponseSec[worker]["qserv"] += workerResponseDelaySec;
                info("no response from Qserv at worker '" +worker + "' for " +
                     std::to_string(_workerServiceNoResponseSec[worker]["qserv"]) + " seconds");
            }
        }
        for (auto&& entry: jobs[0]->clusterHealth().replication()) {

            auto worker = entry.first;
            auto responded = entry.second;

            if (responded) {
                _workerServiceNoResponseSec[worker]["replication"] = 0;
            } else {
                _workerServiceNoResponseSec[worker]["replication"] += workerResponseDelaySec;
                info("no response from Replication at worker '" + worker + "' for " +
                     std::to_string(_workerServiceNoResponseSec[worker]["replication"]) + " seconds");
            }
        }
    }

    // Analyze the intervals to see which workers have reached the eviction
    // threshold.
    std::vector<std::string> workers2evict;

    // Also count the total number of the ENABLED Replication workers
    // (including the evicted ones) which are offline.
    size_t numEnabledWorkersOffline = 0;

    for (auto&& entry: _workerServiceNoResponseSec) {

        auto worker     = entry.first;
        auto workerInfo = serviceProvider()->config()->workerInfo(worker);

        // Both services on the worker must be offline for a duration of
        // the eviction interval before electing the worker for eviction.

        if (entry.second.at("replication") >= _workerEvictTimeoutSec) {
            if (entry.second.at("qserv") >= _workerEvictTimeoutSec) {

                // Only the ENABLED workers are considered for eviction

                if (workerInfo.isEnabled) {
                    workers2evict.push_back(worker);
                    info("worker '" + worker + "' has reached eviction timeout of " +
                         std::to_string(_workerEvictTimeoutSec) + " seconds");
                }
            }

            // Only count the ENABLED workers
            if (workerInfo.isEnabled) {
                numEnabledWorkersOffline++;
            }
        }
    }
    switch (workers2evict.size()) {

        case 0:

            break;

        case 1:

            // An important requirement for evicting a worker is that the Replication
            // services on the remaining ENABLED workers must be up and running.

            if (1 == numEnabledWorkersOffline) {

                // Upstream notification on the evicted worker
                _onWorkerEvictTimeout(workers2evict[0]);

                break;
            }

            // Otherwise, proceed down to the default scenario.

        default:

            // Any successful replication effort is not possible at this stage due
            // to one of the following reasons (among other possibilities):
            //
            //   1) multiple nodes failed simultaneously
            //   2) all services on the worker nodes are down (typically after site outage)
            //   3) network problems
            //
            // So, we just keep monitoring the status of the system. The problem (unless it's
            // cases 2 or 3) should require a manual repair.

            error("automated workers eviction is not possible if multiple workers " +
                  std::to_string(workers2evict.size()) + " are offline");

            break;
    }

    // Keep on getting calls on this method after a wait time
    return true;
}

HealthMonitorTask::HealthMonitorTask(
        Controller::Ptr const& controller,
        Task::AbnormalTerminationCallbackType const& onTerminated,
        WorkerEvictCallbackType const& onWorkerEvictTimeout,
        unsigned int workerEvictTimeoutSec,
        unsigned int workerResponseTimeoutSec,
        unsigned int healthProbeIntervalSec)
    :   Task(controller,
             "HEALTH-MONITOR  ",
             onTerminated,
             healthProbeIntervalSec
        ),
        _onWorkerEvictTimeout(onWorkerEvictTimeout),
        _workerEvictTimeoutSec(workerEvictTimeoutSec),
        _workerResponseTimeoutSec(workerResponseTimeoutSec),
        _numFinishedJobs(0) {
}

}}} // namespace lsst::qserv::replica
