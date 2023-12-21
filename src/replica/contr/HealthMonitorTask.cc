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
#include "replica/contr/HealthMonitorTask.h"

// System headers
#include <map>

// Qserv headers
#include "replica/services/DatabaseServices.h"
#include "util/TimeUtils.h"

using namespace std;

namespace lsst::qserv::replica {

HealthMonitorTask::Ptr HealthMonitorTask::create(Controller::Ptr const& controller,
                                                 Task::AbnormalTerminationCallbackType const& onTerminated,
                                                 WorkerEvictCallbackType const& onWorkerEvictTimeout,
                                                 unsigned int workerEvictTimeoutSec,
                                                 unsigned int workerResponseTimeoutSec,
                                                 unsigned int healthProbeIntervalSec) {
    return Ptr(new HealthMonitorTask(controller, onTerminated, onWorkerEvictTimeout, workerEvictTimeoutSec,
                                     workerResponseTimeoutSec, healthProbeIntervalSec));
}

HealthMonitorTask::WorkerResponseDelay HealthMonitorTask::workerResponseDelay() const {
    replica::Lock lock(_mtx, "HealthMonitorTask::" + string(__func__));
    return _workerServiceNoResponseSec;
}

void HealthMonitorTask::onStart() {
    string const context = "HealthMonitorTask::" + string(__func__);

    replica::Lock lock(_mtx, context);

    for (auto&& worker : serviceProvider()->config()->allWorkers()) {
        _workerServiceNoResponseSec[worker]["qserv"] = 0;
        _workerServiceNoResponseSec[worker]["replication"] = 0;
    }
    _prevUpdateTimeMs = util::TimeUtils::now();
}

bool HealthMonitorTask::onRun() {
    string const context = "HealthMonitorTask::" + string(__func__);

    // Probe hosts. Wait for completion or expiration of the job
    // before analyzing its findings.

    info("ClusterHealthJob");

    _numFinishedJobs = 0;

    auto self = shared_from_base<HealthMonitorTask>();
    string const noParentJobId;

    vector<ClusterHealthJob::Ptr> jobs;
    jobs.emplace_back(ClusterHealthJob::create(
            _workerResponseTimeoutSec, true, /* allWorkers */
            controller(), noParentJobId,
            [self](ClusterHealthJob::Ptr const& job) { self->_numFinishedJobs++; },
            serviceProvider()->config()->get<int>("controller", "health-monitor-priority-level")));
    jobs[0]->start();

    _logStartedEvent(jobs[0]);
    track<ClusterHealthJob>(ClusterHealthJob::typeName(), jobs, _numFinishedJobs);
    _logFinishedEvent(jobs[0]);

    // Compute the actual delay which will also include the wait time since
    // the previous invocation of this method (onRun()).

    auto newUpdateTimeMs = util::TimeUtils::now();
    unsigned int workerResponseDelaySec = (newUpdateTimeMs - _prevUpdateTimeMs) / 1000;
    _prevUpdateTimeMs = newUpdateTimeMs;

    // Update non-response intervals for both services
    {
        replica::Lock lock(_mtx, context);

        for (auto&& entry : jobs[0]->clusterHealth().qserv()) {
            auto workerName = entry.first;
            auto responded = entry.second;

            if (responded) {
                _workerServiceNoResponseSec[workerName]["qserv"] = 0;
            } else {
                _workerServiceNoResponseSec[workerName]["qserv"] += workerResponseDelaySec;
                info("no response from Qserv at worker '" + workerName + "' for " +
                     to_string(_workerServiceNoResponseSec[workerName]["qserv"]) + " seconds");
            }
        }
        for (auto&& entry : jobs[0]->clusterHealth().replication()) {
            auto workerName = entry.first;
            auto responded = entry.second;

            if (responded) {
                _workerServiceNoResponseSec[workerName]["replication"] = 0;
            } else {
                _workerServiceNoResponseSec[workerName]["replication"] += workerResponseDelaySec;
                info("no response from Replication at worker '" + workerName + "' for " +
                     to_string(_workerServiceNoResponseSec[workerName]["replication"]) + " seconds");
            }
        }
    }

    // Analyze the intervals to see which workers have reached the eviction
    // threshold.
    vector<string> workers2evict;

    // Also count the total number of the ENABLED Replication workers
    // (including the evicted ones) which are offline.
    size_t numEnabledWorkersOffline = 0;

    for (auto&& entry : _workerServiceNoResponseSec) {
        auto workerName = entry.first;
        auto worker = serviceProvider()->config()->worker(workerName);

        // Both services on the worker must be offline for a duration of
        // the eviction interval before electing the worker for eviction.

        if (entry.second.at("replication") >= _workerEvictTimeoutSec) {
            if (entry.second.at("qserv") >= _workerEvictTimeoutSec) {
                // Only the ENABLED workers are considered for eviction

                if (worker.isEnabled) {
                    workers2evict.push_back(workerName);
                    info("worker '" + workerName + "' has reached eviction timeout of " +
                         to_string(_workerEvictTimeoutSec) + " seconds");
                }
            }

            // Only count the ENABLED workers
            if (worker.isEnabled) {
                numEnabledWorkersOffline++;
            }
        }
    }

    // There are three requirements which both must be met before attempting
    // to evict workers:
    //
    //   a) exactly one worker is allowed to be evicted at a time
    //   b) the candidate worker must be still ENABLED in the system
    //   c) the Replication services on the remaining ENABLED workers must be up
    //      and running
    //
    // If any abnormalities will be detected in the system, and if the System
    // won't be able to handle them as per the above stated rules then the Monitor
    // will just complain and keep tracking changes in a status of the system.
    // The problem may require a manual repair.

    switch (workers2evict.size()) {
        case 0:
            break;

        case 1:
            if (1 == numEnabledWorkersOffline) {
                // Upstream notification on the evicted worker
                _onWorkerEvictTimeout(workers2evict[0]);

            } else {
                error("single worker eviction is not possible if other workers are offline: " +
                      to_string(numEnabledWorkersOffline));
            }
            break;

        default:
            error("simultaneous eviction of multiple workers is not supported: " +
                  to_string(workers2evict.size()));

            break;
    }

    // Keep on getting calls on this method after a wait time
    return true;
}

HealthMonitorTask::HealthMonitorTask(Controller::Ptr const& controller,
                                     Task::AbnormalTerminationCallbackType const& onTerminated,
                                     WorkerEvictCallbackType const& onWorkerEvictTimeout,
                                     unsigned int workerEvictTimeoutSec,
                                     unsigned int workerResponseTimeoutSec,
                                     unsigned int healthProbeIntervalSec)
        : Task(controller, "HEALTH-MONITOR  ", onTerminated, healthProbeIntervalSec),
          _onWorkerEvictTimeout(onWorkerEvictTimeout),
          _workerEvictTimeoutSec(workerEvictTimeoutSec),
          _workerResponseTimeoutSec(workerResponseTimeoutSec),
          _numFinishedJobs(0),
          _prevUpdateTimeMs(0) {}

void HealthMonitorTask::_logStartedEvent(ClusterHealthJob::Ptr const& job) const {
    ControllerEvent event;

    event.operation = job->typeName();
    event.status = "STARTED";
    event.jobId = job->id();

    event.kvInfo.emplace_back("worker-response-timeout", to_string(_workerResponseTimeoutSec));

    logEvent(event);
}

void HealthMonitorTask::_logFinishedEvent(ClusterHealthJob::Ptr const& job) const {
    ControllerEvent event;

    event.operation = job->typeName();
    event.status = job->state2string();
    event.jobId = job->id();
    event.kvInfo = job->persistentLogData();

    logEvent(event);
}

}  // namespace lsst::qserv::replica
