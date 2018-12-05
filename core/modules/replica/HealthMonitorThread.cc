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
#include "replica/HealthMonitorThread.h"

// System headers
#include <map>

// Qserv headers
#include "replica/ClusterHealthJob.h"
#include "util/BlockPost.h"

namespace lsst {
namespace qserv {
namespace replica {

HealthMonitorThread::Ptr HealthMonitorThread::create(
        Controller::Ptr const& controller,
        ControlThread::AbnormalTerminationCallbackType const& onTerminated,
        WorkerEvictCallbackType const& onWorkerEvictTimeout,
        unsigned int workerEvictTimeoutSec,
        unsigned int workerResponseTimeoutSec,
        unsigned int healthProbeIntervalSec) {
    return Ptr(
        new HealthMonitorThread(
            controller,
            onTerminated,
            onWorkerEvictTimeout,
            workerEvictTimeoutSec,
            workerResponseTimeoutSec,
            healthProbeIntervalSec
        )
    );
}

HealthMonitorThread::WorkerResponseDelay HealthMonitorThread::workerResponseDelay() const {
    util::Lock lock(_mtx, "HealthMonitorThread::workerResponseDelay()");
    return _workerServiceNoResponseSec;
}

void HealthMonitorThread::run() {
 
    std::string const context = "HealthMonitorThread::run()";
    {
        util::Lock lock(_mtx, context);

        for (auto&& worker: serviceProvider()->config()->workers()) {
            _workerServiceNoResponseSec[worker]["qserv"] = 0;
            _workerServiceNoResponseSec[worker]["replication"] = 0;
        }
    }

    std::string const parentJobId;  // no parent jobs

    while (not stopRequested()) {

        // Probe hosts. Wait for completion or expiration of the job
        // before analyzing its findings.

        info("ClusterHealthJob");

        _numFinishedJobs = 0;

        auto self = shared_from_base<HealthMonitorThread>();

        std::vector<ClusterHealthJob::Ptr> jobs;
        jobs.emplace_back(
            ClusterHealthJob::create(
                _workerResponseTimeoutSec,
                controller(),
                parentJobId,
                [self](ClusterHealthJob::Ptr const& job) {
                    self->_numFinishedJobs++;
                }
            )
        );
        jobs[0]->start();

        track<ClusterHealthJob>(ClusterHealthJob::typeName(), jobs, _numFinishedJobs);
 
        // Update non-response intervals for both services
        {
            util::Lock lock(_mtx, context);

            for (auto&& entry: jobs[0]->clusterHealth().qserv()) {

                auto worker = entry.first;
                auto responded = entry.second;

                if (responded) {
                    _workerServiceNoResponseSec[worker]["qserv"] = 0;
                } else {
                    _workerServiceNoResponseSec[worker]["qserv"] += _workerResponseTimeoutSec;
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
                    _workerServiceNoResponseSec[worker]["replication"] += _workerResponseTimeoutSec;
                    info("no response from Replication at worker '" + worker + "' for " +
                         std::to_string(_workerServiceNoResponseSec[worker]["replication"]) + " seconds");
                }
            }
        }

        // Analyze the intervals to see which workers have reached the eviction
        // threshold. Also count the total number of Replication workers (including
        // the evicted ones) which ae offline.

        std::vector<std::string> workers2evict;

        size_t numReplicationWorkersOffline = 0;

        for (auto&& entry: _workerServiceNoResponseSec) {

            auto worker = entry.first;

            // Both services on the worker must be offline for a duration of
            // the eviction interval before electing the worker for eviction.

            if (entry.second.at("replication") >= _workerEvictTimeoutSec) {
                if (entry.second.at("qserv") >= _workerEvictTimeoutSec) {
                    workers2evict.push_back(worker);
                    info("worker '" + worker + "' has reached eviction timeout of " +
                         std::to_string(_workerEvictTimeoutSec) + " seconds");
                }
                numReplicationWorkersOffline++;
            }
        }
        switch (workers2evict.size()) {

            case 0:
                
                // Pause before going for another iteration only if all services on all
                // workers are up. Otherwise we would skew (extend) the "no-response"
                // intervals.

                if (0 == numReplicationWorkersOffline) {
                    util::BlockPost blockPost(1000 * _healthProbeIntervalSec,
                                              1000 * _healthProbeIntervalSec + 1);
                    blockPost.wait();
                }
                break;

            case 1:
                
                // An important requirement for evicting a worker is that the Replication
                // services on the remaining workers must be up and running.

                if (1 == numReplicationWorkersOffline) {

                    // Upstream notification on the evicted worker
                    _onWorkerEvictTimeout(workers2evict[0]);

                    // Reset worker-non-response intervals before resuming this thread
                    //
                    // ATTENTION: the map needs to be rebuild from scratch because one worker
                    // has been evicted from the Configuration.
                    {
                        util::Lock lock(_mtx, context);

                        _workerServiceNoResponseSec.clear();

                        for (auto&& worker: serviceProvider()->config()->workers()) {
                            _workerServiceNoResponseSec[worker]["qserv"] = 0;
                            _workerServiceNoResponseSec[worker]["replication"] = 0;
                        }
                    }
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
    }
}

HealthMonitorThread::HealthMonitorThread(Controller::Ptr const& controller,
                                         ControlThread::AbnormalTerminationCallbackType const& onTerminated,
                                         WorkerEvictCallbackType const& onWorkerEvictTimeout,
                                         unsigned int workerEvictTimeoutSec,
                                         unsigned int workerResponseTimeoutSec,
                                         unsigned int healthProbeIntervalSec)
    :   ControlThread(controller,
                      "HEALTH-MONITOR  ",
                      onTerminated),
        _onWorkerEvictTimeout(onWorkerEvictTimeout),
        _workerEvictTimeoutSec(workerEvictTimeoutSec),
        _workerResponseTimeoutSec(workerResponseTimeoutSec),
        _healthProbeIntervalSec(healthProbeIntervalSec),
        _numFinishedJobs(0) {
}

}}} // namespace lsst::qserv::replica
