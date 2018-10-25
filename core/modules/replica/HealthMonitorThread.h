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
#ifndef LSST_QSERV_HEALTHMONITORTHREAD_H
#define LSST_QSERV_HEALTHMONITORTHREAD_H

// System headers
#include <functional>
#include <set>

// Qserv headers
#include "replica/ControlThread.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HealthMonitorThread represents a thread which monitors a status of
 * the Replication and Qserv worker services and report worker(s) eligible
 * for eviction if the're not responding within the specified timeout.
 */
class HealthMonitorThread
    :   public ControlThread {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<HealthMonitorThread> Ptr;

    /// The function type for notifications on the worker eviction events
    typedef std::function<void(std::string const&)> WorkerEvictCallbackType;

    // Default construction and copy semantics are prohibited

    HealthMonitorThread() = delete;
    HealthMonitorThread(HealthMonitorThread const&) = delete;
    HealthMonitorThread& operator=(HealthMonitorThread const&) = delete;

    ~HealthMonitorThread() final = default;

    /**
     * Create a new thread with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller
     *   a reference to the Controller for launching requests, jobs, etc.
     *
     * @param onTerminated
     *   callback function to be called upon abnormal termination
     *   of the thread. Set it to 'nullptr' if no call back should be made.
     *
     * @param onWorkerEvictTimeout
     *   callback function to be called when one or more workers
     *   are continiously not responding during the specifid period of
     *   time (parameter 'workerEvictTimeoutSec'). A candidate worker becomes
     *   eligible for eviction from the cluster if both Replication and Qserv
     *   worker services are both not respondig.
     *
     * @param workerEvictTimeoutSec
     *   the maximum number of seconds a couple of Replication and Qserv services
     *   run on the same worker node are allowed not to respond before evicting
     *   that worker from the cluster.
     *
     * @param workerResponseTimeoutSec
     *   the number of seconds to wait before a response when probing a remote
     *   worker service (Replication or Qserv). The timeout is needed for contionious
     *   monitoring of all workers even if one (or many of those) are not
     *   responding.
     *
     * @param healthProbeIntervalSec
     *   the number of seconds to wait between iterations of the inner monitoring
     *   loop. This parametr determines a frequency of probes sent to the worker
     *   services.
     *
     * @return
     *   the smart pointer to a new object
     */
    static Ptr create(Controller::Ptr const& controller,
                      ControlThread::CallbackType const& onTerminated,
                      WorkerEvictCallbackType const& onWorkerEvictTimeout,
                      unsigned int workerEvictTimeoutSec,
                      unsigned int workerResponseTimeoutSec,
                      unsigned int healthProbeIntervalSec);

protected:

    /**
     * @see ControlThread::run()
     */
    void run() final;

private:

    /**
     * The constructor is available to the class's factory method
     *
     * @see HealthMonitorThread::create()
     */
    HealthMonitorThread(Controller::Ptr const& controller,
                        CallbackType const& onTerminated,
                        WorkerEvictCallbackType const& onWorkerEvictTimeout,
                        unsigned int workerEvictTimeoutSec,
                        unsigned int workerResponseTimeoutSec,
                        unsigned int healthProbeIntervalSec);

private:

    /// The callback to be called when one or more workers become eligible
    /// for being evicted from the cluster.
    WorkerEvictCallbackType const _onWorkerEvictTimeout;

    /// The maximum number of seconds a couple of Replication and Qserv services
    /// run on the same worker node are allowed not to respond before evicting
    /// that worker from the cluster
    unsigned int _workerEvictTimeoutSec;

    /// The maximum number of seconds to be waited before giving up
    /// on the worker probe requests (applies to operations with both
    /// Replication and Qserv workers).
    unsigned int _workerResponseTimeoutSec;

    /// The number of seconds to wait in the end of each iteration loop before
    /// to begin the new one.
    unsigned int _healthProbeIntervalSec;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HEALTHMONITORTHREAD_H
