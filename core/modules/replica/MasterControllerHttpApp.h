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
#ifndef LSST_QSERV_MASTERCONTROLLERHTTPAPP_H
#define LSST_QSERV_MASTERCONTROLLERHTTPAPP_H

// System headers
#include <string>

// Qserv headers
#include "replica/Application.h"
#include "replica/Controller.h"
#include "replica/DeleteWorkerTask.h"
#include "replica/HealthMonitorTask.h"
#include "replica/HttpProcessor.h"
#include "replica/OneWayFailer.h"
#include "replica/ReplicationTask.h"

// LSST headers
#include "lsst/log/Log.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class MasterControllerHttpApp implements the Replication Controller which
 * allows interactions via the REST API. When it starts the controller launches
 * two tasks running in parallel (in dedicated threads): Linear Replication one and
 * the Health Monitoring one. These tasks can be suspended/resumed via the REST API.
 */
class MasterControllerHttpApp
    :   public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MasterControllerHttpApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of command-line arguments
     */
    static Ptr create(int argc,
                      const char* const argv[]);

    // Default construction and copy semantics are prohibited

    MasterControllerHttpApp() = delete;
    MasterControllerHttpApp(MasterControllerHttpApp const&) = delete;
    MasterControllerHttpApp& operator=(MasterControllerHttpApp const&) = delete;

    ~MasterControllerHttpApp() override = default;

protected:

    /**
     * @see MasterControllerHttpApp::create()
     */
    MasterControllerHttpApp(int argc,
                            const char* const argv[]);

    /**
     * @see Application::runImpl()
     */
    int runImpl() final;

private:

    /**
     * Evict the specified worker from the cluster
     *
     * NOTE: This method is called by the health-monitoring thread when
     * a condition for evicting the worker is detected. The calling thread
     * will be blocked for a duration of this call.
     *
     * @param worker
     *   the name of a worker to be evicted
     */
    void _evict(std::string const& worker);

private:

    // Command line parameters

    unsigned int _healthProbeIntervalSec;
    unsigned int _replicationIntervalSec;
    unsigned int _workerResponseTimeoutSec;
    unsigned int _workerEvictTimeoutSec;
    unsigned int _qservSyncTimeoutSec;
    unsigned int _numReplicas;
    unsigned int _numIter;

    bool _purge;
    bool _forceQservSync;
    bool _permanentDelete;

    /// This flag will be raised by any thread if a non-recoverable
    /// catastrophic failure will be detected.
    OneWayFailer _isFailed;

    /// The controller for launching operations with the Replication system services
    Controller::Ptr _controller;

    // Control threads

    HealthMonitorTask::Ptr _healthMonitorTask;
    ReplicationTask::Ptr   _replicationTask;
    DeleteWorkerTask::Ptr  _deleteWorkerTask;

    HttpProcessor::Ptr _httpProcessor;

    /// Logger stream
    LOG_LOGGER _log;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_MASTERCONTROLLERHTTPAPP_H */
