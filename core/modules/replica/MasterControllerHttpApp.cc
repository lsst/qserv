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
#include "replica/MasterControllerHttpApp.h"

// Qserv headers
#include "util/BlockPost.h"

namespace {

/**
 * This structure encapsulates default values for the Master Controller.
 * These values may be overridden by specifying the corresponding command
 * line options. See the constructor of the application class for further details.
 */
struct {
    unsigned int const healthProbeIntervalSec{60};
    unsigned int const replicationIntervalSec{60};
    unsigned int const workerResponseTimeoutSec{60};
    unsigned int const workerEvictTimeoutSec{3600};
    unsigned int const qservSyncTimeoutSec{60};
    unsigned int const numReplicas{0};

    bool const purge{false};
    bool const forceQservSync{false};
    bool const permanentDelete{false};

} const defaultOptions;

std::string const description {
    "This application is the Master Replication Controller which has"
    " a built-in Cluster Health Monitor and a linear Replication loop."
    " The Monitor would track a status of both Qserv and Replication workers"
    " and trigger the worker exclusion sequence if both services were found"
    " non-responsive within a configured interval."
    " The interval is specified via the corresponding command-line option."
    " And it also has some built-in default value."
    " Also, note that only a single node failure can trigger the worker"
    " exclusion sequence."
    " The controller has the built-in REST API which accepts external commands"
    " or request for information."
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

MasterControllerHttpApp::Ptr MasterControllerHttpApp::create(int argc,
                                                             const char* const argv[]) {
    return Ptr(
        new MasterControllerHttpApp(
            argc,
            argv
        )
    );
}


MasterControllerHttpApp::MasterControllerHttpApp(int argc,
                                                 const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true /* injectDatabaseOptions */,
            true /* boostProtobufVersionCheck */,
            true /* enableServiceProvider */
        ),
        _healthProbeIntervalSec  (::defaultOptions.healthProbeIntervalSec),
        _replicationIntervalSec  (::defaultOptions.replicationIntervalSec),
        _workerResponseTimeoutSec(::defaultOptions.workerResponseTimeoutSec),
        _workerEvictTimeoutSec   (::defaultOptions.workerEvictTimeoutSec),
        _qservSyncTimeoutSec     (::defaultOptions.qservSyncTimeoutSec),
        _numReplicas             (::defaultOptions.numReplicas),
        _purge                   (::defaultOptions.purge),
        _forceQservSync          (::defaultOptions.forceQservSync),
        _permanentDelete         (::defaultOptions.permanentDelete),
        _log(LOG_GET("lsst.qserv.replica.MasterControllerHttpApp")) {

    // Configure the command line parser

    parser().option(
        "health-probe-interval",
        "interval (seconds) between iterations of the health monitoring probes",
        _healthProbeIntervalSec
    ).option(
        "replication-interval",
        "interval (seconds) between running the linear sequence of"
        " actions: check - fix-up - replicate - re-balance",
        _replicationIntervalSec
    ).option(
        "worker-response-timeout",
        "maximum number of seconds to wait before giving up"
        " on worker probes when checking for workers",
        _workerResponseTimeoutSec
    ).option(
        "worker-evict-timeout",
        "the maximum number of seconds to allow troubled workers to recover"
        " from the last catastrophic event before evicting them from a cluster",
        _workerEvictTimeoutSec
    ).option(
        "qserv-sync-timeout",
        "the maximum number of seconds to wait before Qserv workers respond"
        " to the synchronization requests before bailing out and proceeding"
        " to the next step in the normal replication sequence. A value which"
        " differs from " + std::to_string(defaultOptions.qservSyncTimeoutSec) +
        " would override the corresponding parameter specified"
        " in the Configuration.",
        _qservSyncTimeoutSec
    ).flag(
        "qserv-sync-force",
        "the flag which would force Qserv workers to update their list of replicas"
        " even if some of the chunk replicas were still in use by on-going queries."
        " This affect replicas to be deleted from the workers during the synchronization"
        " stages",
        _forceQservSync
    ).option(
        "replicas",
        "the minimal number of replicas when running the replication phase"
        " This number if provided and if it's not " + std::to_string(defaultOptions.numReplicas) +
        " will override the corresponding value found"
        " in the Configuration.",
        _numReplicas
    ).flag(
        "purge",
        "The binary flag which, if provided, enables the 'purge' algorithm in"
        " the end of each replication cycle that eliminates excess replicas which"
        " may have been created by algorithms ran earlier in the cycle.",
        _purge
    ).flag(
        "permanent-worker-delete",
        "The flag would trigger the permanent removal of the evicted workers"
        " from the configuration of the Replication system. Please, use"
        " this option with caution as it will result in losing all records"
        " associated with the deleted workers",
        _permanentDelete
    );
}


int MasterControllerHttpApp::runImpl() {

    LOGS(_log, LOG_LVL_INFO, "MASTER CONTROLLER  " << parser().serializeArguments());

    _controller = Controller::create(serviceProvider());

    // These tasks should be running in parallel

    auto self = shared_from_base<MasterControllerHttpApp>();

    _replicationTask = ReplicationTask::create(
        _controller,
        [self] (Task::Ptr const& ptr) {
            self->_isFailed.fail();
        },
        _qservSyncTimeoutSec,
        _replicationIntervalSec,
        _numReplicas,
        _purge
    );
    _replicationTask->start();

    _healthMonitorTask = HealthMonitorTask::create(
        _controller,
        [self] (Task::Ptr const& ptr) {
            self->_isFailed.fail();
        },
        [self] (std::string const& worker2evict) {
            self->_evict(worker2evict);
        },
        _workerEvictTimeoutSec,
        _workerResponseTimeoutSec,
        _healthProbeIntervalSec
    );
    _healthMonitorTask->start();

    _httpProcessor = HttpProcessor::create(
        _controller,
        [self] (std::string const& worker2evict) {
            self->_evict(worker2evict);
        },
        _healthMonitorTask,
        _replicationTask,
        _deleteWorkerTask
    );

    // Keep running before a catastrophic failure is reported by any
    // above initiated activity

    util::BlockPost blockPost(1000, 2000);
    while (not _isFailed()) {
        blockPost.wait();
    }

    // Stop all threads if any are still running

    _healthMonitorTask->stop();
    _replicationTask->stop();

    if ((_replicationTask != nullptr) and
         _replicationTask->isRunning()) _replicationTask->stop();

    return 1;
}


void MasterControllerHttpApp::_evict(std::string const& worker) {

    // This thread needs to be stopped to avoid any interference with
    // the worker exclusion protocol.

    _replicationTask->stop();

    // This thread will be allowed to run for as long as it's permitted by
    // the corresponding timeouts set for Requests and Jobs in the Configuration,
    // or until a catastrophic failure occurs within any control thread (including
    // this one).

    auto self = shared_from_base<MasterControllerHttpApp>();

    _deleteWorkerTask = DeleteWorkerTask::create(
        _controller,
        [self] (Task::Ptr const& ptr) {
            self->_isFailed.fail();
        },
        worker,
        _permanentDelete
    );
    _deleteWorkerTask->startAndWait(
        [self] (Task::Ptr const& ptr) -> bool {
            return self->_isFailed();
        }
    );
    _deleteWorkerTask->stop();      // it's safe to call this method even if the thread is
                                    // no longer running.

    _deleteWorkerTask = nullptr;    // the object is no longer needed because it was
                                    // created for a specific worker.

    // Resume the normal replication sequence unless a catastrophic failure
    // in the system has been detected

    if (not _isFailed()) _replicationTask->start();
}


}}} // namespace lsst::qserv::replica
