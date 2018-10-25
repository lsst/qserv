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

/**
 * qserv-replica-master.cc is a latest version of a fixed-logic replication
 * Controller Replication Controller executing a sequence of jobs in an infinite loop.
 * The application is not meant to respond to any external communications (commands,
 * etc.). Also, it doesn't have a state (checkpoints)  which would allow resuming
 * unfinished tasks in case of restarts.
 */

// System headers
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/Application.h"
#include "replica/Configuration.h"
#include "replica/DeleteWorkerThread.h"
#include "replica/HealthMonitorThread.h"
#include "replica/ReplicationThread.h"
#include "util/BlockPost.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

std::string const description =
    "This application is the Master Replication Controller which has"
    " a built-in Cluster Health Monitor and a linear Replication loop."
    " The Monitor would track a status of both Qserv and Replication workers"
    " and trigger the worker exclusion sequence if both services were found"
    " non-responsive within a configured interval."
    " The interval is specified via the corresponidng command-line option."
    " And it also has some built-in default value."
    " Also, note that only a single node failure can trigger the worker"
    " exclusion sequence."
    " The controller has a fixed logic, and can't accept any external commands.";

std::string const logger =
    "lsst.qserv.replica.qserv-replica-master";

class MasterControllerApp
    :   public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MasterControllerApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of comand-line arguments
     *
     * @param description
     *   the description of the application to be printed when the command-line
     *   parser is invoked in the "help" mode.
     *
     * @param logger
     *   the name of a logger
     */
    static Ptr create(int argc,
                      const char* const argv[],
                      std::string const& description,
                      std::string const& logger) {
        return Ptr(
            new MasterControllerApp(
                argc,
                argv,
                description,
                logger
            )
        );
    }

    // Default constrution and copy semantics are prohibited

    MasterControllerApp() = delete;
    MasterControllerApp(MasterControllerApp const&) = delete;
    MasterControllerApp& operator=(MasterControllerApp const&) = delete;

    ~MasterControllerApp() override = default;

protected:

    /**
     * @see MasterControllerApp::create()
     */
    MasterControllerApp(int argc,
                        const char* const argv[],
                        std::string const& description,
                        std::string const& logger)
        :   Application(
                argc,
                argv,
                description,
                true /* injectDatabaseOptions */,
                true /* boostProtobufVersionCheck */,
                true /* enableServiceProvider */
            ),
            _healthProbeIntervalSec  (60),
            _replicationIntervalSec  (60),
            _workerResponseTimeoutSec(60),
            _workerEvictTimeoutSec   (3600),
            _qservSyncTimeoutSec     (60),
            _numReplicas             (0),
            _numIter                 (0),
            _purge          (false),
            _forceQservSync (false),
            _permanentDelete(false),
            _failed         (false),
            _log(LOG_GET(logger)) {

        // Configure the command line parser

        parser().option(
            "health-probe-interval",
            "interval (seconds) between running the health monitor",
            _healthProbeIntervalSec
        ).option(
            "replication-interval",
            "interval (seconds) between running the linear sequence of"
            " actions: check - fixup - replicate - rebalance",
            _replicationIntervalSec
        ).option(
            "worker-response-timeout",
            "maximum number of seconds to wait before giving up"
            " on worker probes when checking workers' statuses",
            _workerResponseTimeoutSec
        ).option(
            "worker-evict-timeout",
            "the maximum number of seconds to allow troubles workers to recover"
            " from the last catastrophic event before evicting them from a cluster",
            _workerEvictTimeoutSec
        ).option(
            "qserv-sync-timeout",
            "the maximum number of seconds to wait before Qserv workers respond"
            " to the synchronization requests before bailing out and proceeding"
            " to the next step in the normal replication sequence. A value which"
            " differs from 0 would override the corresponding parameter specified"
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
            " This number of provided will override the corresponding value found in"
            " in the Configuration.",
            _numReplicas
        ).option(
            "iter",
            "the number of iterations (a value of 0 means running indefinitively)",
            _numIter
        ).flag(
            "purge",
            "also run the purge algorithm in the end of each repliocation cycle in order"
            " to eliminate excess replicas which might get created by olgorithm ran earlier"
            " in the cycle",
            _purge
        ).flag(
            "permanent-worker-delete",
            "this flag would result in the permanent removal of the excluded workers"
            " workers from the configuration of the Replication system. Please, use"
            " this option with caution as it may result in loosing some Configuration"
            " data for the deleted workers",
            _permanentDelete
        );
    }

    /**
     * @see Application::runImpl()
     */
    int runImpl() final {

        LOGS(_log, LOG_LVL_INFO, "MASTER CONTROLLER  " << parser().serializeArguments());

        _controller = Controller::create(serviceProvider());

        // These threads should be running simultaneously

        auto self = shared_from_base<MasterControllerApp>();

        _replicationThread = ReplicationThread::create(
            _controller,
            [self] (ControlThread::Ptr const& ptr) {
                self->_failed = true;
            },
            _qservSyncTimeoutSec,
            _replicationIntervalSec,
            _numReplicas,
            _numIter,
            _purge
        );
        _replicationThread->start();

        _healthMonitorThread = HealthMonitorThread::create(
            _controller,
            [self] (ControlThread::Ptr const& ptr) {
                self->_failed = true;
            },
            [self] (std::string const& worker2evict) {
                self->_evict(worker2evict);
            },
            _workerEvictTimeoutSec,
            _workerResponseTimeoutSec,
            _healthProbeIntervalSec
        );
        _healthMonitorThread->start();

        // Keep running before a catastrophic failure is reported by any
        // above initiated activity

        util::BlockPost blockPost(1000, 2000);
        while (not _failed) {
            blockPost.wait();
        }

        // Stop all threads if any are still running

        _healthMonitorThread->stop();
        _replicationThread->stop();

        return 1;
    }

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
    void _evict(std::string const& worker) {

        // This thread needs to be stopped to avoid any interference with
        // the worker exclusion protocol.

        _replicationThread->stop();

        // This thread will be allowed to run for as long as it's permitted by
        // the corresponding timeouts set for Requests and Jobs in the Configuration,
        // or until a catastrophic failure occures within any control thread (including
        // this one).

        auto self = shared_from_base<MasterControllerApp>();

        _deleteWorkerThread = DeleteWorkerThread::create(
            _controller,
            [self] (ControlThread::Ptr const& ptr) {
                self->_failed = true;
            },
            worker,
            _permanentDelete
        );
        _deleteWorkerThread->startAndWait(
            [self] (ControlThread::Ptr const& ptr) -> bool {
                return self->_failed;
            }
        );
        _deleteWorkerThread->stop();    // it's safe to call this method even if the thread is
                                        // no longer running.

        // Resume the normal replication sequence unless a catastrophic failure
        // in the system has been detected
        
        if (not _failed) _replicationThread->start();
    }

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
    std::atomic<bool> _failed;

    /// The controller for launching oprations with the Replication system services
    Controller::Ptr _controller;

    // Control threads

    HealthMonitorThread::Ptr _healthMonitorThread;
    ReplicationThread::Ptr   _replicationThread;
    DeleteWorkerThread::Ptr  _deleteWorkerThread;

    /// Logger stream
    LOG_LOGGER _log;
};

} /// namespace

int main(int argc, const char* const argv[]) {
    try {
        auto app = ::MasterControllerApp::create(
            argc,
            argv,
            description,
            logger
        );
        return app->run();
    } catch (std::exception const& ex) {
        std::cerr << "main()  the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
