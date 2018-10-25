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
 * qserv-replica-master-v1.cc is an original version of a fixed-logic
 * replication Controller executing a sequence of jobs in an infinite loop.
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
#include "replica/ClusterHealthJob.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DeleteWorkerJob.h"
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/PurgeJob.h"
#include "replica/RebalanceJob.h"
#include "replica/ReplicateJob.h"
#include "replica/QservSyncJob.h"
#include "util/BlockPost.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

class MasterControllerApp
    :   public Application {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<MasterControllerApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very bqse inheritance from 'enable_shared_from_this'.
     *
     * @param argc
     *   the number of command-line arguments
     *
     * @param argv
     *   the vector of comand-line arguments
     */
    static Ptr create(int argc,
                      const char* const argv[]) {
        return Ptr(new MasterControllerApp(argc, argv));
    }

    // Default constrution and copy semantics are prohibited

    MasterControllerApp() = delete;
    MasterControllerApp(MasterControllerApp const&) = delete;
    MasterControllerApp& operator=(MasterControllerApp const&) = delete;

    ~MasterControllerApp() override = default;

protected:

    /**
     * Implement a user-defined sequence of actions here.
     *
     * @return completion status
     */
    int runImpl() final {

        LOGS(_log, LOG_LVL_INFO, "MASTER            " << parser().serializeArguments());

        _controller = Controller::create(serviceProvider());

        // Start both activities in separate threads
        
        startReplicationSequence();
        startHealthMonitor();
        
        // Keep running before a catastrophic failure is reported by any
        // above initiated activity

        util::BlockPost blockPost(1000, 2000);
        while (not _failed) {
            blockPost.wait();
        }        
        return 0;
    }

private:

    /**
     * @see MasterControllerApp::create()
     */
    MasterControllerApp(int argc,
                        const char* const argv[])
        :   Application(
                argc,
                argv,
                "This application is the Master Replication Controller which has"
                " a built-in Cluster Health Monitor and a linear Replication loop."
                " The Monitor would track a status of both Qserv and Replication workers"
                " and trigger the worker exclusion sequence if both services were found"
                " non-responsive within a configured interval."
                " The interval is specified via the corresponidng command-line option."
                " And it also has some built-in default value."
                " Also, note that only a single node failure can trigger the worker"
                " exclusion sequence."
                " The controller has a fixed logic, and can't accept any external commands.",
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
            _failed         (false),
            _stopReplication(false),
            _forceQservSync (false),
            _permanentDelete(false),
            _healthMonitorContext  ("HEALTH-MONITOR    "),
            _replicationLoopContext("REPLICATION-LOOP  "),
            _log(LOG_GET("lsst.qserv.replica.qserv-replica-master")){

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
        );
    }

    /**
     * Run the normal sequence of jobs in a  detached
     * thread until a catastrophic failure happens or an external flag telling
     * the thread to abort its activities and cancel on-going jobs is set.
     */
    void startReplicationSequence() {
    
        LOGS(_log, LOG_LVL_INFO, _replicationLoopContext << "start");

        std::thread replicationThread([&]() {
            replicationSequence();
        });
        replicationThread.detach();
    }
    
    void replicationSequence() {

        try {
            
            // ---------------------------------------------------------------------
            // Start the normal loop asynchronously for all known database families.
            // Each wave of jobs is followed by the synchronization stage to ensure
            // Qserv stays in sync with the Replicaiton system.
            // --------------------------------------------------------------
    
            unsigned int _numIterCompleted = 0;

            while (not (_stopReplication or _failed)) {

                bool const saveReplicaInfo = true;

                if (launch<FindAllJob>("FindAllJob", saveReplicaInfo)) break;
                if (sync()) break;

                if (launch<FixUpJob>("FixUpJob")) break;
                if (sync()) break;

                if (launch<ReplicateJob>("ReplicateJob", _numReplicas)) break;
                if (sync()) break;

                bool const estimateOnly = false;
                if (launch<RebalanceJob>("RebalanceJob", estimateOnly)) break;
                if (sync()) break;

                if (_purge) {
                    if (launch<PurgeJob>("PurgeJob", _numReplicas)) break;
                    if (sync()) break;
                }

                // Wait before going for another iteration

                util::BlockPost blockPost(1000 * _replicationIntervalSec,
                                          1000 * _replicationIntervalSec + 1);
                blockPost.wait();

                // Stop the application if running in the iteration restricted mode
                // and a desired number of iterations has been reached.
                ++_numIterCompleted;
                if (0 != _numIter) {
                    if (_numIterCompleted >= _numIter) {
                        LOGS(_log, LOG_LVL_INFO, _replicationLoopContext
                             << "desired number of iterations has been reached");
                        _failed = true;
                    }
                }
            }

        } catch (std::exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, _replicationLoopContext << "exception: " << ex.what());
            _failed = true;
        }
        
        // Reset this flag to let the Health Monitoring thread know that
        // this thread has finished.

        _stopReplication = false;
    }

    /**
     * Launch and track a job of the specified type. Note that parameters
     * of the job are passed as variadic arguments to the method.
     *
     * @param jobName - the name of a job
     * @param Fargs   - job-specific variadic parameters
     * 
     * @return 'true' if the job cancelation sequence was initiated
     */
    template <class T, typename...Targs>
    bool launch(std::string const& jobName,
                Targs... Fargs) {

        LOGS(_log, LOG_LVL_INFO, _replicationLoopContext << jobName);

        // ---------------
        // Launch the jobs

        std::vector<Job::Ptr> jobs;
        std::atomic<size_t> numFinishedJobs{0};

        for (auto&& family: serviceProvider()->config()->databaseFamilies()) {
            auto job = T::create(
                family,
                Fargs...,
                _controller,
                _parentJobId,
                [&numFinishedJobs](typename T::Ptr const& job) {
                    ++numFinishedJobs;
                }
            );
            job->start();
            jobs.push_back(job);
        }

        // --------------------------------------------------------------
        // Track the completion of all jobs. Also monitor the termination
        // conditions.

        LOGS(_log, LOG_LVL_INFO, _replicationLoopContext << jobName << ": tracking started");

        util::BlockPost blockPost(1000, 1001);  // ~1 second wait time between iterations
    
        while (numFinishedJobs != jobs.size()) {
            if (_stopReplication or _failed) {
                for (auto&& job: jobs) {
                    job->cancel();
                }
                LOGS(_log, LOG_LVL_INFO, _replicationLoopContext << jobName << ": tracking aborted");
                return true;
            }
            blockPost.wait();
        }
        LOGS(_log, LOG_LVL_INFO, _replicationLoopContext << jobName << ": tracking finished");
        return false;
    }

    /**
     * Launch Qserv synchronization jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool sync() {
        return launch<QservSyncJob>("QservSyncJob",
                                    _qservSyncTimeoutSec,
                                    _forceQservSync);
    }

    /**
     * Run the cluster Health Monitoing sequence of actions in a detached
     * thread until a catastrophic failure happens or an external flag telling
     * the the thread to abort its activities and cancel on-going jobs is set.
     *
     * Single worker eviction starts when:
     *
     * - both Qserv and Replication services are not responding
     * within the specificed eviction interval on a worker to be marked for
     * eviction
     *
     * - and the remaining (excluding the evicted workers) Replication workers
     * must be available
     *
     * Otherwise, the Health Monitoring thread will keep tracking status
     * of both services on the worker nodes. In the mean time, the Replication
     * thread will keep trying its best effort in populating a cluster with
     * replicas (even if some replication worker agents were down).
     */
    void startHealthMonitor() {

        LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "start");

        std::thread healthMonitorThread([&]() {
            healthMonitoringSequence();
        });
        healthMonitorThread.detach();
    }

    void healthMonitoringSequence() {

        try {

            // Accumulate here non-response intervals for each workers until either will
            // reach the "eviction" threshold. Then trigger worker eviction sequence.

            std::map<std::string,           // worker
                     std::map<std::string,  // service
                              unsigned int>> workerServiceNoResponseSec;

            for (auto&& worker: _controller->serviceProvider()->config()->workers()) {
                workerServiceNoResponseSec[worker]["qserv"] = 0;
                workerServiceNoResponseSec[worker]["replication"] = 0;
            }
            while (not _failed) {

                // ---------------------------------------------------------
                // Probe hosts. Wait for completion or expiration of the job
                // before analyzing its findings.

                LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "ClusterHealthJob");

                std::atomic<bool> finished(false);
                auto const job = ClusterHealthJob::create(
                    _workerResponseTimeoutSec,
                    _controller,
                    _parentJobId,
                    [&finished](ClusterHealthJob::Ptr const& job) {
                        finished = true;
                    }
                );
                job->start();

                if (track(job, finished, "ClusterHealthJob")) return;
                
                // -----------------------------------------------
                // Update non-response intervals for both services

                for (auto&& entry: job->clusterHealth().qserv()) {

                    auto worker = entry.first;
                    auto responded = entry.second;

                    if (responded) {
                        workerServiceNoResponseSec[worker]["qserv"] = 0;
                    } else {
                        workerServiceNoResponseSec[worker]["qserv"] += _workerResponseTimeoutSec;
                        LOGS(_log, LOG_LVL_INFO, _healthMonitorContext
                             << "no response from Qserv at worker '" << worker << "' for "
                             << workerServiceNoResponseSec[worker]["qserv"] << " seconds");
                    }
                }
                for (auto&& entry: job->clusterHealth().replication()) {

                    auto worker = entry.first;
                    auto responded = entry.second;

                    if (responded) {
                        workerServiceNoResponseSec[worker]["replication"] = 0;
                    } else {
                        workerServiceNoResponseSec[worker]["replication"] += _workerResponseTimeoutSec;
                        LOGS(_log, LOG_LVL_INFO, _healthMonitorContext
                             << "no response from Replication at worker '" << worker << "' for "
                             << workerServiceNoResponseSec[worker]["replication"] << " seconds");
                    }
                }

                // ------------------------------------------------------------------------
                // Analyze the intervals to see which workers have reached the eviciton
                // threshold. Also count the total number of Replication workers (including
                // the evicted ones) which ae offline.

                std::vector<std::string> workers2evict;

                size_t numReplicationWorkersOffline = 0;

                for (auto&& entry: workerServiceNoResponseSec) {

                    auto worker = entry.first;

                    // Both services on the worker must be offline for a duration of
                    // the eviction interval before electing the worker for eviction.

                    if (entry.second.at("replication") >= _workerEvictTimeoutSec) {
                        if (entry.second.at("qserv") >= _workerEvictTimeoutSec) {
                            workers2evict.push_back(worker);
                            LOGS(_log, LOG_LVL_INFO, _healthMonitorContext
                                 << "worker '" << worker << "' has reached eviction timeout of "
                                 << _workerEvictTimeoutSec << " seconds");
                        }
                        numReplicationWorkersOffline++;
                    }
                }
                switch (workers2evict.size()) {

                    case 0:
                        
                        // --------------------------------------------------------------------
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
                        
                        // ----------------------------------------------------------------------
                        // An important requirement for evicting a worker is that the Replication
                        // services on the remaining workers must be up and running.

                        if (1 == numReplicationWorkersOffline) {

                            std::string const worker = workers2evict[0];

                            // Stop the Replication sequence and wait before it finishes or fails
                            // unless the cancellation is already in-progress.

                            if (_stopReplication.exchange(true)) {
                                throw std::logic_error(
                                            _healthMonitorContext +
                                            "the cancellation of the Replication thread is already in progress");
                            }

                            LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "Replication cancellation: tracking started");

                            util::BlockPost blockPost(1000, 2000);

                            while (not _stopReplication and not _failed) {
                                LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "Replication cancellation: tracking ...");
                                blockPost.wait();
                            }

                            // In case if the other thread failed then quit this
                            // thread as well
                            if (_failed) {
                                if (_stopReplication) _stopReplication = false;
                                LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "Replication cancellation: tracking aborted");
                                return;
                            }
                            LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "Replication cancellation: tracking finished");

                            // ----------------
                            // Evict the worker

                            LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << "DeleteWorkerJob");

                            std::atomic<bool> finished(false);

                            auto const job = DeleteWorkerJob::create(
                                worker,
                                _permanentDelete,
                                _controller,
                                _parentJobId,
                                [&finished](DeleteWorkerJob::Ptr const& job) {
                                    finished = true;
                                }
                            );
                            job->start();

                            if (track(job, finished, "DeleteWorkerJob")) return;

                            // -------------------------------------------------------------------------
                            // Reset worker-non-response intervals before restart the Replication thread
                            //
                            // ATTENTION: the map needs to be rebuild from scratch because one worker
                            // has been evicted from the Configuration.

                            workerServiceNoResponseSec.clear();

                            for (auto&& worker: _controller->serviceProvider()->config()->workers()) {
                                workerServiceNoResponseSec[worker]["qserv"] = 0;
                                workerServiceNoResponseSec[worker]["replication"] = 0;
                            }

                            startReplicationSequence();
                            break;
                        }
                        
                        // Otherwise, proceed down to the default scenario.

                    default:

                        // Any succesful replication effort is not possible at this stage due
                        // to one of the following reasons (among other possibilities):
                        //
                        //   1) multiple nodes failed simultaneously
                        //   2) all services on the worker nodes are down (typically after site outage)
                        //   3) network problems
                        //
                        // So, we just keep monitoring the status of the system. The problem (unless it's
                        // cases 2 or 3) should require a manual repair.

                        LOGS(_log, LOG_LVL_ERROR, _healthMonitorContext
                             << "automated workers eviction is not possible because too many workers "
                             << workers2evict.size() << " are offline");

                        break;
                }
            }

        } catch (std::exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, _healthMonitorContext << "exception: " << ex.what());
            _failed = true;
        }
    }

    /**
     * Track a job in a context of the Health Monitoring thread
     *
     * @param job      - pointer to the job to be tracked
     * @param finished - job completion flag to monitor
     * @param name     - name of the job
     *
     * @return 'true' if a catastrophic failure detected and the tracking had to abort
     */
    bool track(Job::Ptr const& job,
               std::atomic<bool> const& finished,
               std::string const& name) {

        LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << name << ": tracking started");

        util::BlockPost blockPost(1000, 2000);

        while (not finished and not _failed) {
            blockPost.wait();
        }
        if (_failed) {
            job->cancel();
            LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << name << ": tracking aborted");
            return true;
        }
        LOGS(_log, LOG_LVL_INFO, _healthMonitorContext << name << ": tracking finished");

        return false;
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

    /// The Controller for submititng requests and jobs
    Controller::Ptr _controller;

    /// The flag to indicate a catastrophic activity which must result
    /// in a termination of the application
    std::atomic<bool> _failed;
    
    /// This flag is raised by the health monitoring thread to notify
    /// the replication thread that it should wrap up its operation
    /// and quit
    std::atomic<bool> _stopReplication;

    /// no parent for any job initited by the application
    std::string const _parentJobId;

    /// Force Qserv synchronization if 'true'
    bool _forceQservSync;

    /// Permanently delete workers if set to 'true'
    bool _permanentDelete;

    // Context strings for threads

    std::string _healthMonitorContext;
    std::string _replicationLoopContext;

    /// Logger stream
    LOG_LOGGER _log;
};

} /// namespace

int main(int argc, const char* const argv[]) {
    try {
        auto app = ::MasterControllerApp::create(argc, argv);
        return app->run();
    } catch (std::exception const& ex) {
        std::cerr << "main()  the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
