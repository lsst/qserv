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

/// qserv-replica-master.cc is a fixed-logic replication Controller executing
/// a sequence of jobs in an infinite loop. The application is not
/// meant to respond to any external communications (commands, etc.)

// System headers
#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/ClusterHealthJob.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DeleteWorkerJob.h"
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/PurgeJob.h"
#include "replica/RebalanceJob.h"
#include "replica/ReplicateJob.h"
#include "replica/ServiceProvider.h"
#include "replica/QservSyncJob.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.qserv-replica-master");

struct Application {

    // Command line parameters
    
    std::string configUrl;

    unsigned int healthProbeIntervalSec;
    unsigned int replicationIntervalSec;
    unsigned int workerResponseTimeoutSec;
    unsigned int workerEvictTimeoutSec;
    unsigned int qservSyncTimeoutSec;

    unsigned int numReplicas;

    unsigned int numIter;

    // Replication system context

    ServiceProvider::Ptr provider;
    Controller::Ptr controller;

    std::vector<std::string> databaseFamilies;

    /// The flag to indicate a catastrophic activity which must result
    /// in a termination of the application
    std::atomic<bool> failed;
    
    /// This flag is raised by the health monitoring thread to notify
    /// the replication thread that it should wrap up its operation
    /// and quit
    std::atomic<bool> stopReplication;

    /// A collection of jobs launched at each stage
    std::vector<Job::Ptr> jobs;

    /// A number of jobs which have which finished
    std::atomic<size_t> numFinishedJobs;

    /// no parent for any job initited by the application
    std::string const parentJobId;

    /// Force Qserv synchronization if 'true'
    bool forceQservSync;

    /// Permanently delete workers if set to 'true'
    bool permanentDelete;

    // Context strings for threads

    std::string healthMonitorContext;
    std::string replicationLoopContext;

    /**
     * Construct the application and begin its execution
     */
    Application(int argc, const char* const argv[])
        :   configUrl               ("file:replication.cfg"),
            healthProbeIntervalSec  (60),
            replicationIntervalSec  (60),
            workerResponseTimeoutSec(60),
            workerEvictTimeoutSec   (3600),
            qservSyncTimeoutSec     (60),
            numReplicas             (0),
            numIter                 (0),
            failed                  (false),
            stopReplication         (false),
            numFinishedJobs         (0),
            forceQservSync          (false),
            permanentDelete         (false),
            healthMonitorContext    ("HEALTH-MONITOR    "),
            replicationLoopContext  ("REPLICATION-LOOP  ") {

        // Parse command line parameters

        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  [--config=<url>]\n"
            "  [--health-probe-interval=<seconds>]\n"
            "  [--replication-interval=<seconds>]\n"
            "  [--worker-response-timeout=<seconds>]\n"
            "  [--worker-evict-timeout=<seconds>]\n"
            "  [--qserv-sync-timeout=<seconds>]\n"
            "  [--replicas=<number>]\n"
            "  [--iter=<num>]\n"
            "\n"
            "Flags and options:\n"
            "  --config                   - configuration URL (a file or a database connection string)\n"
            "                               [ DEFAULT: " + configUrl + " ]\n"
            "\n"
            "  --health-probe-interval    - interval (seconds) between running the health monitor\n"
            "                               [ DEFAULT: " + std::to_string(healthProbeIntervalSec) + " seconds ]\n"
            "\n"
            "\n"
            "  --replication-interval     - interval (seconds) between running the normal sequence of\n"
            "                               actions: check - fixup - replicate - rebalance\n"
            "                               [ DEFAULT: " + std::to_string(replicationIntervalSec) + " seconds ]\n"
            "\n"
            "  --worker-response-timeout  - maximum number of seconds to wait before giving up\n"
            "                               on worker probes when checking workers' statuses\n"
            "                               [ DEFAULT: " + std::to_string(workerResponseTimeoutSec) + " seconds ]\n"
           "\n"
            "  --worker-evict-timeout     - maximum number of seconds to allow troubles workers to recover\n"
            "                               from the last catastrophic event before evicting them from a cluster\n"
            "                               [ DEFAULT: " + std::to_string(workerEvictTimeoutSec) + " seconds ]\n"
            "\n"
            "  --qserv-sync-timeout       - maximum number of seconds to wait before Qserv workers respond\n"
            "                               to the synchronization requests before bailing out and proceeding\n"
            "                               to the next step in the normal replication sequence. A value which\n"
            "                               differs from 0 would override the corresponding parameter specified\n"
            "                               in the Configuration.\n"
            "                               [ DEFAULT: " + std::to_string(qservSyncTimeoutSec) + " seconds ]\n"
            "\n"
            "  --replicas                 - minimal number of replicas when running the replication phase\n"
            "                               This number of provided will override the corresponding value found in\n"
            "                               in the Configuration.\n"
            "                               [ DEFAULT: " + std::to_string(numReplicas) + " replicas of each chunk ]\n"
            "\n"
            "  --iter                     - the number of iterations\n"
            "                               [ DEFAULT: " + std::to_string(numIter) + " ]\n");

        configUrl = parser.option<std::string>("config", configUrl);

        healthProbeIntervalSec   = parser.option<unsigned int>("health-probe-interval",   healthProbeIntervalSec);
        replicationIntervalSec   = parser.option<unsigned int>("replication-interval",    replicationIntervalSec);
        workerResponseTimeoutSec = parser.option<unsigned int>("worker-response-timeout", workerResponseTimeoutSec);
        workerEvictTimeoutSec    = parser.option<unsigned int>("worker-evict-timeout",    workerEvictTimeoutSec);
        qservSyncTimeoutSec      = parser.option<unsigned int>("qserv-sync-timeout",      qservSyncTimeoutSec);
        numReplicas              = parser.option<unsigned int>("replicas",                numReplicas);

        numIter = parser.option<unsigned int>("iter", numIter);

        LOGS(_log, LOG_LVL_INFO, "MASTER            "
             << "configUrl="               << configUrl << " "
             << "health-probe-interval="   << healthProbeIntervalSec << " "
             << "replication-interval="    << replicationIntervalSec << " "
             << "worker-response-timeout=" << workerResponseTimeoutSec << " "
             << "worker-evict-timeout="    << workerEvictTimeoutSec << " "
             << "qserv-sync-timeout="      << qservSyncTimeoutSec << " "
             << "replicas="                << numReplicas << " "
             << "iter="                    << numIter);

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        provider   = ServiceProvider::create(configUrl);
        controller = Controller::create(provider);

        controller->run();

        databaseFamilies = provider->config()->databaseFamilies();

        // Start both activities in separate threads
        
        startReplicationSequence();
        startHealthMonitor();
        
        // Keep running before a catastrophic failure is reported by any
        // above initiated activity

        util::BlockPost blockPost(1000, 2000);
        while (not failed) {
            blockPost.wait();
        }

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();
    }

    /**
     * Run the normal sequence of jobs in a  detached
     * thread until a catastrophic failure happens or an external flag telling
     * the the thread to abort its activities and cancel on-going jobs is set.
     */
    void startReplicationSequence() {
    
        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "start");

        std::thread replicationThread([this] () {

            try {
                
                // ---------------------------------------------------------------------
                // Start the normal loop asynchronously for all known database families.
                // Each wave of jobs is followed by the synchronization stage to ensure
                // Qserv stays in sync with the Replicaiton system.
                // --------------------------------------------------------------
        
                unsigned int numIterCompleted = 0;

                while (not stopReplication and not failed) {
            
                    if (launchFindAllJobs()) break;
                    if (launchSyncJobs()) break;
    
                    if (launchFixUpAllJobs()) break;
                    if (launchSyncJobs()) break;
    
                    if (launchReplicateJobs()) break;
                    if (launchSyncJobs()) break;
    
                    if (launchRebalanceJobs()) break;
                    if (launchSyncJobs()) break;
    
                    // Wait before going for another iteration
    
                    util::BlockPost blockPost(1000 * replicationIntervalSec,
                                              1000 * replicationIntervalSec + 1);
                    blockPost.wait();

                    // Stop the application if running in the iteration restricted mode
                    // and a desired number of iterations has been reached.
                    ++numIterCompleted;
                    if (0 != numIter) {
                        if (numIterCompleted >= numIter) {
                            LOGS(_log, LOG_LVL_INFO, replicationLoopContext
                                 << "desired number of iterations has been reached");
                            failed = true;
                        }
                    }
                }
    
            } catch (std::exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, replicationLoopContext << "exception: " << ex.what());
                failed = true;
            }
            
            // Reset this flag to let the Health Monitoring thread know that
            // this thread has finished.

            stopReplication = false;
        });
        replicationThread.detach();
    }

    /**
     * Launch and track the chunk info harvest jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool launchFindAllJobs() {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "FindAllJobs");

        jobs.clear();
        numFinishedJobs = 0;

        bool const saveReplicaInfo = true;

        for (auto&& family: databaseFamilies) {
            auto job = FindAllJob::create(
                family,
                saveReplicaInfo,
                controller,
                parentJobId,
                [this](FindAllJob::Ptr const& job) {
                    ++(this->numFinishedJobs);
                }
            );
            job->start();
            jobs.push_back(job);
        }
        return trackJobs( "FindAllJobs");
    }

    /**
     * Launch collocation problems fixup jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool launchFixUpAllJobs() {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "FixUpJob");

        jobs.clear();
        numFinishedJobs = 0;

        for (auto&& family: databaseFamilies) {
            auto job = FixUpJob::create(
                family,
                controller,
                parentJobId,
                [this](FixUpJob::Ptr const& job) {
                    ++(this->numFinishedJobs);
                }
            );
            job->start();
            jobs.push_back(job);
        }
        return trackJobs("FixUpJob");
    }

    /**
     * Launch the replication jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool launchReplicateJobs() {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "ReplicateJob");

        jobs.clear();
        numFinishedJobs = 0;

        for (auto&& family: databaseFamilies) {
            auto job = ReplicateJob::create(
                family,
                numReplicas,
                controller,
                parentJobId,
                [this](ReplicateJob::Ptr const& job) {
                    ++(this->numFinishedJobs);
                }
            );
            job->start();
            jobs.push_back(job);
        }
        return trackJobs("ReplicateJob");
    }

    /**
     * Launch replica rebalance jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool launchRebalanceJobs() {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "RebalanceJob");

        jobs.clear();
        numFinishedJobs = 0;

        bool const estimateOnly = false;

        for (auto&& family: databaseFamilies) {
            auto job = RebalanceJob::create(
                family,
                estimateOnly,
                controller,
                parentJobId,
                [this](RebalanceJob::Ptr const& job) {
                    ++(this->numFinishedJobs);
                }
            );
            job->start();
            jobs.push_back(job);
        }
        return trackJobs("RebalanceJob");
    }

    /**
     * Launch and track the synchronization jobs.
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool launchSyncJobs() {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << "QservSyncJob");

        jobs.clear();
        numFinishedJobs = 0;

        for (auto&& family: databaseFamilies) {
            auto job = QservSyncJob::create(
                family,
                controller,
                qservSyncTimeoutSec,
                parentJobId,
                forceQservSync,
                [this](QservSyncJob::Ptr const& job) {
                    ++(this->numFinishedJobs);
                }
            );
            job->start();
            jobs.push_back(job);
        }
        return trackJobs("QservSyncJob");
    }

    /**
     * Track the completion of all jobs. Also monitor the termination
     * conditions.
     *
     * @param name - stage (a group of jobs) to be tracked
     *
     * @return 'true' if the job cancelation sequence was initiated
     */
    bool trackJobs(std::string const& name) {

        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << name << ": tracking started");

        util::BlockPost blockPost(1000, 2000);
    
        while (numFinishedJobs != jobs.size()) {
            if (stopReplication or failed) {
                for (auto&& job: jobs) {
                    job->cancel();
                }
                LOGS(_log, LOG_LVL_INFO, replicationLoopContext << name << ": tracking aborted");
                return true;
            }
            blockPost.wait();
        }
        LOGS(_log, LOG_LVL_INFO, replicationLoopContext << name << ": tracking finished");
        return false;
    }

    /**
     * Run the cluster Health Monitoing sequence of actions in a detached
     * thread until a catastrophic failure happens or an external flag telling
     * the the thread to abort its activities and cancel on-going jobs is set.
     *
     * BOTH of the following REQUIREMENTS must be met before initiating single
     * worker eviction:
     *
     * - EVICTED WORKER: both Qserv abd Replication services are not responding
     *   within the specificed eviction interval.
     *
     * - REPLICAITON CLUSTER: the remaining (excluding the EVICTED WORKER) workers
     *   must be available.
     *
     * Otherwise, the Health Monitoring thread will keep tracking status
     * of both services on the worker nodes. At the mean time, the Replication
     * thread will keep trying its best effort in populating a cluster with
     * replicas (even if some replication worker agenst were down).
     */
    void startHealthMonitor() {

        LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "start");

        std::thread healthMonitorThread([this]() {
    
            try {
    
                // Accumulte here non-response intervals for each workers until either will
                // reach the "eviction" threshold. Then trigger worker eviction sequence.

                std::map<std::string,           // worker
                         std::map<std::string,  // service
                                  unsigned int>> workerServiceNoResponseSec;

                for (auto&& worker: controller->serviceProvider()->config()->workers()) {
                    workerServiceNoResponseSec[worker]["qserv"] = 0;
                    workerServiceNoResponseSec[worker]["replication"] = 0;
                }
                while (not failed) {

                    // ---------------------------------------------------------
                    // Probe hosts. Wait for completion or expiration of the job
                    // before analyzing its findings.

                    LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "ClusterHealthJob");

                    std::atomic<bool> finished(false);
                    auto const job = ClusterHealthJob::create(
                        controller,
                        workerResponseTimeoutSec,
                        parentJobId,
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
                            workerServiceNoResponseSec[worker]["qserv"] += workerResponseTimeoutSec;
                            LOGS(_log, LOG_LVL_INFO, healthMonitorContext
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
                            workerServiceNoResponseSec[worker]["replication"] += workerResponseTimeoutSec;
                            LOGS(_log, LOG_LVL_INFO, healthMonitorContext
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

                        if (entry.second.at("replication") >= workerEvictTimeoutSec) {
                            if (entry.second.at("qserv") >= workerEvictTimeoutSec) {
                                workers2evict.push_back(worker);
                                LOGS(_log, LOG_LVL_INFO, healthMonitorContext
                                     << "worker '" << worker << "' has reached eviction timeout of "
                                     << workerEvictTimeoutSec << " seconds");
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
                                util::BlockPost blockPost(1000 * healthProbeIntervalSec,
                                                          1000 * healthProbeIntervalSec + 1);
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

                                if (stopReplication.exchange(true)) {
                                    throw std::logic_error(
                                                healthMonitorContext +
                                                "the cancellation of the Replication thread is already in progress");
                                }

                                LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "Replication cancellation: tracking started");

                                util::BlockPost blockPost(1000, 2000);

                                while (not stopReplication and not failed) {
                                    LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "Replication cancellation: tracking ...");
                                    blockPost.wait();
                                }

                                // In case if the other thread failed then quite this
                                // thread as well
                                if (failed) {
                                    if (stopReplication) stopReplication = false;
                                    LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "Replication cancellation: tracking aborted");
                                    return;
                                }
                                LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "Replication cancellation: tracking finished");

                                // ----------------
                                // Evict the worker

                                LOGS(_log, LOG_LVL_INFO, healthMonitorContext << "DeleteWorkerJob");

                                std::atomic<bool> finished(false);

                                auto const job = DeleteWorkerJob::create(
                                    worker,
                                    permanentDelete,
                                    controller,
                                    parentJobId,
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

                                for (auto&& worker: controller->serviceProvider()->config()->workers()) {
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

                            LOGS(_log, LOG_LVL_INFO, healthMonitorContext
                                 << "automated workers eviction is not possible because too many workers "
                                 << workers2evict.size() << " are offline");

                            break;
                    }
                }

            } catch (std::exception const& ex) {
                LOGS(_log, LOG_LVL_ERROR, healthMonitorContext << "exception: " << ex.what());
                failed = true;
            }
        });
        healthMonitorThread.detach();
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

        LOGS(_log, LOG_LVL_INFO, healthMonitorContext << name << ": tracking started");

        util::BlockPost blockPost(1000, 2000);

        while (not finished and not failed) {
            blockPost.wait();
        }
        if (failed) {
            job->cancel();
            LOGS(_log, LOG_LVL_INFO, healthMonitorContext << name << ": tracking aborted");
            return true;
        }
        LOGS(_log, LOG_LVL_INFO, healthMonitorContext << name << ": tracking finished");

        return false;
    }
};

} /// namespace

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {

        // Instantiate and run the application right away
        ::Application app(argc, argv);

    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "main()  exception: " << ex.what());
        return 1;
    }
    return 0;
}
