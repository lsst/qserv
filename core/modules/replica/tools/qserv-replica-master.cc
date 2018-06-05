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
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/FindAllJob.h"
#include "replica/FixUpJob.h"
#include "replica/JobController.h"
#include "replica/PurgeJob.h"
#include "replica/ReplicateJob.h"
#include "replica/ServiceProvider.h"
#include "replica/VerifyJob.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;

namespace {

// Command line parameters
std::string  configUrl;
unsigned int numReplicas;
bool         computeCheckSum;
bool         bestEffort;
bool         progressReport;
bool         errorReport;
bool         chunkLocksReport;

/**
 * Recursive function for continious submition of the replica verification
 * jobs. The method will ensure a new job will be launched immediatelly upon
 * a completion of the previous one unless the later was was explicitly canceled.
 */
void submitVerifyJob(replica::JobController::Ptr const& jobCtrl) {
    jobCtrl->verify (
        [jobCtrl] (replica::VerifyJob::Ptr const& job) {
            if (job->extendedState() != replica::Job::ExtendedState::CANCELLED) {
                submitVerifyJob (jobCtrl);
            }
        },
        [] (replica::VerifyJob::Ptr const& job,
            replica::ReplicaDiff const& selfReplicaDiff,
            std::vector<replica::ReplicaDiff> const& otherReplicaDiff) {

            replica::ReplicaInfo const& r1 = selfReplicaDiff.replica1();
            replica::ReplicaInfo const& r2 = selfReplicaDiff.replica2();
            std::cout
                << "Compared with OWN previous state  "
                << " " << std::setw(20) << r1.database() << " " << std::setw(12) << r1.chunk()
                << " " << std::setw(20) << r1.worker()   << " " << std::setw(20) << r2.worker() << " "
                << " " << selfReplicaDiff.flags2string()
                << std::endl;

            for (auto const& diff: otherReplicaDiff) {
                replica::ReplicaInfo const& r1 = diff.replica1();
                replica::ReplicaInfo const& r2 = diff.replica2();
                std::cout
                    << "Compared with OTHER replica state "
                    << " " << std::setw(20) << r1.database() << " " << std::setw(12) << r1.chunk()
                    << " " << std::setw(20) << r1.worker()   << " " << std::setw(20) << r2.worker() << " "
                    << " " << diff.flags2string()
                    << std::endl;
            }
        },
        computeCheckSum
    );
}

/// Run the aaplication
bool run() {

    try {

        ///////////////////////////////////////////////////////////////////////////
        // Start the JobbController in its own thread before ininitating any jobs
        // Note that omFinish callbak which are activated upon a completion
        // of the job will be run in a thread wich will differ from the current one

        replica::ServiceProvider::Ptr const provider  = replica::ServiceProvider::create(configUrl);
        replica::JobController::Ptr   const jobCtrl   = replica::JobController::create(provider);

        jobCtrl->run();

        // Refresh the current disposition of replicas accross the cluster.
        // This will also update the state of replicas within the database.

        bool const saveReplicaInfo = true;      // Always do this because chained jobs may
                                                // depend on a persistent state of replicas.

        for (auto const& databaseFamily: provider->config()->databaseFamilies()) {
            std::atomic<bool> finished{false};
            replica::FindAllJob::Ptr const job = jobCtrl->findAll(
                databaseFamily,
                saveReplicaInfo,
                [&finished] (replica::FindAllJob::Ptr const& job) {
                    finished = true;
                }
            );
            util::BlockPost blockPost(1000,2000);
            while (not finished) {
                blockPost.wait();
            }
        }

        // Launch a never-ending replicas verification job

        submitVerifyJob(jobCtrl);

        // Launch a series of jobs witin an infinite loop for each databse family
        //
        // ATTENTION: families are updated on each iteration to promptly see
        //            changes in the system configuration.

        while (true) {

            // Check for chunks which need to be fixed and do so if the ones
            // were found.

            for (auto const& databaseFamily: provider->config()->databaseFamilies()) {
                std::atomic<bool> finished{false};
                replica::FixUpJob::Ptr const job = jobCtrl->fixUp(
                    databaseFamily,
                    [&finished] (replica::FixUpJob::Ptr const& job) {
                        finished = true;
                    }
                );
                util::BlockPost blockPost(1000,2000);
                while (not finished) {
                    blockPost.wait();
                }
            }

            // Check the replication level and bring the minimum number of replicas
            // to the desired level if needed

            for (auto const& databaseFamily: provider->config()->databaseFamilies()) {
                std::atomic<bool> finished{false};
                replica::ReplicateJob::Ptr const job = jobCtrl->replicate(
                    databaseFamily,
                    numReplicas,
                    [&finished] (replica::ReplicateJob::Ptr const& job) {
                        finished = true;
                    }
                );
                util::BlockPost blockPost(1000,2000);
                while (not finished) {
                    blockPost.wait();
                }
            }

            // Check the replication level and shave off the excess replicas
            // to the desired level if needed

            for (auto const& databaseFamily: provider->config()->databaseFamilies()) {
                std::atomic<bool> finished{false};
                replica::PurgeJob::Ptr const job = jobCtrl->purge(
                    databaseFamily,
                    numReplicas,
                    [&finished] (replica::PurgeJob::Ptr const& job) {
                        finished = true;
                    }
                );
                util::BlockPost blockPost(1000,2000);
                while (not finished) {
                    blockPost.wait();
                }
            }
        }

        /////////////////////////////////////////
        // Gracefully shutdown the Job Controller

        jobCtrl->stop();

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return true;
}
} /// namespace

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  [--config=<url>]\n"
            "  [--replicas=<number>]\n"
            "  [--check-sum]\n"
            "  [--best-effort]\n"
            "  [--progress-report]\n"
            "  [--error-report]\n"
            "  [--chunk-locks-report]\n"
            "\n"
            "Flags and options:\n"
            "  --config             - a configuration URL (a configuration file or a set of the database\n"
            "                         connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --replicas           - the desired number of replicas [ DEFAULT: '0' to pull the number\n"
            "                         from the Configuration]\n"
            "  --check-sum          - compute check/control sum of files\n"
            "  --best-effort        - allowing the operation even after not getting chunk disposition from\n"
            "                         all workers\n"
            "  --progress-report    - progress report when executing batches of requests\n"
            "  --error-report       - detailed report on failed requests\n"
            "  --chunk-locks-report - report chunks which are locked after finishing each job\n");

        ::configUrl        = parser.option<std::string>("config", "file:replication.cfg");
        ::numReplicas      = parser.option<unsigned int>("replicas", 0);
        ::computeCheckSum  = parser.flag("check-sum");
        ::bestEffort       = parser.flag("best-effort");
        ::progressReport   = parser.flag("progress-report");
        ::errorReport      = parser.flag("error-report");
        ::chunkLocksReport = parser.flag("chunk-locks-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::run();
    return 0;
}
