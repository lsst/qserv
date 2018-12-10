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

/// qserv-replica-job-delete-worker.cc is a Controller application
/// for testing the corresponding job.

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/DeleteWorkerJob.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;

namespace {

// Command line parameters

std::string worker;
std::string configUrl;
bool        permanentDelete;
bool        progressReport;
bool        errorReport;
bool        chunkLocksReport;

/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////////
        // Start the provider in its own thread pool before initiating any requests
        // or jobs.
        //
        // Note that onFinish callbacks which are activated upon the completion of
        // the requests or jobs will be run by a thread from the pool.

        replica::ServiceProvider::Ptr const provider   = replica::ServiceProvider::create(configUrl);
        replica::Controller::Ptr      const controller = replica::Controller::create(provider);

        provider->run();

        ////////////////////
        // Start replication

        std::atomic<bool> finished{false};
        auto job = replica::DeleteWorkerJob::create(
            worker,
            permanentDelete,
            controller,
            std::string(),
            [&finished] (replica::DeleteWorkerJob::Ptr job) {
                finished = true;
            }
        );
        job->start();

        util::BlockPost blockPost(1000,2000);
        while (not finished) {
            blockPost.wait();
        }

        replica::DeleteWorkerJobResult const& jobReport = job->getReplicaData();

        //////////////////////////////////
        // New replicas created by the job

        std::cout
            << "REPLICAS:\n"
            << "----------+----------+-----------------------------------------\n"
            << "    chunk | database | workers\n";

        for (auto const& databaseFamilyEntry: jobReport.chunks) {
            for (auto const& chunkEntry: databaseFamilyEntry.second) {

                unsigned int const& chunk = chunkEntry.first;
                for (auto const& databaseEntry: chunkEntry.second) {

                    std::string const& database = databaseEntry.first;

                    std::cout
                        << " "   << std::setw(8) << chunk
                        << " | " << std::setw(8) << database
                        << " | ";

                    for (auto const& replicaEntry: databaseEntry.second) {

                        std::string          const& worker = replicaEntry.first;
                        replica::ReplicaInfo const& info   = replicaEntry.second;

                        std::cout << worker << (info.status() != replica::ReplicaInfo::Status::COMPLETE ? "(!)" : "") << " ";
                    }
                    std::cout << "\n";
                }
            }
        }
        std::cout
            << "----------+----------+-----------------------------------------\n"
            << std::endl;

        //////////////////////////////////////////////////
        // Orphan chunks left as a result of the operation

        std::cout
            << "ORPHAN CHUNKS\n"
            << "-------+--------------------\n";

        for (auto const& chunkEntry: jobReport.orphanChunks) {
            unsigned int const chunk = chunkEntry.first;
            for (auto const& databaseEntry: chunkEntry.second) {
                std::string const& database = databaseEntry.first;
                std::cout
                    << " " << std::setw(6) << chunk << " | " << database << "\n";
            }
        }

        ///////////////////////////////////////////////////
        // Shutdown the provider and join with its threads

        provider->stop();

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
            "  <worker> [--config=<url>]\n"
            "           [--permanent-delete]\n"
            "           [--progress-report]\n"
            "           [--error-report]\n"
            "           [--chunk-locks-report]\n"
            "\n"
            "Parameters:\n"
            "  <worker>             - the name of a worker to be removed\n"
            "\n"
            "Flags and options:\n"
            "  --config             - a configuration URL (a configuration file or a set of the database\n"
            "                         connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --permanent-delete   - permanently delete a worker from the Configuration\n"
            "  --progress-report    - progress report when executing batches of requests\n"
            "  --error-report       - the flag triggering detailed report on failed requests\n"
            "  --chunk-locks-report - report chunks which are locked\n");

        ::worker           = parser.parameter<std::string>(1);
        ::configUrl        = parser.option<std::string>("config", "file:replication.cfg");
        ::permanentDelete  = parser.flag("permanent-delete");
        ::progressReport   = parser.flag("progress-report");
        ::errorReport      = parser.flag("error-report");
        ::chunkLocksReport = parser.flag("chunk-locks-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
