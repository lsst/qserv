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

/// qserv-replica-job-sync.cc implements a command-line tool which synchronizes
/// chunk configurations of Qserv workers with the status of good replicas
/// known to the Replication System for the specified database family.

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
#include "replica/Controller.h"
#include "replica/QservSyncJob.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

// Command line parameters

std::string databaseFamily;
std::string configUrl;
bool        force;
bool        progressReport;
bool        errorReport;
bool        chunkLocksReport;

/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        replica::ServiceProvider::pointer const provider   = replica::ServiceProvider::create(configUrl);
        replica::Controller::pointer      const controller = replica::Controller::create(provider);

        controller->run();

        ////////////////////////////////////////
        // Find all replicas accross all workers

        std::atomic<bool> finished{false};
        auto job = replica::QservSyncJob::create(
            databaseFamily,
            controller,
            std::string(),
            force,
            [&finished] (replica::QservSyncJob::pointer const& job) {
                finished = true;
            }
        );
        job->start();

        util::BlockPost blockPost(1000,2000);
        while (not finished) {
            blockPost.wait();
        }

        //////////////////////////////
        // Analyse and display results

        replica::QservSyncJobResult const& replicaData = job->getReplicaData();

        std::cout
            << "\n"
            << "CHUNK DISTRIBUTION:\n"
            << "----------------------------------------+--------------+--------------\n"
            << "                                 worker | prev #chunks |  new #chunks \n"
            << "----------------------------------------+--------------+--------------\n";

        for (auto const& workerEntry: replicaData.workers) {
            std::string const& worker    = workerEntry.first;
            bool        const  succeeded = workerEntry.second;
            std::cout
                << " " << std::setw(38) << worker << " | " << std::setw(12)
                << (succeeded ? std::to_string(replicaData.prevReplicas.at(worker).size()) : "FAILED")
                << " | " << std::setw(12)
                << (succeeded ? std::to_string(replicaData.newReplicas.at(worker).size()) : "FAILED")
                << "\n";
        }

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

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
            "  <database-family> [--config=<url>]\n"
            "                    [--force]\n"
            "                    [--progress-report]\n"
            "                    [--error-report]\n"
            "                    [--chunk-locks-report]\n"
            "\n"
            "Parameters:\n"
            "  <database-family>    - the name of a database family to inspect\n"
            "\n"
            "Flags and options:\n"
            "  --config             - a configuration URL (a configuration file or a set of the database\n"
            "                         connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --force              - force teh operation even if some replicas are in use\n"
            "  --progress-report    - progress report when executing batches of requests\n"
            "  --error-report       - detailed report on failed requests\n"
            "  --chunk-locks-report - report chunks which are locked\n");

        ::databaseFamily   = parser.parameter<std::string>(1);
        ::configUrl        = parser.option<std::string>("config", "file:replication.cfg");
        ::force            = parser.flag("force");
        ::progressReport   = parser.flag("progress-report");
        ::errorReport      = parser.flag("error-report");
        ::chunkLocksReport = parser.flag("chunk-locks-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
