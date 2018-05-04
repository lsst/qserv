/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// replica_job_fixup.cc implements a command-line tool which analyzes
/// chunk disposition within the specified database family and tries
/// to fix chunk co-location problems if found.

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
#include "replica/FixUpJob.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

// Command line parameters

std::string databaseFamily;
std::string configUrl;
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

        replica::ServiceProvider::Ptr const provider   = replica::ServiceProvider::create(configUrl);
        replica::Controller::Ptr      const controller = replica::Controller::create(provider);

        controller->run();

        ////////////////////////////////////////
        // Find all replicas accross all workers

        std::atomic<bool> finished{false};
        auto job = replica::FixUpJob::create(
            databaseFamily,
            controller,
            std::string(),
            [&finished] (replica::FixUpJob::Ptr job) {
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

        replica::FixUpJobResult const& replicaData = job->getReplicaData();

        std::cout
            << "\n"
            << "WORKERS:";
        for (auto const& worker: provider->config()->workers()) {
            std::cout << " " << worker;
        }
        std::cout
            << std::endl;

        // Failed workers
        std::set<std::string> failedWorkers;

        for (auto const& entry: replicaData.workers) {
            if (!entry.second) failedWorkers.insert(entry.first);
        }
        std::map<std::string, std::vector<unsigned int>> worker2chunks;     // Chunks hosted by a worker

        for (replica::ReplicaInfo const& replica: replicaData.replicas) {
            worker2chunks[replica.worker()].push_back(replica.chunk());
        }
        std::cout
            << "\n"
            << "CHUNK DISTRIBUTION:\n"
            << "----------+------------\n"
            << "   worker | num.chunks \n"
            << "----------+------------\n";

        for (auto const& worker: provider->config()->workers()) {
            std::cout
                << " " << std::setw(8) << worker << " | " << std::setw(10)
                << (failedWorkers.count(worker) ? "*" : std::to_string(worker2chunks[worker].size())) << "\n";
        }
        std::cout
            << "----------+------------\n"
            << std::endl;

        std::cout
            << "REPLICAS:\n"
            << "----------+----------+-----+-----------------------------------------\n"
            << "    chunk | database | rep | workers\n";

        unsigned int prevChunk = (unsigned int) -1;

        for (auto const& chunkEntry: replicaData.chunks) {

            unsigned int const& chunk = chunkEntry.first;
            for (auto const& databaseEntry: chunkEntry.second) {

                std::string const& database    = databaseEntry.first;
                size_t      const  numReplicas = databaseEntry.second.size();

                if (chunk != prevChunk)
                    std::cout
                        << "----------+----------+-----+-----------------------------------------\n";

                prevChunk = chunk;

                std::cout
                    << " "   << std::setw(8) << chunk
                    << " | " << std::setw(8) << database
                    << " | " << std::setw(3) << numReplicas
                    << " | ";

                for (auto const& replicaEntry: databaseEntry.second) {

                    std::string          const& worker = replicaEntry.first;
                    replica::ReplicaInfo const& info   = replicaEntry.second;

                    std::cout << worker << (info.status() != replica::ReplicaInfo::Status::COMPLETE ? "(!)" : "") << " ";
                }
                std::cout << "\n";
            }
        }
        std::cout
            << "----------+----------+-----+-----------------------------------------\n"
            << std::endl;

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
            "  --progress-report    - progress report when executing batches of requests\n"
            "  --error-report       - detailed report on failed requests\n"
            "  --chunk-locks-report - report chunks which are locked\n");

        ::databaseFamily   = parser.parameter<std::string>(1);
        ::configUrl        = parser.option<std::string>("config", "file:replication.cfg");
        ::progressReport   = parser.flag("progress-report");
        ::errorReport      = parser.flag("error-report");
        ::chunkLocksReport = parser.flag("chunk-locks-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
