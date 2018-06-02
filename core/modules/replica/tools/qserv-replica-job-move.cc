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

/// qserv-replica-job-move.cc is a single job Controller application
/// which is meant to run the corresponding job.

// System headers
#include <atomic>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/MoveReplicaJob.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;

namespace {

// Command line parameters

std::string  databaseFamily;
unsigned int chunk;
std::string  sourceWorker;
std::string  destinationWorker;
std::string  configUrl;
bool         purge;
bool         progressReport;
bool         errorReport;
bool         chunkLocksReport;

template <class COLLECTION>
void printReplicaInfo(std::string const& collectionName,
                      COLLECTION  const& collection) {
    std::cout
        << collectionName << ":\n"
        << "----------+----------+-----+-----------------------------------------\n"
        << "    chunk | database | rep | workers\n";

    unsigned int prevChunk = (unsigned int) -1;

    for (auto const& chunkEntry: collection) {

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
}

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

        ////////////////////
        // Start replication

        std::atomic<bool> finished{false};
        auto job = replica::MoveReplicaJob::create(
            databaseFamily,
            chunk,
            sourceWorker,
            destinationWorker,
            purge,
            controller,
            std::string(),
            [&finished] (replica::MoveReplicaJob::Ptr job) {
                finished = true;
            }
        );
        job->start();

        util::BlockPost blockPost(1000,2000);
        while (not finished) {
            std::cout
                << "qserv-replica-job-move:"
                << "  Controller::numActiveRequests: " << controller->numActiveRequests()
                << ", MoveReplicaJob::state: " << job->state2string()
                << std::endl;
            blockPost.wait();
        }

        //////////////////////////////
        // Analyse and display results

        replica::MoveReplicaJobResult const& replicaData = job->getReplicaData();

        printReplicaInfo("CREATED REPLICAS", replicaData.createdChunks);
        printReplicaInfo("DELETED REPLICAS", replicaData.deletedChunks);

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
            "  <database-family> <chunk> <source-worker> <destination-worker>\n"
            "    [--config=<url>]\n"
            "    [--purge]\n"
            "    [--progress-report]\n"
            "    [--error-report]\n"
            "    [--chunk-locks-report]\n"
            "\n"
            "Parameters:\n"
            "  <database-family>    - the name of a database family to inspect\n"
            "\n"
            "Flags and options:\n"
            "  --config             - a configuration URL (a configuration file or a set of the database\n"
            "                         connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --purge              - purge the input replica at the source worker upon a successful\n"
            "                         completion of the operation\n"
            "  --progress-report    - progress report when executing batches of requests\n"
            "  --error-report       - the flag triggering detailed report on failed requests\n"
            "  --chunk-locks-report - report chunks which are locked\n");

        ::databaseFamily    = parser.parameter<std::string>(1);
        ::chunk             = parser.parameter<unsigned int>(2);
        ::sourceWorker      = parser.parameter<std::string>(3);
        ::destinationWorker = parser.parameter<std::string>(4);
        ::configUrl         = parser.option<std::string>("config", "file:replication.cfg");
        ::purge             = parser.flag("purge");
        ::progressReport    = parser.flag("progress-report");
        ::errorReport       = parser.flag("error-report");
        ::chunkLocksReport  = parser.flag("chunk-locks-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
