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

/// replica_job_chunks.cc implements a command-line tool which analyzes
/// and reports chunk disposition in the specified database family.

// System headers
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
#include "replica/FindAllJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

#define OUT std::cout

namespace {

// Command line parameters

std::string databaseFamily;
std::string configUrl;
bool        progressReport;
bool        errorReport;
bool        detailedReport;
bool        chunkLocksReport = false;

void dump(replica::FindAllJobResult const& replicaData) {
    OUT << "*** DETAILED REPORTS ***\n"
        << "\nCO-LOCATION:\n";
    for (auto const& chunk2workers: replicaData.isColocated) {
        unsigned int chunk = chunk2workers.first;

        for (auto const& worker2colocated: chunk2workers.second) {
            std::string const& destinationWorker = worker2colocated.first;
            bool        const  isColocated       = worker2colocated.second;
            
            OUT << "  "
                << "  chunk: "  << std::setw(6) << chunk
                << "  worker: " << std::setw(12) << destinationWorker
                << "  isColocated: " << (isColocated ? "YES" : "NO")
                << "\n";
        }
    }
}

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

        auto job =
            replica::FindAllJob::create(
                databaseFamily,
                controller,
                [] (replica::FindAllJob::pointer const& job) {
                    // Not using the callback because the completion of
                    // the request will be caught by the tracker below
                    ;
                }
            );

        job->start();
        job->track(progressReport,
                   errorReport,
                   chunkLocksReport,
                   OUT);

        //////////////////////////////
        // Analyse and display results

        replica::FindAllJobResult const& replicaData = job->getReplicaData();
        if (detailedReport) {
            dump(replicaData);
        }

        // Build and print a map of worker "numbers" to use them instead of
        // (potentially) very long worker identifiers

        std::vector<std::string> const workers = provider->config()->workers();
        std::map<std::string,size_t> worker2idx;

        OUT << "\n"
            << "WORKERS:\n";
        for (size_t idx = 0, num = workers.size(); idx < num; ++idx) {
            std::string const& worker = workers[idx];
            worker2idx[worker] = idx;
            OUT << std::setw(3) << idx << ": " << worker << "\n";
        }
        OUT << std::endl;

        // Build a set of failed workers

        std::set<std::string> failedWorkers;
        for (auto const& entry: replicaData.workers) {
            if (not entry.second) { failedWorkers.insert(entry.first); }
        }
        std::map<std::string,
                 std::map<unsigned int,
                         std::map<std::string,
                                  bool>>> worker2chunks2databases;

        for (replica::ReplicaInfoCollection const& replicaCollection: replicaData.replicas) {
            for (replica::ReplicaInfo const& replica: replicaCollection) {
                worker2chunks2databases[replica.worker()][replica.chunk()][replica.database()] = true;
            }
        }

        OUT << "\n"
            << "CHUNK DISTRIBUTION:\n"
            << "---------+------------\n"
            << "  worker | num.chunks \n"
            << "---------+------------\n";

        for (auto const& worker: provider->config()->workers()) {
            OUT << " " << std::setw(7) << worker2idx[worker] << " | " << std::setw(10)
                << (failedWorkers.count(worker) ? "*" : std::to_string(worker2chunks2databases[worker].size()))
                << "\n";
        }
        OUT << "---------+------------\n"
            << std::endl;

        OUT << "REPLICAS:\n"
            << "-------------+----------+-----+-----+-----------------------------------------\n"
            << "       chunk | database | rep | r+- | workers\n";

        size_t const replicationLevel = provider->config()->replicationLevel(databaseFamily);

        unsigned int prevChunk  = (unsigned int) -1;

        for (auto const& chunkEntry: replicaData.chunks) {

            unsigned int const& chunk = chunkEntry.first;
            for (auto const& databaseEntry: chunkEntry.second) {

                std::string const& database = databaseEntry.first;

                size_t      const  numReplicas        = databaseEntry.second.size();
                long long   const  numReplicasDiff    = numReplicas - replicationLevel;
                std::string const  numReplicasDiffStr = numReplicasDiff ? std::to_string(numReplicasDiff) : "";

                if (chunk != prevChunk) {
                    OUT << "-------------+----------+-----+-----+-----------------------------------------\n";
                }
                prevChunk = chunk;

                OUT << " "   << std::setw(11) << chunk
                    << " | " << std::setw(8)  << database
                    << " | " << std::setw(3)  << numReplicas
                    << " | " << std::setw(3)  << numReplicasDiffStr
                    << " | ";

                for (auto const& replicaEntry: databaseEntry.second) {

                    std::string     const& worker = replicaEntry.first;
                    replica::ReplicaInfo const& info   = replicaEntry.second;

                    OUT << worker2idx[worker] << (info.status() != replica::ReplicaInfo::Status::COMPLETE ? "(!)" : "") << " ";
                }
                OUT << "\n";
            }
        }
        OUT << "-------------+----------+-----+-----+-----------------------------------------\n"
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
            "                    [--detailed-report]\n"
            "\n"
            "Parameters:\n"
            "  <database-family>  - the name of a database family to inspect\n"
            "\n"
            "Flags and options:\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --progress-report  - progress report when executing batches of requests\n"
            "  --error-report     - detailed report on failed requests\n"
            "  --detailed-report  - detailed report on results\n");

        ::databaseFamily = parser.parameter<std::string>(1);
        ::configUrl      = parser.option<std::string>("config", "file:replication.cfg");
        ::progressReport = parser.flag("progress-report");
        ::errorReport    = parser.flag("error-report");
        ::detailedReport = parser.flag("detailed-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    ::test();
    return 0;
}