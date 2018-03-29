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
#include <atomic>
#include <list>
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
#include "replica/QservGetReplicasJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

#define OUT std::cout

namespace {

// Command line parameters

std::string databaseFamily;
std::string configUrl;
bool        pullQservReplicas;
bool        progressReport;
bool        errorReport;
bool        detailedReport;

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

/**
 * @return a string in which participating workers are represented by some
 * non default character at the corresponding worker position starting with
 * index 0 (counting from the left to the right).
 *
 * The meaning of characters:
 *   '-' - the default character
 *   'R' - the worker is known to the replication system only
 *   'Q' - the worker is known to Qserv only
 *   'B' - th eworker is known to both the Replication system and Qserv
 *
 * @param worker2idx - index map for workers (worker name to its 0-basd index)
 * @param workers    - collection of worker names participating in the operation
 */
std::string workers2str(std::map<std::string,size_t> const& worker2idx,
                        std::set<std::string> const& workers,
                        std::set<std::string> const& qservWorkers) {

    // Prepare a blank line using symbol '--' as a placeholder for workers
    // at the relative 0-based positions
    std::string result(3*worker2idx.size(), ' ');
    for (size_t idx = 0, num = worker2idx.size(); idx < num; ++idx) {
        result[3*idx]   = '-';
        result[3*idx+1] = '-';
    }

    // Fill-in participating workers at their positions in the line
    for (auto const& worker: workers) {
        result[3*worker2idx.at(worker)] = 'R';
    }
    for (auto const& worker: qservWorkers) {
        result[3*worker2idx.at(worker)+1] = 'Q';
    }
    return result;
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

        util::BlockPost blockPost(1000,2000);

        ///////////////////////////////////////////////////////////////////
        // Start two parallel jobs, the firts one getting the latest state
        // of replicas accross the Replication cluster, and the second one
        // getting a list of replicas known to Qserv workers.

        std::atomic<bool> replicaJobFinished{false};
        auto findAllJob = replica::FindAllJob::create(
            databaseFamily,
            controller,
            std::string(),
            [&replicaJobFinished] (replica::FindAllJob::pointer const& job) {
                replicaJobFinished = true;
            }
        );
        findAllJob->start();

        replica::QservGetReplicasJob::pointer qservGetReplicasJob;
        if (pullQservReplicas) {
            std::atomic<bool> qservJobFinished{false};
            bool const inUseOnly = false;
            qservGetReplicasJob = replica::QservGetReplicasJob::create(
                databaseFamily,
                controller,
                std::string(),
                inUseOnly,
                [&qservJobFinished] (replica::QservGetReplicasJob::pointer const& job) {
                    qservJobFinished = true;
                }
            );
            qservGetReplicasJob->start();

            while (not (replicaJobFinished and qservJobFinished)) {
                blockPost.wait();
            }
            OUT << "qserv-replica-job-cunks:\n"
                << "   FindAllJob          finished: " << findAllJob->state2string() << "\n"
                << "   QservGetReplicasJob finished: " << qservGetReplicasJob->state2string() << "\n"
                << std::endl;
        } else {
            while (not replicaJobFinished) {
                blockPost.wait();
            }
            OUT << "qserv-replica-job-cunks:\n"
                << "   FindAllJob          finished: " << findAllJob->state2string() << "\n"
                << std::endl;
        }

        //////////////////////////////
        // Analyse and display results

        replica::FindAllJobResult const& replicaData = findAllJob->getReplicaData();
        if (detailedReport) {
            dump(replicaData);
        }
        replica::QservGetReplicasJobResult qservReplicaData;
        if (pullQservReplicas) {
            qservReplicaData = qservGetReplicasJob->getReplicaData();
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

        // Count chunk replicas per worker from both sources

        std::map<std::string,size_t> worker2numChunks;
        for (auto const& replicaCollection: replicaData.replicas) {
            for (auto const& replica: replicaCollection) {
                worker2numChunks[replica.worker()]++;
            }
        }

        std::map<std::string,size_t> qservWorker2numChunks;
        if (pullQservReplicas) {
            for (auto const& entry: qservReplicaData.replicas) {
                auto const& worker = entry.first;
                auto const& replicaCollection = entry.second;
                qservWorker2numChunks[worker] = replicaCollection.size();
            }
        }

        OUT << "\n"
            << "CHUNK DISTRIBUTION PER EACH WORKER:\n"
            << "  LEGEND:\n"
            << "    for numbers of chunk replicas from different sources: 'R'eplication, 'Q'serv\n"
            << "-----+------------------------------------------+--------+--------\n"
            << " idx |                                   worker |      R |      Q \n"
            << "-----+------------------------------------------+--------+--------\n";

        for (auto const& worker: provider->config()->workers()) {

            std::string const numReplicasStr =
                replicaData.workers.at(worker) ?
                    std::to_string(worker2numChunks[worker]) :
                    "*";

            std::string const numQservReplicasStr =
                pullQservReplicas and qservReplicaData.workers.at(worker) ?
                    std::to_string(qservWorker2numChunks[worker]) :
                    "*";

            OUT << " " << std::setw(3)  << worker2idx[worker] << " |"
                << " " << std::setw(40) << worker << " |"
                << " " << std::setw(6)  << numReplicasStr << " |"
                << " " << std::setw(6)  << numQservReplicasStr
                << "\n";
        }
        OUT << "-----+------------------------------------------+--------+--------\n"
            << std::endl;

        OUT << "REPLICAS:\n"
            << "  LEGEND:\n"
            << "    for the desired minimal replication 'L'evel\n"
            << "    for numbers of chunk replicas from different sources: 'R'eplication, 'Q'serv\n"
            << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n"
            << "       chunk |             database |   R |  R-L |   Q |  Q-R | replicas at workers\n"
            << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n";

        size_t const replicationLevel = provider->config()->replicationLevel(databaseFamily);

        for (auto chunk: replicaData.chunks.chunkNumbers()) {
            auto chunkMap = replicaData.chunks.chunk(chunk);

            for (auto database: chunkMap.databaseNames()) {
                auto databaseMap = chunkMap.database(database);

                size_t      const  numReplicas        = databaseMap.size();
                std::string const  numReplicasStr     = numReplicas ? std::to_string(numReplicas) : "";
                long long   const  numReplicasDiff    = numReplicas - replicationLevel;
                std::string const  numReplicasDiffStr = numReplicasDiff ? std::to_string(numReplicasDiff) : "";

                OUT << " "   << std::setw(11) << chunk
                    << " | " << std::setw(20) << database
                    << " | " << std::setw(3)  << numReplicasStr
                    << " | " << std::setw(4)  << numReplicasDiffStr;

                if (pullQservReplicas) {
                    size_t const numQservReplicas =
                        qservReplicaData.useCount.chunkExists(chunk) and
                        qservReplicaData.useCount.atChunk(chunk).databaseExists(database) ?
                            qservReplicaData.useCount.atChunk(chunk).atDatabase(database).size() :
                            0;

                    std::string const numQservReplicasStr     = numQservReplicas ? std::to_string(numQservReplicas) : "";
                    long long   const numQservReplicasDiff    = numQservReplicas - numReplicas;
                    std::string const numQservReplicasDiffStr = numQservReplicasDiff ? std::to_string(numQservReplicasDiff) : "";

                    OUT << " | " << std::setw(3)  << numQservReplicasStr
                        << " | " << std::setw(4)  << numQservReplicasDiffStr;
                } else {
                    OUT << " | " << std::setw(3)  << "*"
                        << " | " << std::setw(4)  << "*";
                }

                std::set<std::string> workers;
                for (auto worker: databaseMap.workerNames()) {
                    workers.insert(worker);
                }
                std::set<std::string> qservWorkers;
                if (qservReplicaData.useCount.chunkExists(chunk) and
                    qservReplicaData.useCount.atChunk(chunk).databaseExists(database)) {
                    for (auto const& worker: qservReplicaData.useCount
                                                             .atChunk(chunk)
                                                             .atDatabase(database)
                                                             .workerNames()) {
                        qservWorkers.insert(worker);
                    }
                }

                OUT << " | " << workers2str(worker2idx, workers, qservWorkers) << "\n";
            }
        }
        OUT << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n"
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
            "  --qserv-replicas   - also pull replicas from Qserv workers for the analysis\n"
            "  --progress-report  - progress report when executing batches of requests\n"
            "  --error-report     - detailed report on failed requests\n"
            "  --detailed-report  - detailed report on results\n");

        ::databaseFamily    = parser.parameter<std::string>(1);
        ::configUrl         = parser.option<std::string>("config", "file:replication.cfg");
        ::pullQservReplicas = parser.flag("qserv-replicas");
        ::progressReport    = parser.flag("progress-report");
        ::errorReport       = parser.flag("error-report");
        ::detailedReport    = parser.flag("detailed-report");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
