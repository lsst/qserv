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

// Class header
#include "replica/ChunksApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/FindAllJob.h"
#include "replica/QservGetReplicasJob.h"
#include "replica/ReplicaInfo.h"
#include "util/BlockPost.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description {
    "This is a Controller application which launches a single job Controller in order"
    " to acquire, analyze and reports chunk disposition within a database family."
};

/**
 * Dump the replica info
 */
void dump(lsst::qserv::replica::FindAllJobResult const& replicaData) {

    cout << "*** DETAILED REPORTS ***\n"
         << "\nCO-LOCATION:\n";

    for (auto&& chunk2workers: replicaData.isColocated) {
        unsigned int const chunk = chunk2workers.first;

        for (auto&& worker2colocated: chunk2workers.second) {
            auto&& destinationWorker = worker2colocated.first;
            bool const isColocated   = worker2colocated.second;

            cout << "  "
                 << "  chunk: "  << setw(6) << chunk
                 << "  worker: " << setw(12) << destinationWorker
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
 *   'B' - the worker is known to both the Replication system and Qserv
 *
 * @param worker2idx - index map for workers (worker name to its 0-based index)
 * @param workers    - collection of worker names participating in the operation
 */
string workers2str(map<string, size_t> const& worker2idx,
                   set<string> const& workers,
                   set<string> const& qservWorkers) {

    // Prepare a blank line using symbol '--' as a placeholder for workers
    // at the relative 0-based positions
    string result(3*worker2idx.size(), ' ');
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

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ChunksApp::Ptr ChunksApp::create(int argc,
                                 const char* const argv[]) {
    return Ptr(
        new ChunksApp(
            argc,
            argv
        )
    );
}


ChunksApp::ChunksApp(int argc,
                     const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().required(
        "database-family",
        "the name of a database family to inspect",
        _databaseFamily);

    parser().flag(
        "all-workers",
        "the flag for selecting all workers regardless of their status (DISABLED or READ-ONLY)",
        _allWorkers);

    parser().option(
        "timeout",
        "maximum timeout (seconds) for the replica disposition requests",
        _timeoutSec);

    parser().flag(
        "do-not-save-replica",
        "the flag which (if used) prevents the application from saving replica info in a database."
        " This may significantly speed up the application in setups where the number of chunks is on"
        " a scale of one million, or exceeds it.",
        _doNotSaveReplicaInfo);

    parser().flag(
        "qserv-replicas",
        "the flag for pulling chunk disposition from Qserv workers for the combined analysis",
        _pullQservReplicas);

    parser().flag(
        "detailed-report",
        "the flag triggering detailed report on the found replicas",
        _detailedReport);

    parser().option(
        "tables-page-size",
        "the number of rows in the table of replicas (0 means no pages)",
        _pageSize);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in reports",
        _verticalSeparator);
}


int ChunksApp::runImpl() {

    auto controller = Controller::create(serviceProvider());

    // Workers requested
    auto const workers = _allWorkers ?
        serviceProvider()->config()->allWorkers() :
        serviceProvider()->config()->workers();

    // Limit requests and jobs execution time if such limit was provided
    if (_timeoutSec != 0) {
        serviceProvider()->config()->setControllerRequestTimeoutSec(_timeoutSec);
        serviceProvider()->config()->setJobTimeoutSec(_timeoutSec);
    }

    ///////////////////////////////////////////////////////////////////
    // Start two parallel jobs, the first one getting the latest state
    // of replicas across the Replication cluster, and the second one
    // getting a list of replicas known to Qserv workers.
    //
    // ATTENTION: jobs are allowed to be partially successful if some
    // workers are offline.

    util::BlockPost blockPost(1000,2000);

    atomic<bool> replicaJobFinished{false};
    auto findAllJob = FindAllJob::create(
        _databaseFamily,
        not _doNotSaveReplicaInfo,
        controller,
        string(),
        [&replicaJobFinished] (FindAllJob::Ptr const& job) {
            replicaJobFinished = true;
        }
    );
    findAllJob->start();

    QservGetReplicasJob::Ptr qservGetReplicasJob;
    if (_pullQservReplicas) {
        atomic<bool> qservJobFinished{false};
        bool const inUseOnly = false;
        qservGetReplicasJob = QservGetReplicasJob::create(
            _databaseFamily,
            inUseOnly,
            controller,
            string(),
            [&qservJobFinished] (QservGetReplicasJob::Ptr const& job) {
                qservJobFinished = true;
            }
        );
        qservGetReplicasJob->start();

        while (not (replicaJobFinished and qservJobFinished)) {
            blockPost.wait();
        }
        cout << "qserv-replica-job-chunks:\n"
             << "   FindAllJob          finished: " << findAllJob->state2string() << "\n"
             << "   QservGetReplicasJob finished: " << qservGetReplicasJob->state2string() << "\n"
             << endl;
    } else {
        while (not replicaJobFinished) {
            blockPost.wait();
        }
        cout << "qserv-replica-job-chunks:\n"
             << "   FindAllJob          finished: " << findAllJob->state2string() << "\n"
             << endl;
    }

    //////////////////////////////
    // Analyze and display results

    FindAllJobResult const& replicaData = findAllJob->getReplicaData();
    if (_detailedReport) {
        ::dump(replicaData);
    }
    QservGetReplicasJobResult qservReplicaData;
    if (_pullQservReplicas) {
        qservReplicaData = qservGetReplicasJob->getReplicaData();
    }

    // Build and print a map of worker "numbers" to use them instead of
    // (potentially) very long worker identifiers

    map<string, size_t> worker2idx;

    cout << "\n"
         << "WORKERS:\n";
    for (size_t idx = 0, num = workers.size(); idx < num; ++idx) {
        string const& worker = workers[idx];
        worker2idx[worker] = idx;
        cout << setw(3) << idx << ": " << worker << "\n";
    }
    cout << endl;

    // Count chunk replicas per worker from both sources

    map<string, size_t> worker2numChunks;
    for (auto const& replicaCollection: replicaData.replicas) {
        for (auto const& replica: replicaCollection) {
            worker2numChunks[replica.worker()]++;
        }
    }

    map<string, size_t> qservWorker2numChunks;
    if (_pullQservReplicas) {
        for (auto const& entry: qservReplicaData.replicas) {
            auto const& worker = entry.first;
            auto const& replicaCollection = entry.second;
            qservWorker2numChunks[worker] = replicaCollection.size();
        }
    }

    cout << "\n"
         << "CHUNK DISTRIBUTION PER EACH WORKER:\n"
         << "  LEGEND:\n"
         << "    for numbers of chunk replicas from different sources: 'R'eplication, 'Q'serv\n"
         << "-----+------------------------------------------+--------+--------\n"
         << " idx |                                   worker |      R |      Q \n"
         << "-----+------------------------------------------+--------+--------\n";

    for (auto const& worker: workers) {

        string const numReplicasStr =
            replicaData.workers.at(worker) ?
                to_string(worker2numChunks[worker]) :
                "*";

        string const numQservReplicasStr =
            _pullQservReplicas and qservReplicaData.workers.at(worker) ?
                to_string(qservWorker2numChunks[worker]) :
                "*";

        cout << " " << setw(3)  << worker2idx[worker] << " |"
             << " " << setw(40) << worker << " |"
             << " " << setw(6)  << numReplicasStr << " |"
             << " " << setw(6)  << numQservReplicasStr
             << "\n";
    }
    cout << "-----+------------------------------------------+--------+--------\n"
         << endl;

    cout << "REPLICAS:\n"
         << "  LEGEND:\n"
         << "    for the desired minimal replication 'L'evel\n"
         << "    for numbers of chunk replicas from different sources: 'R'eplication, 'Q'serv\n"
         << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n"
         << "       chunk |             database |   R |  R-L |   Q |  Q-R | replicas at workers\n"
         << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n";

    size_t const replicationLevel = serviceProvider()->config()->replicationLevel(_databaseFamily);

    for (auto&& chunk: replicaData.chunks) {
        auto&& chunkNumber = chunk.first; 
        auto&& databases   = chunk.second;

        for (auto&& database: databases) {
            auto&& databaseName = database.first;
            auto&& workers      = database.second;

            size_t    const  numReplicas        = workers.size();
            string    const  numReplicasStr     = numReplicas ? to_string(numReplicas) : "";
            long long const  numReplicasDiff    = numReplicas - replicationLevel;
            string    const  numReplicasDiffStr = numReplicasDiff ? to_string(numReplicasDiff) : "";

            cout << " "   << setw(11) << chunkNumber
                 << " | " << setw(20) << databaseName
                 << " | " << setw(3)  << numReplicasStr
                 << " | " << setw(4)  << numReplicasDiffStr;

            if (_pullQservReplicas) {
                size_t const numQservReplicas =
                    qservReplicaData.useCount.chunkExists(chunkNumber) and
                    qservReplicaData.useCount.atChunk(chunkNumber).databaseExists(databaseName) ?
                        qservReplicaData.useCount.atChunk(chunkNumber).atDatabase(databaseName).size() :
                        0;

                string    const numQservReplicasStr     = numQservReplicas ? to_string(numQservReplicas) : "";
                long long const numQservReplicasDiff    = numQservReplicas - numReplicas;
                string    const numQservReplicasDiffStr = numQservReplicasDiff ? to_string(numQservReplicasDiff) : "";

                cout << " | " << setw(3)  << numQservReplicasStr
                     << " | " << setw(4)  << numQservReplicasDiffStr;
            } else {
                cout << " | " << setw(3)  << "*"
                     << " | " << setw(4)  << "*";
            }

            set<string> workerNames;
            for (auto&& name: workers.workerNames()) {
                workerNames.insert(name);
            }
            set<string> qservWorkerNames;
            if (qservReplicaData.useCount.chunkExists(chunkNumber) and
                qservReplicaData.useCount.atChunk(chunkNumber).databaseExists(databaseName)) {
                for (auto&& name: qservReplicaData.useCount
                                                  .atChunk(chunkNumber)
                                                  .atDatabase(databaseName)
                                                  .workerNames()) {
                    qservWorkerNames.insert(name);
                }
            }
            cout << " | " << ::workers2str(worker2idx, workerNames, qservWorkerNames) << "\n";
        }
    }
    cout << "-------------+----------------------+-----+------+-----+------+------------------------------------------\n"
         << endl;

    return 0;
}

}}} // namespace lsst::qserv::replica
