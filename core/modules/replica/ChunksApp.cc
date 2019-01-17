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
#include <set>
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
 *   '-' - the default character meaning no replica reported
 *   '*' - the worker didn't report any data due to a timeout or some other problem
 *   'R' - the worker is known to the replication system only
 *   'Q' - the worker is known to Qserv only
 *   'B' - the worker is known to both the Replication system and Qserv
 *
 * @param worker2idx - index map for workers (worker name to its 0-based index)
 * @param workers    - collection of worker names participating in the operation
 */
string workers2str(map<string, size_t> const& worker2idx,
                   set<string> const& workers,
                   set<string> const& badWorkers,
                   set<string> const& qservWorkers,
                   set<string> const& badQservWorkers) {

    // Prepare a blank line using symbols '--' as a placeholder for workers
    // at the relative 0-based positions. The last placeholder is intentionally
    // shorter by 1 character to avoid leaving the trailing white space character.

    string result(3*worker2idx.size() - 1, ' ');
    for (size_t idx = 0, num = worker2idx.size(); idx < num; ++idx) {
        result[3*idx]   = '-';
        result[3*idx+1] = '-';
    }

    // Fill-in participating workers at their positions in the line

    for (auto const& worker: workers) {
        result[3*worker2idx.at(worker)] = 'R';
    }
    for (auto const& worker: badWorkers) {
        result[3*worker2idx.at(worker)] = '*';
    }
    for (auto const& worker: qservWorkers) {
        result[3*worker2idx.at(worker)+1] = 'Q';
    }
    for (auto const& worker: badQservWorkers) {
        result[3*worker2idx.at(worker)+1] = '*';
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
        "worker-response-timeout",
        "maximum timeout (seconds) to wait before the replica scanning requests will finish."
        " Setting this timeout to some reasonably low umber would prevent the application from"
        " hanging for a substantial duration of time (which depends on the default Configuration)"
        " in case if some workers were down. The parameter applies to operations with both"
        " the Replication and Qserv workers.",
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

    // Limit request execution time if such limit was provided
    if (_timeoutSec != 0) {
        serviceProvider()->config()->setControllerRequestTimeoutSec(_timeoutSec);
        //serviceProvider()->config()->setJobTimeoutSec(_timeoutSec);
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
             << "   QservGetReplicasJob finished: " << qservGetReplicasJob->state2string() << "\n";
    } else {
        while (not replicaJobFinished) {
            blockPost.wait();
        }
        cout << "qserv-replica-job-chunks:\n"
             << "   FindAllJob          finished: " << findAllJob->state2string() << "\n";
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

    // Build a map of worker "numbers" to use them instead of (potentially) very long
    // worker identifiers

    map<string, size_t> worker2idx;
    for (size_t idx = 0, num = workers.size(); idx < num; ++idx) {
        string const& worker = workers[idx];
        worker2idx[worker] = idx;
    }

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

    // Remember bad workers

    set<string> badWorkers;
    set<string> badQservWorkers;
    for (auto const& worker: workers) {
        if (not replicaData.workers.at(worker)) badWorkers.insert(worker);
        if (_pullQservReplicas and not qservReplicaData.workers.at(worker)) badQservWorkers.insert(worker);
    }

    // Print a summary table with the number of chunks across both types
    // of workers.
    {
        vector<size_t> workerIdx;
        vector<string> workerName;
        vector<string> numReplicas;
        vector<string> numQservReplicas;

        for (auto const& worker: workers) {

            workerIdx.push_back(worker2idx[worker]);
            workerName.push_back(worker);

            numReplicas.push_back(
                replicaData.workers.at(worker) ?
                    to_string(worker2numChunks[worker]) :
                    "*");

            numQservReplicas.push_back(
                _pullQservReplicas and qservReplicaData.workers.at(worker) ?
                    to_string(qservWorker2numChunks[worker]) :
                    "*");
        }
        util::ColumnTablePrinter table("NUMBER OF CHUNKS REPORTED BY WORKERS:", "  ", _verticalSeparator);

        table.addColumn("idx",         workerIdx);
        table.addColumn("worker",      workerName, util::ColumnTablePrinter::LEFT);
        table.addColumn("Replication", numReplicas);
        table.addColumn("Qserv",       numQservReplicas);

        cout << "\n";
        table.print(cout, false, false);
    }

    // Print a table with the replica disposition for known chunks and databases
    // across both types of workers.
    {
        vector<unsigned int> columnChunkNumber;
        vector<string>       columnDatabaseName;
        vector<string>       columnNumReplicas;
        vector<string>       columnNumReplicasDiff;
        vector<string>       columnNumQservReplicas;
        vector<string>       columnNumQservReplicasDiff;
        vector<string>       columnReplicasAtWorkers;

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

                columnChunkNumber    .push_back(chunkNumber);
                columnDatabaseName   .push_back(databaseName);
                columnNumReplicas    .push_back(numReplicasStr);
                columnNumReplicasDiff.push_back(numReplicasDiffStr);

                string numQservReplicasStr = "*";
                string numQservReplicasDiffStr = "*";

                if (_pullQservReplicas) {

                    size_t const numQservReplicas =
                        qservReplicaData.useCount.chunkExists(chunkNumber) and
                        qservReplicaData.useCount.atChunk(chunkNumber).databaseExists(databaseName) ?
                            qservReplicaData.useCount.atChunk(chunkNumber).atDatabase(databaseName).size() :
                            0;

                    numQservReplicasStr = numQservReplicas ? to_string(numQservReplicas) : "";

                    long long const numQservReplicasDiff = numQservReplicas - numReplicas;
                    numQservReplicasDiffStr = numQservReplicasDiff ? to_string(numQservReplicasDiff) : "";
                }
                columnNumQservReplicas    .push_back(numQservReplicasStr);
                columnNumQservReplicasDiff.push_back(numQservReplicasDiffStr);

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
                columnReplicasAtWorkers.push_back(::workers2str(
                    worker2idx,
                    workerNames,
                    badWorkers,
                    qservWorkerNames,
                    badQservWorkers
                ));
            }
        }
        util::ColumnTablePrinter table("REPLICAS (desired 'L'evel, 'R'eplication, 'Q'serv):", "  ", _verticalSeparator);

        table.addColumn("chunk",               columnChunkNumber);
        table.addColumn("database",            columnDatabaseName, util::ColumnTablePrinter::LEFT);
        table.addColumn("  R",                 columnNumReplicas);
        table.addColumn("R-L",                 columnNumReplicasDiff);
        table.addColumn("  Q",                 columnNumQservReplicas);
        table.addColumn("Q-R",                 columnNumQservReplicasDiff);
        table.addColumn("replicas at workers", columnReplicasAtWorkers, util::ColumnTablePrinter::LEFT);

        cout << "\n";
        table.print(cout, false, false, _pageSize, _pageSize != 0);
    }
    cout << endl;

    return 0;
}

}}} // namespace lsst::qserv::replica
