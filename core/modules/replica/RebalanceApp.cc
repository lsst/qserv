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
#include "replica/RebalanceApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/RebalanceJob.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description {
    "This application makes the best effort to ensure replicas are distributed"
    " equally among the worker nodes. And while doing so the re-balancing algorithm"
    " will both preserve the replication level of chunks and to keep the chunk"
    " collocation intact."
};

using namespace lsst::qserv::replica;

void printPlan(RebalanceJobResult const& r) {
    cout
        << "THE REBALANCE PLAN:\n"
        << "  totalWorkers:    " << r.totalWorkers    << "  (not counting workers which failed to report chunks)\n"
        << "  totalGoodChunks: " << r.totalGoodChunks << "  (good chunks reported by the precursor job)\n"
        << "  avgChunks:       " << r.avgChunks       << "\n"
        << "\n"
        << "--------+--------------------------+--------------------------\n"
        << "  chunk |            source worker |       destination worker \n"
        << "--------+--------------------------+--------------------------\n";

    for (auto const& chunkEntry: r.plan) {
        unsigned int const chunk = chunkEntry.first;
        for (auto const& sourceWorkerEntry: chunkEntry.second) {
            string const& sourceWorker      = sourceWorkerEntry.first;
            string const& destinationWorker = sourceWorkerEntry.second;
            cout
                << " " << setw(6)  << chunk             << " |"
                << " " << setw(24) << sourceWorker      << " |"
                << " " << setw(24) << destinationWorker << "\n";
        }
    }
    cout
        << "--------+--------------------------+--------------------------\n"
        << endl;
}

void printReplicaInfo(string const& collectionName,
                      RebalanceJobResult::ChunkDatabaseWorker const& collection) {
    cout
        << collectionName << ":\n"
        << "----------+----------+-----+-----------------------------------------\n"
        << "    chunk | database | rep | workers\n";

    unsigned int prevChunk = (unsigned int) -1;

    for (auto const& chunkEntry: collection) {

        unsigned int const& chunk = chunkEntry.first;
        for (auto const& databaseEntry: chunkEntry.second) {

            string const& database    = databaseEntry.first;
            size_t      const  numReplicas = databaseEntry.second.size();

            if (chunk != prevChunk)
                cout
                    << "----------+----------+-----+-----------------------------------------\n";

            prevChunk = chunk;

            cout
                << " "   << setw(8) << chunk
                << " | " << setw(8) << database
                << " | " << setw(3) << numReplicas
                << " | ";

            for (auto const& replicaEntry: databaseEntry.second) {

                string const& worker = replicaEntry.first;
                ReplicaInfo const& info   = replicaEntry.second;

                cout << worker << (info.status() != ReplicaInfo::Status::COMPLETE ? "(!)" : "") << " ";
            }
            cout << "\n";
        }
    }
    cout
        << "----------+----------+-----+-----------------------------------------\n"
        << endl;
}


} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

RebalanceApp::Ptr RebalanceApp::create(int argc,
                                       const char* const argv[]) {
    return Ptr(
        new RebalanceApp(
            argc,
            argv
        )
    );
}


RebalanceApp::RebalanceApp(int argc,
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
        "The name of a database family",
        _databaseFamily);

    parser().flag(
        "estimate-only",
        "Do not make any changes to chunk disposition. Just produce and print"
        " an estimated re-balancing plan.",
        _estimateOnly
    );
}


int RebalanceApp::runImpl() {

    // Once started this job will run indefinitely or until it fails and throws
    // an exception.

    atomic<bool> finished{false};
    auto const job = RebalanceJob::create(
        _databaseFamily,
        _estimateOnly,
        Controller::create(serviceProvider()),
        string(),
        [&finished] (RebalanceJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }

    // Analyze and display results

    RebalanceJobResult const& result = job->getReplicaData();

    ::printPlan(result);
    if (not _estimateOnly) {
        ::printReplicaInfo("CREATED REPLICAS", result.createdChunks);
        ::printReplicaInfo("DELETED REPLICAS", result.deletedChunks);
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
