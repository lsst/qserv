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
#include "replica/SyncApp.h"

// System headers
#include <atomic>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/QservSyncJob.h"
#include "util/BlockPost.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application synchronizes collections of chunks at the Qserv workers"
    " with what the Replication system sees as 'good' chunks in the data directories.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

SyncApp::Ptr SyncApp::create(int argc, char* argv[]) {
    return Ptr(
        new SyncApp(argc, argv)
    );
}


SyncApp::SyncApp(int argc, char* argv[])
    :   Application(
            argc, argv,
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

    parser().option(
        "worker-response-timeout",
        "The maximum timeout (seconds) to wait before worker requests will finish."
        " Setting this timeout to some reasonably low number would prevent the application from"
        " hanging for a substantial duration of time (which depends on the default Configuration)"
        " in case if some workers were down. The parameter applies to operations with both"
        " the Replication and Qserv workers.",
        _timeoutSec);

    parser().flag(
        "force",
        "Force the Qerv workers to proceed with requested chunk updates regardless of the chunk"
        " usage status.",
        _force);
}


int SyncApp::runImpl() {

    // Run the synchronization algorithm

    atomic<bool> finished{false};
    auto const job = QservSyncJob::create(
        _databaseFamily,
        _timeoutSec,
        _force,
        Controller::create(serviceProvider()),
        std::string(),
        [&finished] (replica::QservSyncJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }

    // Analyze and display results

    QservSyncJobResult const& replicaData = job->getReplicaData();

    vector<string> columnWorker;
    vector<string> columnNumPrevChunks;
    vector<string> columnNumNewChunks;

    for (auto&& workerEntry: replicaData.workers) {

        std::string const& worker    = workerEntry.first;
        bool        const  succeeded = workerEntry.second;

        columnWorker       .push_back(worker);
        columnNumPrevChunks.push_back(succeeded ? to_string(replicaData.prevReplicas.at(worker).size()) : "FAILED");
        columnNumNewChunks .push_back(succeeded ? to_string(replicaData.newReplicas .at(worker).size()) : "FAILED");
    }
    util::ColumnTablePrinter table("CHUNK DISTRIBUTION:", "  ", false);

    table.addColumn("worker",       columnWorker, util::ColumnTablePrinter::LEFT);
    table.addColumn("prev #chunks", columnNumPrevChunks);
    table.addColumn("new #chunks",  columnNumNewChunks);

    cout << "\n";
    table.print(cout, false, false);

    return 0;
}

}}} // namespace lsst::qserv::replica
