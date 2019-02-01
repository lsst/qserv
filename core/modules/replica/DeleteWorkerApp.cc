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
#include "replica/DeleteWorkerApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/DeleteWorkerJob.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description {
    "This application disable a worker from any active use in a replication setup."
    " All chunks hosted by  the worker node will be distributed across the cluster."
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

DeleteWorkerApp::Ptr DeleteWorkerApp::create(int argc,
                                             const char* const argv[]) {
    return Ptr(
        new DeleteWorkerApp(
            argc,
            argv
        )
    );
}


DeleteWorkerApp::DeleteWorkerApp(int argc,
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
        "worker",
        "The name of a worker to be deleted",
        _workerName);

    parser().flag(
        "permanent-delete",
        "Permanently delete a worker from the Configuration",
        _permanentDelete);

    parser().option(
        "tables-page-size",
        "the number of rows in the table of replicas (0 means no pages)",
        _pageSize);
}


int DeleteWorkerApp::runImpl() {

    atomic<bool> finished{false};
    auto const job = DeleteWorkerJob::create(
        _workerName,
        _permanentDelete,
        Controller::create(serviceProvider()),
        string(),
        [&finished] (DeleteWorkerJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }

    // Analyze and display results

    cout << "\n";
    replica::printAsTable("NEWELY CREATED CHUNKS", "  ", job->getReplicaData().chunks, cout, _pageSize);
    cout << "\n";
    replica::printAsTable("ORPHAN CHUNKS", "  ", job->getReplicaData().orphanChunks, cout, _pageSize);
    cout << "\n";

    return 0;
}

}}} // namespace lsst::qserv::replica
