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
#include "replica/ClusterHealthApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/ClusterHealthJob.h"
#include "util/BlockPost.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application probes and reports a status of the Replication system's"
    " and Qserv workers to see if they respond within the specified (or implied)"
    " timeout.";


/**
 * 
 * @param caption
 *   Table caption
 *
 * @param workerResponded
 *   The collection of workers to be reported
 */
void printStatus(string const& caption,
                 map<string, bool> const& workerResponded) {

    namespace util = lsst::qserv::util;

    vector<string> columnWorker;
    vector<string> columnStatus;

    for (auto&& entry: workerResponded) {
        columnWorker.push_back(entry.first);
        columnStatus.push_back(entry.second ? "UP" : "*");
    }
    util::ColumnTablePrinter table(caption, "  ", false);

    table.addColumn("worker", columnWorker, util::ColumnTablePrinter::LEFT);
    table.addColumn("status", columnStatus, util::ColumnTablePrinter::LEFT);

    table.print(cout, false, false);
}

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ClusterHealthApp::Ptr ClusterHealthApp::create(int argc, char* argv[]) {
    return Ptr(
        new ClusterHealthApp(argc, argv)
    );
}


ClusterHealthApp::ClusterHealthApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().option(
        "timeout",
        "The timeout (seconds) for status requests sent to the Replication"
        " system's and Qserv workers.",
        _timeoutSec);

    parser().flag(
        "all-workers",
        "Extend a scope of the operation to probes all known workers instead of"
        " just the ENABLED ones.",
        _allWorkers);
}


int ClusterHealthApp::runImpl() {

    // Send probes to workers of both types

    atomic<bool> finished{false};
    auto const job = ClusterHealthJob::create(
        _timeoutSec,
        _allWorkers,
        Controller::create(serviceProvider()),
        string(),
        [&finished] (ClusterHealthJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }

    // Analyze and display results

    cout << "ClusterHealth job finished: " << job->state2string() << endl;

    if (job->extendedState() == Job::ExtendedState::SUCCESS) {

        auto&& health = job->clusterHealth();

        cout << endl;
        ::printStatus("REPLICATION WORKERS:", health.replication());
        cout << endl;
        ::printStatus("QSERV WORKERS:", health.qserv());
        cout << endl;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
