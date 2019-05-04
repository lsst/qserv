/*
 * LSST Data Management System
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
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/ClusterHealthJob.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application probes and reports a status of the Replication system's"
    " and Qserv workers to see if they respond within the specified (or implied)"
    " timeout.";
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

    auto const job = ClusterHealthJob::create(
        _timeoutSec,
        _allWorkers,
        Controller::create(serviceProvider())
    );
    job->start();
    job->wait();

    // Analyze and display results

    cout << "ClusterHealth::" + string(__func__) + " job finished: " << job->state2string() << endl;

    if (job->extendedState() == Job::ExtendedState::SUCCESS) {

        auto&& health = job->clusterHealth();

        map<string, map<string,string>> worker2status;
        for (auto&& entry: health.qserv()) {
            worker2status[entry.first]["qserv"] = entry.second ? "UP" : "*";
        }
        for (auto&& entry: health.replication()) {
            worker2status[entry.first]["replication"] = entry.second ? "UP" : "*";
        }
        vector<string> columnWorker;
        vector<string> columnQserv;
        vector<string> columnReplica;
        for (auto&& entry: worker2status) {
            columnWorker .push_back(entry.first);
            columnQserv  .push_back(entry.second["qserv"]);
            columnReplica.push_back(entry.second["replication"]);
        }
        util::ColumnTablePrinter table("STATUS", "  ", false);

        table.addColumn("worker",      columnWorker,  util::ColumnTablePrinter::LEFT);
        table.addColumn("qserv",       columnQserv,   util::ColumnTablePrinter::LEFT);
        table.addColumn("replication", columnReplica, util::ColumnTablePrinter::LEFT);

        cout << endl;
        table.print(cout, false, false);
        cout << endl;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
