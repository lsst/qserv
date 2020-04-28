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
#include "replica/RebalanceApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/RebalanceJob.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

string const description =
    "This application makes the best effort to ensure replicas are distributed"
    " equally among the worker nodes. And while doing so the re-balancing algorithm"
    " will both preserve the replication level of chunks and to keep the chunk"
    " collocation intact.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;
bool const injectXrootdOptions = true;

void printPlan(RebalanceJobResult const& r) {

    cout << "\nTHE REBALANCE PLAN:\n"
         << "  totalWorkers:    " << r.totalWorkers    << "  (not counting workers which failed to report chunks)\n"
         << "  totalGoodChunks: " << r.totalGoodChunks << "  (good chunks reported by the precursor job)\n"
         << "  avgChunks:       " << r.avgChunks       << "\n"
         << "\n";

    vector<unsigned int> columnChunk;
    vector<string>       columnSourceWorker;
    vector<string>       columnDestinationWorker;

    for (auto&& chunkEntry: r.plan) {
        auto const chunkNumber = chunkEntry.first;

        for (auto&& sourceWorkerEntry: chunkEntry.second) {
            auto&& sourceWorker      = sourceWorkerEntry.first;
            auto&& destinationWorker = sourceWorkerEntry.second;

            columnChunk            .push_back(chunkNumber);
            columnSourceWorker     .push_back(sourceWorker);
            columnDestinationWorker.push_back(destinationWorker);
        }
    }
    util::ColumnTablePrinter table("", "  ", false);

    table.addColumn("chunk",              columnChunk );
    table.addColumn("source worker",      columnSourceWorker,      util::ColumnTablePrinter::LEFT);
    table.addColumn("destination worker", columnDestinationWorker, util::ColumnTablePrinter::LEFT);

    table.print(cout, false, false);

    cout << endl;
}

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

RebalanceApp::Ptr RebalanceApp::create(int argc, char* argv[]) {
    return Ptr(
        new RebalanceApp(argc, argv)
    );
}


RebalanceApp::RebalanceApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider,
            injectXrootdOptions
        ) {

    // Configure the command line parser

    parser().required(
        "database-family",
        "The name of a database family.",
        _databaseFamily);

    parser().flag(
        "estimate-only",
        "Do not make any changes to chunk disposition. Just produce and print"
        " an estimated re-balancing plan.",
        _estimateOnly
    );

    parser().option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize);
}


int RebalanceApp::runImpl() {

    auto const job = RebalanceJob::create(
        _databaseFamily,
        _estimateOnly,
        Controller::create(serviceProvider())
    );
    job->start();
    job->wait();

    auto&& jobResult = job->getReplicaData();

    cout << "\n";
    ::printPlan(jobResult);
    if (not _estimateOnly) {
        cout << "\n";
        replica::printAsTable("CREATED REPLICAS", "  ", jobResult.createdChunks, cout, _pageSize);
        cout << "\n";
        replica::printAsTable("DELETED REPLICAS", "  ", jobResult.deletedChunks, cout, _pageSize);
    }
    cout << "\n";
    return 0;
}

}}} // namespace lsst::qserv::replica
