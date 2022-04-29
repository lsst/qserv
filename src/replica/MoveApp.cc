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
#include "replica/MoveApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/MoveReplicaJob.h"
#include "replica/ReplicaInfo.h"

using namespace std;

namespace {

string const description =
        "This application makes the best effort to ensure replicas are distributed"
        " equally among the worker nodes. And while doing so the re-balancing algorithm"
        " will both preserve the replication level of chunks and to keep the chunk"
        " collocation intact.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst { namespace qserv { namespace replica {

MoveApp::Ptr MoveApp::create(int argc, char* argv[]) { return Ptr(new MoveApp(argc, argv)); }

MoveApp::MoveApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().required("database-family", "The name of a database family.", _databaseFamily)
            .required("chunk", "The chunk to be affected by the operation.", _chunk)
            .required("source-worker", "The name of a worker which has the replica to be moved.",
                      _sourceWorker)
            .required("destination-worker",
                      "The name of a worker where the replica will be moved (must not"
                      " be the same worker as the source one).",
                      _destinationWorker)
            .flag("purge",
                  "Purge the input replica at the source worker upon a successful"
                  " completion of the operation.",
                  _purge)
            .option("tables-page-size", "The number of rows in the table of replicas (0 means no pages).",
                    _pageSize);
}

int MoveApp::runImpl() {
    string const noParentJobId;
    auto const job = MoveReplicaJob::create(_databaseFamily, _chunk, _sourceWorker, _destinationWorker,
                                            _purge, Controller::create(serviceProvider()), noParentJobId,
                                            nullptr,  // no callback
                                            PRIORITY_NORMAL);
    job->start();
    job->wait();

    // Analyze and display results

    auto&& jobResult = job->getReplicaData();

    cout << "\n";
    replica::printAsTable("CREATED REPLICAS", "  ", jobResult.createdChunks, cout, _pageSize);
    cout << "\n";
    replica::printAsTable("DELETED REPLICAS", "  ", jobResult.deletedChunks, cout, _pageSize);
    cout << "\n";

    return 0;
}

}}}  // namespace lsst::qserv::replica
