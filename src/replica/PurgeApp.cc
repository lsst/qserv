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
#include "replica/PurgeApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/PurgeJob.h"

using namespace std;

namespace {

string const description =
        "This application purges excess replicas for all chunks of"
        " a database family down to the minimally required replication level. And while"
        " doing so, the application will make the best effort to leave worker nodes as"
        " balanced as possible, and it will also preserve chunk collocation.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

PurgeApp::Ptr PurgeApp::create(int argc, char* argv[]) { return Ptr(new PurgeApp(argc, argv)); }

PurgeApp::PurgeApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().required("database-family", "The name of a database family", _databaseFamily)
            .option("replicas",
                    "The maximum number of replicas to be left for each chunk (leaving"
                    " it to the default value 0 will pull the actual value of the parameter"
                    " from the Configuration).",
                    _replicas)
            .option("tables-page-size", "The number of rows in the table of replicas (0 means no pages).",
                    _pageSize);
}

int PurgeApp::runImpl() {
    string const noParentJobId;
    auto const job =
            PurgeJob::create(_databaseFamily, _replicas, Controller::create(serviceProvider()), noParentJobId,
                             nullptr,  // no callback
                             PRIORITY_NORMAL);
    job->start();
    job->wait();

    cout << "\n";
    replica::printAsTable("DELETED REPLICAS", "  ", job->getReplicaData().chunks, cout, _pageSize);
    cout << "\n";

    return job->extendedState() == Job::SUCCESS ? 0 : 1;
}

}  // namespace lsst::qserv::replica
