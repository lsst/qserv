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
#include "replica/FixUpApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/FixUpJob.h"

using namespace std;

namespace {

string const description =
    "This application finds and corrects various problems with replicas in a scope"
    " of a database family. And while doing so, the application will make the best"
    " effort to leave worker nodes as balanced as possible, and it will also preserve"
    " chunk collocation.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;
bool const injectXrootdOptions = true;

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

FixUpApp::Ptr FixUpApp::create(int argc, char* argv[]) {
    return Ptr(
        new FixUpApp(argc, argv)
    );
}


FixUpApp::FixUpApp(int argc, char* argv[])
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
        _databaseFamily
    ).option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize
    );
}


int FixUpApp::runImpl() {

    string const noParentJobId;
    auto const job = FixUpJob::create(
        _databaseFamily,
        Controller::create(serviceProvider()),
        noParentJobId,
        nullptr,    // no callback
        PRIORITY_NORMAL
    );
    job->start();
    job->wait();

    // Analyze and display results

    cout << "\n";
    replica::printAsTable("CREATED REPLICAS", "  ", job->getReplicaData().chunks, cout, _pageSize);
    cout << "\n";

    return job->extendedState() == Job::SUCCESS ? 0 : 1;
}

}}} // namespace lsst::qserv::replica
