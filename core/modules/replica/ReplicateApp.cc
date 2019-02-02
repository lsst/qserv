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
#include "replica/ReplicateApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/ReplicateJob.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application analyzes the replication level for all chunks of a given"
    " database family and brings the number of replicas up to the explicitly specified"
    " (via the corresponding option) or implied (as per the site Configuration)"
    " minimum level. Chunks which already have the desired replication level won't"
    " be affected by the operation.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ReplicateApp::Ptr ReplicateApp::create(int argc, char* argv[]) {
    return Ptr(
        new ReplicateApp(argc, argv)
    );
}


ReplicateApp::ReplicateApp(int argc, char* argv[])
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
        "The name of a database family.",
        _databaseFamily);

    parser().option(
        "replicas",
        "The minimum number of replicas to be guaranteed for each chunk (leaving"
        " it to the default value 0 will pull the actual value of the parameter"
        " from the Configuration).",
        _replicas
    );

    parser().option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize);
}


int ReplicateApp::runImpl() {

    atomic<bool> finished{false};
    auto const job = ReplicateJob::create(
        _databaseFamily,
        _replicas,
        Controller::create(serviceProvider()),
        string(),
        [&finished] (ReplicateJob::Ptr const& job) {
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
    replica::printAsTable("CREATED REPLICAS", "  ", job->getReplicaData().chunks, cout, _pageSize);
    cout << "\n";

    return 0;
}

}}} // namespace lsst::qserv::replica
