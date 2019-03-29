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
#include "replica/SqlApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/SqlResultSet.h"
#include "replica/SqlJob.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application executes the same query against worker databases of"
    " select workers. Result sets will be reported upon a completion of"
    " the application.";

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

SqlApp::Ptr SqlApp::create(int argc, char* argv[]) {
    return Ptr(
        new SqlApp(argc, argv)
    );
}


SqlApp::SqlApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().required(
        "user",
        "Worker-side MySQL user account for executing the query.",
        _mysqlUser);

    parser().required(
        "password",
        "Password for the MySQL account.",
        _mysqlPassword);

    parser().required(
        "query",
        "The query to be executed on all select workers",
        _query);

    parser().flag(
        "all-workers",
        "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY).",
        _allWorkers);

    parser().option(
        "max-rows",
        "The maximum number of rows to be pulled from result set at workers"
        " when processing queries. NOTE: This parameter has nothing to do with"
        " the SQL's 'LIMIT <num-rows>'. It serves as an additional fail safe"
        " mechanism preventing protocol buffers from being overloaded by huge"
        " result sets which might be accidentally initiated by users.",
        _maxRows);

    parser().option(
        "worker-response-timeout",
        "Maximum timeout (seconds) to wait before queries would finish."
        " Setting this timeout to some reasonably low number would prevent the application from"
        " hanging for a substantial duration of time (which depends on the default Configuration)"
        " in case if some workers were down. The parameter applies to operations with"
        " the Replication workers.",
        _timeoutSec);

    parser().option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize);
}


int SqlApp::runImpl() {

    // Limit request execution time if such limit was provided
    if (_timeoutSec != 0) {
        serviceProvider()->config()->setControllerRequestTimeoutSec(_timeoutSec);
    }

    atomic<bool> finished{false};
    auto const job = SqlJob::create(
        _query,
        _mysqlUser,
        _mysqlPassword,
        _maxRows,
        _allWorkers,
        Controller::create(serviceProvider()),
        string(),
        [&finished] (SqlJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }

    // Analyze and display results for each worker

    auto const& resultData = job->getResultData();
    for (auto&& itr : resultData.resultSets) {
        auto&& worker = itr.first;
        auto&& resultSet = itr.second;

        bool const succeeded = resultData.workers.at(worker);
        if (not succeeded) {
            cout << "worker: " << worker << ",  error: " << resultSet.error << endl;
            continue;
        }
        string const caption =
            "worker: " + worker + ",  performance [sec]: " + to_string(resultSet.performanceSec);
        string const indent = "";

        auto table = resultSet.toColumnTable(caption, indent);

        bool const topSeparator    = false;
        bool const bottomSeparator = false;
        bool const repeatedHeader  = false;

        table.print(cout, topSeparator, bottomSeparator, _pageSize, repeatedHeader);
        cout << "\n";
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
