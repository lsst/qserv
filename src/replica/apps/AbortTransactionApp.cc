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
#include "replica/apps/AbortTransactionApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <map>
#include <vector>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/contr/Controller.h"
#include "replica/jobs/AbortTransactionJob.h"
#include "replica/services/ServiceProvider.h"

using namespace std;

namespace {

string const description =
        "This application aborts a transaction by dropping MySQL table partitions"
        " corresponding to the transaction at the relevant worker databases."
        " And while doing so, the application will make the best effort to leave"
        " worker nodes as balanced as possible.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

AbortTransactionApp::Ptr AbortTransactionApp::create(int argc, char* argv[]) {
    return Ptr(new AbortTransactionApp(argc, argv));
}

AbortTransactionApp::AbortTransactionApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().required("transaction",
                      "The identifier of a super-transaction which must be in the ABORTED state."
                      " A database which is associated with the transaction should not be PUBLISHED yet.",
                      _transactionId)
            .flag("all-workers", "The flag includes all known workers (not just ENABLED) into the operation.",
                  _allWorkers)
            .option("report-level",
                    "The option which controls the verbosity of the job completion report."
                    " Supported report levels:"
                    " 0: no report, just return the completion status to the shell."
                    " 1: report a summary, including the job completion status, the number"
                    " of tables failed to be processed, as well as the number of tables"
                    " which have been successfully processed."
                    " 2: report processing status of each table failed to be processed by the operation."
                    " The result will include the name of the table, the name of a worker on which"
                    " the table was expected to be residing, the completion status of"
                    " the operation, and an error message (if any) reported by the remote"
                    " worker service. Results will be presented in a tabular format with a row"
                    " per each table involved into the operation."
                    " 3: also include into the report all tables which were successfully"
                    " processed by the operation.",
                    _reportLevel);
}

int AbortTransactionApp::runImpl() {
    string const noParentJobId;
    auto const job = AbortTransactionJob::create(_transactionId, _allWorkers,
                                                 Controller::create(serviceProvider()), noParentJobId,
                                                 nullptr,  // no callback
                                                 PRIORITY_NORMAL);
    job->start();
    job->wait();

    if (_reportLevel > 0) {
        cout << "Job completion status: " << Job::state2string(job->extendedState()) << endl;

        auto&& resultData = job->getResultData();
        size_t numSucceeded = 0;
        map<ProtocolStatusExt, size_t> numFailed;
        resultData.iterate([&numFailed, &numSucceeded](SqlJobResult::Worker const& worker,
                                                       SqlJobResult::Scope const& table,
                                                       SqlResultSet::ResultSet const& resultSet) {
            if (resultSet.extendedStatus == ProtocolStatusExt::NONE) {
                numSucceeded++;
            } else {
                numFailed[resultSet.extendedStatus]++;
            }
        });
        cout << "Table processing summary:\n"
             << "  succeeded: " << numSucceeded << "\n";
        if (numFailed.size() == 0) {
            cout << "  failed: " << 0 << endl;
        } else {
            cout << "  failed:\n";
            for (auto&& entry : numFailed) {
                auto const extendedStatus = entry.first;
                auto const counter = entry.second;
                cout << "    " << status2string(extendedStatus) << ": " << counter << endl;
            }
        }
        if (_reportLevel > 1) {
            string const caption = "Tables results:";
            string const indent = "";
            bool const verticalSeparator = true;
            bool const reportAll = _reportLevel > 2;
            auto tablePrinter =
                    resultData.toColumnTable(caption, indent, verticalSeparator, reportAll, "table");

            bool const topSeparator = false;
            bool const bottomSeparator = false;
            size_t const pageSize = 0;
            bool const repeatedHeader = false;
            tablePrinter.print(cout, topSeparator, bottomSeparator, pageSize, repeatedHeader);
        }
    }
    return job->extendedState() == Job::SUCCESS ? 0 : 1;
}

}  // namespace lsst::qserv::replica
