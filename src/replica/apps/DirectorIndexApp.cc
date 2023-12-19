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
#include "replica/apps/DirectorIndexApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <set>
#include <vector>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/contr/Controller.h"
#include "replica/jobs/DirectorIndexJob.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

string const description =
        "This is a Controller application which launches a single job Controller in order"
        " to harvest the 'director' index data from the 'director' tables of a select"
        " database and load these data into the corresponidng 'director' index table."
        " Maximum timeout (seconds) to wait before the index data extraction requests sent"
        " to workers will finish should be set via option --controller--request-timeout-sec."
        " Setting this timeout to some reasonably low number would prevent the application from"
        " hanging for a substantial duration of time in case if some workers were down.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

DirectorIndexApp::Ptr DirectorIndexApp::create(int argc, char* argv[]) {
    return Ptr(new DirectorIndexApp(argc, argv));
}

DirectorIndexApp::DirectorIndexApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().required("database", "The name of a database.", _database)
            .required("table", "The name of the director table.", _table)
            .option("transaction",
                    "An identifier of a super-transaction corresponding to a MySQL partition of the"
                    " 'director' table. If the option isn't used then the complete content of"
                    " the table will be scanned, and the scan won't include the super-transaction"
                    " column 'qserv_trans_id'.",
                    _transactionId)
            .flag("all-workers",
                  "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY).",
                  _allWorkers)
            .option("qserv-czar-db", "A connection URL to the MySQL server of the Qserv master database.",
                    _qservCzarDbUrl)
            .flag("detailed-report",
                  "The flag triggering detailed report on the harvested 'director' index data."
                  " The report will also include MySQL errors (f any) for each chunk.",
                  _detailedReport)
            .option("tables-page-size", "The number of rows in the table of chunks (0 means no pages).",
                    _pageSize)
            .flag("tables-vertical-separator",
                  "Print vertical separator when displaying tabular data in reports.", _verticalSeparator);
}

int DirectorIndexApp::runImpl() {
    if (!_qservCzarDbUrl.empty()) {
        // IMPORTANT: set the connector, then clear it up to avoid
        // contaminating the log files when logging command line arguments
        // parsed by the application.
        Configuration::setQservCzarDbUrl(_qservCzarDbUrl);
        _qservCzarDbUrl = "******";
    }
    auto const controller = Controller::create(serviceProvider());

    string const noParentJobId;
    auto const job = DirectorIndexJob::create(_database, _table,
                                              _transactionId != numeric_limits<TransactionId>::max(),
                                              _transactionId, _allWorkers, controller, noParentJobId,
                                              nullptr,  // no callback
                                              PRIORITY_NORMAL);
    job->start();
    job->wait();

    vector<string> columnWorker;
    vector<string> columnChunk;
    vector<string> columnError;

    for (auto&& workerItr : job->getResultData().error) {
        auto&& worker = workerItr.first;
        for (auto&& chunkItr : workerItr.second) {
            auto&& chunk = chunkItr.first;
            auto&& error = chunkItr.second;
            if (not error.empty()) {
                columnWorker.push_back(worker);
                columnChunk.push_back(to_string(chunk));
                columnError.push_back(error);
            }
        }
    }
    util::ColumnTablePrinter table("RESULTS FOR CHUNKS", "  ", _verticalSeparator);
    table.addColumn("worker", columnWorker, util::ColumnTablePrinter::LEFT);
    table.addColumn("chunk", columnChunk);
    table.addColumn("error", columnError, util::ColumnTablePrinter::LEFT);

    cout << "\n";
    table.print(cout, false, false, _pageSize, _pageSize != 0);

    return 0;
}

}  // namespace lsst::qserv::replica
