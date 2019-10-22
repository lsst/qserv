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
#include "replica/AbortTransactionApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/AbortTransactionJob.h"

using namespace std;

namespace {

string const description =
    "This application aborts a transaction by dropping MySQL table partitions"
    " corresponding to the transaction at the relevant worker databases."
    " And while doing so, the application will make the best effort to leave"
    " worker nodes as balanced as possible.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

AbortTransactionApp::Ptr AbortTransactionApp::create(int argc, char* argv[]) {
    return Ptr(new AbortTransactionApp(argc, argv));
}


AbortTransactionApp::AbortTransactionApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().required(
        "transaction",
        "The identifier of a super-transaction which must be in the ABORTED state."
        " A database which is associated with the transaction should not be PUBLISHED yet.",
        _transactionId);

    parser().flag(
        "all-workers",
        "The flag will include all known workers (not just ENABLED) into the operation.",
        _allWorkers);
}


int AbortTransactionApp::runImpl() {

    auto const job = AbortTransactionJob::create(
        _transactionId,
        _allWorkers,
        Controller::create(serviceProvider())
    );
    job->start();
    job->wait();

    // TODO: Analyze and display results

    return 0;
}

}}} // namespace lsst::qserv::replica
