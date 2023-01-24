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
#include "replica/TransactionsApp.h"

// System headers
#include <chrono>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/NamedMutexRegistry.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "replica/Mutex.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description = "This application is meant for testing persistent states of super-transaction";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = true;

vector<string> const transactionStates = {"IS_STARTING",  "STARTED",       "IS_FINISHING",
                                          "IS_ABORTING",  "FINISHED",      "ABORTED",
                                          "START_FAILED", "FINISH_FAILED", "ABORT_FAILED"};

}  // namespace

namespace lsst::qserv::replica {

TransactionsApp::Ptr TransactionsApp::create(int argc, char* argv[]) {
    return Ptr(new TransactionsApp(argc, argv));
}

TransactionsApp::TransactionsApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    vector<string> commands = {"FIND", "LIST", "CREATE", "UPDATE"};
    commands.insert(commands.end(), transactionStates.begin(), transactionStates.end());
    parser().commands("operation", commands, _operation)
            .option("tables-page-size",
                    "The number of rows in the table of a query result set (0 means no pages).",
                    _sqlPageSize);

    parser().command("FIND")
            .description("Find an existing transaction by its unique identifier.")
            .required("id", "A unique identifier of a transaction to be looked up for.", _id);

    parser().command("LIST")
            .description(
                    "Find existing transactions associated with a specific database (if provided)."
                    " If no database name is provided then all transactions will be reported")
            .optional("database", "The name of a database associated with a transaction(s).", _databaseName);

    parser().command("CREATE")
            .description(
                    "Create a new transaction in a scope of the specified database. Set a state of"
                    " the transaction to IS_STARTING. The command also allows to set the initial value"
                    " of the transaction's 'context' attribute.")
            .optional("database", "The name of a database to be associated with a new transaction.",
                      _databaseName);

    parser().command("UPDATE")
            .description(
                    "Update an existing transaction. The transaction will get a new state."
                    " The context attribute of the transaction will be updated as well if requested."
                    " Events may be also added to the transaction.")
            .required("id", "A unique identifier of an existing transaction.", _id)
            .required("state", "The new state of the transaction.", _state);
}

int TransactionsApp::runImpl() {
    string const context = "TransactionsApp::" + string(__func__) + " ";
    auto const service = serviceProvider()->databaseServices();

    if ("FIND" == _operation) {
        _print(service->transaction(_id));
    } else if ("LIST" == _operation) {
        _print(service->transactions(_databaseName));
    } else if ("CREATE" == _operation) {
        NamedMutexRegistry registry;
        unique_ptr<replica::Lock> lock;
        _print(service->createTransaction(_databaseName, registry, lock));
    } else if ("UPDATE" == _operation) {
        _print(service->updateTransaction(_id, _state));
    } else {
        throw logic_error(context + "unsupported operation: " + _operation);
    }
    return 0;
}

void TransactionsApp::_print(vector<TransactionInfo> const& collection) const {
    vector<uint32_t> colId;
    vector<string> colDatabase;
    vector<string> colState;
    vector<string> colBeginTime;
    vector<string> colStartTime;
    vector<string> colTransTime;
    vector<string> colEndTime;

    for (auto&& info : collection) {
        colId.push_back(info.id);
        colDatabase.push_back(info.database);
        colState.push_back(TransactionInfo::state2string(info.state));
        colStartTime.push_back(PerformanceUtils::toDateTimeString(chrono::milliseconds(info.beginTime)));
        colTransTime.push_back(PerformanceUtils::toDateTimeString(chrono::milliseconds(info.transitionTime)));
        colBeginTime.push_back(PerformanceUtils::toDateTimeString(chrono::milliseconds(info.startTime)));
        colEndTime.push_back(
                info.endTime == 0 ? ""
                                  : PerformanceUtils::toDateTimeString(chrono::milliseconds(info.endTime)));
    }

    util::ColumnTablePrinter table("SUPER-TRANSACTIONS:", "  ");

    table.addColumn("id", colId);
    table.addColumn("database", colDatabase, table.LEFT);
    table.addColumn("state", colState, table.LEFT);
    table.addColumn("begin time", colBeginTime, table.LEFT);
    table.addColumn("start time", colStartTime, table.LEFT);
    table.addColumn("trans time", colTransTime, table.LEFT);
    table.addColumn("end time", colEndTime, table.LEFT);

    cout << "\n";
    table.print(cout, false, false, _sqlPageSize);
    cout << "\n";
}

void TransactionsApp::_print(TransactionInfo const& info) const {
    vector<TransactionInfo> collection;
    collection.push_back(info);
    _print(collection);
}

}  // namespace lsst::qserv::replica
