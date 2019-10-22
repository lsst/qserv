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
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This application is meant for testing persistent states of super-transaction";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

TransactionsApp::Ptr TransactionsApp::create(int argc, char* argv[]) {
    return Ptr(
        new TransactionsApp(argc, argv)
    );
}


TransactionsApp::TransactionsApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            false   /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "operation",
        {   "FIND",
            "LIST",
            "BEGIN",
            "END"
        },
        _operation);

    parser().option(
        "tables-page-size",
        "The number of rows in the table of a query result set (0 means no pages).",
        _sqlPageSize);

    auto& findCmd = parser().command("FIND");

    findCmd.description(
        "Find an existing transaction by its unique identifier.");

    findCmd.required(
        "id",
        "A unique identifier of a transaction to be looked up for.",
        _id);

    auto& listCmd = parser().command("LIST");

    listCmd.description(
        "Find existing transactions associated with a specific database (if provided)."
        " If no database name is provided then all transactions will be reported");

    listCmd.optional(
        "database",
        "The name of a database associated with a transaction(s).",
        _databaseName);

    auto& beginCmd = parser().command("BEGIN");

    beginCmd.description(
        "Begin a new transaction in a scope of the specified database.");

    beginCmd.optional(
        "database",
        "The name of a database to be associated with a new transaction.",
        _databaseName);

    auto& endCmd = parser().command("END");

    endCmd.description(
        "End normally or abnormally (depending on a presence of an optional flag"
        " an existing transaction.");

    endCmd.required(
        "id",
        "A unique identifier of a transaction to be ended.",
        _id);

    endCmd.flag(
        "abort",
        "Abort the transaction",
        _abort
    );
}


int TransactionsApp::runImpl() {

    string const context = "TransactionsApp::" + string(__func__) + " ";

    auto const service = serviceProvider()->databaseServices();

    if      ("FIND"   == _operation) { _print(service->transaction(_id)); }
    else if ("LIST"   == _operation) { _print(service->transactions(_databaseName)); }
    else if ("BEGIN"  == _operation) { _print(service->beginTransaction(_databaseName)); }
    else if ("END"    == _operation) { _print(service->endTransaction(_id, _abort)); }
    else { throw logic_error(context + "unsupported operation: " + _operation); }

    return 0;
}


void TransactionsApp::_print(vector<TransactionInfo> const& collection) const {

    vector<uint32_t> colId;
    vector<string>   colDatabase;
    vector<string>   colState;
    vector<string>   colBeginTime;
    vector<string>   colEndTime;

    for (auto&& info: collection) {
        colId       .push_back(info.id);
        colDatabase .push_back(info.database);
        colState    .push_back(info.state);
        colBeginTime.push_back(PerformanceUtils::toDateTimeString(chrono::milliseconds(info.beginTime)));
        colEndTime  .push_back(info.endTime == 0 ? "" : PerformanceUtils::toDateTimeString(chrono::milliseconds(info.endTime)));
    }

    util::ColumnTablePrinter table("SUPER-TRANSACTIONS:", "  ");

    table.addColumn("id",         colId);
    table.addColumn("database",   colDatabase,  table.LEFT);
    table.addColumn("state",      colState,     table.LEFT);
    table.addColumn("begin time", colBeginTime, table.LEFT);
    table.addColumn("end time",   colEndTime,   table.LEFT);

    cout << "\n";
    table.print(cout, false, false, _sqlPageSize);
    cout << "\n";
}


void TransactionsApp::_print(TransactionInfo const& info) const {
    vector<TransactionInfo> collection;
    collection.push_back(info);
    _print(collection);
}

}}} // namespace lsst::qserv::replica
