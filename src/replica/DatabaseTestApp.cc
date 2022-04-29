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
#include "replica/DatabaseTestApp.h"

// System headers
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
#include "util/TablePrinter.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

string const description =
        "This application is for testing the DatabaseServices API used by"
        " the Replication system implementation.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = true;

string asString(time_t t) {
    ostringstream ss;
    ss << put_time(localtime(&t), "%F %T");
    return ss.str();
}

string asString(uint64_t ms) {
    // Milliseconds since Epoch
    chrono::time_point<chrono::system_clock> tp((chrono::milliseconds(ms)));
    // Convert to system time
    time_t t = chrono::system_clock::to_time_t(tp);
    return asString(t);
}

string asStringIf(uint64_t ms) { return 0 == ms ? "" : asString(ms); }

void dump(vector<ReplicaInfo> const& replicas) {
    for (auto&& r : replicas) {
        cout << "\n"
             << " ------------------ REPLICA ------------------\n"
             << "\n"
             << "             chunk: " << r.chunk() << "\n"
             << "          database: " << r.database() << "\n"
             << "            worker: " << r.worker() << "\n"
             << "            status: " << ReplicaInfo::status2string(r.status()) << "\n"
             << "        verifyTime: " << asStringIf(r.verifyTime()) << "\n"
             << " beginTransferTime: " << asStringIf(r.beginTransferTime()) << "\n"
             << "   endTransferTime: " << asStringIf(r.endTransferTime()) << "\n";
        for (auto&& f : r.fileInfo()) {
            cout << "\n"
                 << "              name: " << f.name << "\n"
                 << "              size: " << f.size << "\n"
                 << "             mtime: " << asString(f.mtime) << "\n"
                 << "                cs: " << f.cs << "\n"
                 << "            inSize: " << f.inSize << "\n"
                 << " beginTransferTime: " << asStringIf(f.beginTransferTime) << "\n"
                 << "   endTransferTime: " << asStringIf(f.endTransferTime) << "\n";
        }
    }
    cout << endl;
}

}  // namespace

namespace lsst { namespace qserv { namespace replica {

DatabaseTestApp::Ptr DatabaseTestApp::create(int argc, char* argv[]) {
    return Ptr(new DatabaseTestApp(argc, argv));
}

DatabaseTestApp::DatabaseTestApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().commands("operation",
                      {"CONFIGURATION", "DATABASES", "FIND_OLDEST_REPLICAS", "FIND_REPLICAS",
                       "FIND_REPLICAS_1", "FIND_WORKER_REPLICAS_1", "FIND_WORKER_REPLICAS_2",
                       "FIND_WORKER_REPLICAS_3", "FIND_WORKER_REPLICAS_4", "TABLE_ROW_STATS"},
                      _operation)
            .option("tables-page-size", "The number of rows in the table of replicas (0 means no pages).",
                    _pageSize)
            .flag("tables-vertical-separator",
                  "Print vertical separator when displaying tabular data in dumps.", _verticalSeparator);

    parser().command("CONFIGURATION")
            .description("Dump the current configuration of the Replication system.");

    parser().command("DATABASES")
            .description(
                    "Get a list of databases for a given selection criteria from Configuration."
                    " If flags --all and --published are not used then the command will report"
                    " a subset of databases (for a given family or all families) which are not"
                    " yet PUBLISHED.")
            .option("database-family",
                    "The name of a database family. This option will narrow a scope of the operation"
                    " to the specified family only. Otherwise databases of all known families will"
                    " be considered.",
                    _databaseFamilyName)
            .flag("all",
                  "Report all known databases in the specified family (if the one was provided)"
                  " or all families regardless if they are PUBLISHED or not. If this flag is not"
                  " used then a subset of databases in question is determined by a presence of"
                  " flag --published",
                  _allDatabases)
            .flag("published",
                  "Report a subset of PUBLISHED databases in the specified family (if the one was provided)"
                  " or all families. This flag is used only if flag --all is not used.",
                  _isPublished);

    parser().command("FIND_OLDEST_REPLICAS")
            .description(
                    "Find oldest replicas. The number of replicas can be also limited by using"
                    " option --replicas.")
            .option("replicas", "The maximum number of replicas to be returned when querying the database.",
                    _maxReplicas)
            .flag("enabled-workers-only",
                  "Limit a scope of an operation to workers which are presently enabled in"
                  " the Replication system.",
                  _enabledWorkersOnly);

    parser().command("FIND_REPLICAS")
            .description("Find replicas of a given chunk in a scope of a database.")
            .required("chunk", "The chunk number.", _chunk)
            .required("database", "The name of a database.", _databaseName)
            .flag("enabled-workers-only",
                  "Limit a scope of an operation to workers which are presently enabled in"
                  " the Replication system.",
                  _enabledWorkersOnly);

    parser().command("FIND_REPLICAS_1")
            .description("Find replicas of a collection of two chunks in a scope of a database.")
            .required("chunk1", "The first chunk number.", _chunk1)
            .required("chunk2", "The second chunk number.", _chunk2)
            .required("database", "The name of a database.", _databaseName)
            .flag("enabled-workers-only",
                  "Limit a scope of an operation to workers which are presently enabled in"
                  " the Replication system.",
                  _enabledWorkersOnly);

    parser().command("FIND_WORKER_REPLICAS_1")
            .description("Find replicas at a given worker.")
            .required("worker", "The name of a worker.", _workerName);

    parser().command("FIND_WORKER_REPLICAS_2")
            .description("Find replicas at a given worker for the specified database only.")
            .required("worker", "The name of a worker", _workerName)
            .required("database", "The name of a database", _databaseName);

    parser().command("FIND_WORKER_REPLICAS_3")
            .description("Find replicas of a chunk at a given worker.")
            .required("chunk", "The chunk number.", _chunk)
            .required("worker", "The name of a worker", _workerName);

    parser().command("FIND_WORKER_REPLICAS_4")
            .description("Find replicas of a chunk at a given worker.")
            .required("chunk", "The chunk number.", _chunk)
            .required("worker", "The name of a worker.", _workerName)
            .required("database-family", "The name of a database family.", _databaseFamilyName)
            .flag("all",
                  "Report all known databases in the specified family (if the one was provided)"
                  " or all families regardless if they are PUBLISHED or not. If this flag is not"
                  " used then a subset of databases in question is determined by a presence of"
                  " flag --published",
                  _allDatabases)
            .flag("published",
                  "Report a subset of PUBLISHED databases in the specified family (if the one was provided)"
                  " or all families. This flag is used only if flag --all is not used.",
                  _isPublished);

    parser().command("TABLE_ROW_STATS")
            .description("Report table stats (numers of rows) from the internal tables.")
            .required("database", "The name of a database.", _databaseName)
            .required("table", "The name of a table.", _tableName)
            .optional("transaction-id",
                      "A unique identifier of a transaction. NOTE: using the transaction 0 (which is also"
                      " the default value of this optional parameter) would result in reporting stats"
                      " across all transactions.",
                      _transactionId);
}

int DatabaseTestApp::runImpl() {
    if ("CONFIGURATION" == _operation) {
        cout << serviceProvider()->config()->toJson().dump() << endl;
        return 0;
    }
    if ("DATABASES" == _operation) {
        auto const databases =
                serviceProvider()->config()->databases(_databaseFamilyName, _allDatabases, _isPublished);
        for (auto&& database : databases) {
            cout << database << "\n";
        }
        cout << endl;
        return 0;
    }
    if ("TABLE_ROW_STATS" == _operation) {
        _dump(serviceProvider()->databaseServices()->tableRowStats(_databaseName, _tableName,
                                                                   _transactionId));
        return 0;
    }

    vector<ReplicaInfo> replicas;
    if ("FIND_OLDEST_REPLICAS" == _operation) {
        serviceProvider()->databaseServices()->findOldestReplicas(replicas, _maxReplicas,
                                                                  _enabledWorkersOnly);
    } else if ("FIND_REPLICAS" == _operation) {
        serviceProvider()->databaseServices()->findReplicas(replicas, _chunk, _databaseName,
                                                            _enabledWorkersOnly);
    } else if ("FIND_REPLICAS_1" == _operation) {
        vector<unsigned int> chunks;
        chunks.push_back(_chunk1);
        chunks.push_back(_chunk2);
        serviceProvider()->databaseServices()->findReplicas(replicas, chunks, _databaseName,
                                                            _enabledWorkersOnly);
    } else if ("FIND_WORKER_REPLICAS_1" == _operation) {
        serviceProvider()->databaseServices()->findWorkerReplicas(replicas, _workerName);
    } else if ("FIND_WORKER_REPLICAS_2" == _operation) {
        serviceProvider()->databaseServices()->findWorkerReplicas(replicas, _workerName, _databaseName);
    } else if ("FIND_WORKER_REPLICAS_3" == _operation) {
        serviceProvider()->databaseServices()->findWorkerReplicas(replicas, _chunk, _workerName);
    } else if ("FIND_WORKER_REPLICAS_4" == _operation) {
        serviceProvider()->databaseServices()->findWorkerReplicas(
                replicas, _chunk, _workerName, _databaseFamilyName, _allDatabases, _isPublished);
    } else {
        throw logic_error(string(__func__) + ": unsupported operation: " + _operation);
    }
    ::dump(replicas);
    return 0;
}

void DatabaseTestApp::_dump(TableRowStats const& stats) {
    vector<string> transactionId;
    transactionId.reserve(stats.entries.size());

    vector<string> chunk;
    chunk.reserve(stats.entries.size());

    vector<string> isOverlap;
    isOverlap.reserve(stats.entries.size());

    vector<string> numRows;
    numRows.reserve(stats.entries.size());

    vector<string> updateTime;
    updateTime.reserve(stats.entries.size());

    for (auto&& e : stats.entries) {
        transactionId.emplace_back(to_string(e.transactionId));
        chunk.emplace_back(to_string(e.chunk));
        isOverlap.emplace_back(replica::bool2str(e.isOverlap));
        numRows.emplace_back(to_string(e.numRows));
        updateTime.emplace_back(to_string(e.updateTime));
    }

    string const caption = "DATABASE: " + stats.database + ", TABLE: " + stats.table;
    string const indent;
    util::ColumnTablePrinter table(caption, indent, _verticalSeparator);
    table.addColumn("transaction-id", transactionId);
    table.addColumn("chunk", chunk);
    table.addColumn("is_overlap", isOverlap);
    table.addColumn("rows", numRows);
    table.addColumn("updated", updateTime);

    bool const topSeparator = false;
    bool const bottomSeparator = false;
    bool const repeatedHeader = false;
    table.print(cout, topSeparator, bottomSeparator, _pageSize, repeatedHeader);
}

}}}  // namespace lsst::qserv::replica
