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
#include "replica/QservWorkerApp.h"

// System headers
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/GetReplicasQservMgtRequest.h"
#include "replica/QservMgtServices.h"
#include "replica/SetReplicasQservMgtRequest.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This is an application for operations with Qserv workers.";

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

QservWorkerApp::Ptr QservWorkerApp::create(int argc, char* argv[]) {
    return Ptr(
        new QservWorkerApp(argc, argv)
    );
}


QservWorkerApp::QservWorkerApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"ADD_REPLICA", "REMOVE_REPLICA", "GET_REPLICAS", "SET_REPLICAS"},
        _command);

    // Parameters, options and flags shared by all commands

    parser().required(
        "worker",
        "The name of a Qserv worker.",
        _workerName);

    parser().flag(
        "force",
        "Force the worker to proceed with requested"
        " replica removal regardless of the replica usage status.",
        _forceRemove);

    parser().option(
        "tables-page-size",
        "The number of rows in the table of replicas (0 means no pages).",
        _pageSize);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in reports.",
        _verticalSeparator);

    // Command-specific parameters, options and flags

    auto&& addCmd = parser().command("ADD_REPLICA");

    addCmd.description(
        "Add a single replica of a chunk to the worker." );

    addCmd.required(
        "database",
        "The name of a database.",
        _databaseName);

    addCmd.required(
        "chunk",
        "The number of a chunk.",
        _chunkNumber);

    // Command-specific parameters, options and flags

    auto&& removeCmd = parser().command("REMOVE_REPLICA");

    removeCmd.description(
        "Remove a single replica of a chunk from the worker.");

    removeCmd.required(
        "database",
        "The name of a database.",
        _databaseName);

    removeCmd.required(
        "chunk",
        "The number of a chunk.",
        _chunkNumber);

    // Command-specific parameters, options and flags

    auto&& getCmd = parser().command("GET_REPLICAS");

    getCmd.description(
        "Obtain a set of replicas which are known to the Qserv worker."
        " Then print the replica info.");

    getCmd.required(
        "database-family",
        "The name of a database family.",
        _familyName);

    getCmd.flag(
        "in-use-only",
        "Limit a scope of operations to a subset of chunks which are in use.",
        _inUseOnly);

    // Command-specific parameters, options and flags

    auto&& setCmd = parser().command("SET_REPLICAS");

    setCmd.description(
        "Tell the Qserv worker to set a new collection of replicas instead of what"
        " it may had at a time when this operation was initiated. The previous set"
        " of the replica info will be printed upon a completion of the operation.");

    setCmd.required(
        "filename",
        "The name of of a file with space-separated pairs of <database>:<chunk>.",
        _inFileName);
}


int QservWorkerApp::runImpl() {

    if (_command == "GET_REPLICAS") {

        auto const request = serviceProvider()->qservMgtServices()->getReplicas(
            _familyName,
            _workerName,
            _inUseOnly,
            string(),
            [this] (GetReplicasQservMgtRequest::Ptr const& request) {
                cout << "state: " << request->state2string() << endl;
                if (request->extendedState() == QservMgtRequest::SUCCESS) {
                    this->_dump(request->replicas());
                }
            }
        );
        request->wait();

    } else if (_command == "SET_REPLICAS") {

        QservReplicaCollection replicas;
        _readInFile(replicas);

        cout << "replicas read: " << replicas.size() << endl;

        auto const request = serviceProvider()->qservMgtServices()->setReplicas(
            _workerName,
            replicas,
            _forceRemove,
            string(),
            [this] (SetReplicasQservMgtRequest::Ptr const& request) {
                cout << "state: " << request->state2string() << endl;
                if (request->extendedState() == QservMgtRequest::SUCCESS) {
                    this->_dump(request->replicas());
                }
            }
        );
        request->wait();

    } else if (_command == "ADD_REPLICA") {

        auto const request = serviceProvider()->qservMgtServices()->addReplica(
            _chunkNumber,
            {_databaseName},
            _workerName,
            [] (AddReplicaQservMgtRequest::Ptr const& request) {
                cout << "state: " << request->state2string() << endl;
            }
        );
        request->wait();

    } else if (_command == "REMOVE_REPLICA") {

        auto const request = serviceProvider()->qservMgtServices()->removeReplica(
            _chunkNumber,
            {_databaseName},
            _workerName,
            _forceRemove,
            [] (RemoveReplicaQservMgtRequest::Ptr const& request) {
                cout << "state: " << request->state2string() << endl;
            }
        );
        request->wait();

    } else {
        throw logic_error(
                "QservWorkerApp::" + string(__func__) + "  unsupported command: " + _command);
    }
    return 0;
}


void QservWorkerApp::_readInFile(QservReplicaCollection& replicas) const {

    replicas.clear();

    ifstream infile(_inFileName);
    if (not infile.good()) {
        cerr << "failed to open file: " << _inFileName << endl;
        throw runtime_error(
                "QservWorkerApp::" + string(__func__) + "failed to open file: " + _inFileName);
    }

    string databaseAndChunk;
    while (infile >> databaseAndChunk) {

        if (databaseAndChunk.empty()) { continue; }

        string::size_type const pos = databaseAndChunk.rfind(':');
        if ((pos == string::npos) or
            (pos == 0) or (pos == databaseAndChunk.size() - 1)) {
            throw runtime_error(
                    "QservWorkerApp::" + string(__func__) +
                    "  failed to parse file: " + _inFileName +
                    ", illegal <database>::<chunk> pair: '" + databaseAndChunk + "'");
        }
        unsigned int const chunk    = (unsigned int)(stoul(databaseAndChunk.substr(pos + 1)));
        string  const database = databaseAndChunk.substr(0, pos);

        replicas.emplace_back(
            QservReplica{
                chunk,
                database,
                0   /* useCount (UNUSED) */
            }
        );
    }
}


void QservWorkerApp::_dump(QservReplicaCollection const& collection) const {

    vector<string>       columnDatabaseName;
    vector<unsigned int> columnChunkNumber;
    vector<size_t>       columnUseCount;

    for (auto&& replica: collection) {
        columnDatabaseName.push_back(replica.database);
        columnChunkNumber .push_back(replica.chunk);
        columnUseCount    .push_back(replica.useCount);
    }

    util::ColumnTablePrinter table("REPLICAS:", "  ", _verticalSeparator);

    table.addColumn("database",  columnDatabaseName,   util::ColumnTablePrinter::LEFT);
    table.addColumn("chunk",     columnChunkNumber);
    table.addColumn("use count", columnUseCount);

    cout << "\n";
    table.print(cout, false, false, _pageSize, _pageSize != 0);
}

}}} // namespace lsst::qserv::replica
