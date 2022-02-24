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
#include "replica/FileExportApp.h"

// System headers˙
#include <algorithm>
#include <cctype>
#include <fstream>
#include <streambuf>
#include <iostream>
#include <limits>
#include <stdexcept>

// Qserv headers
#include "replica/ExportClient.h"
#include "replica/Performance.h"
#include "util/File.h"

using namespace std;
using namespace nlohmann;

namespace {

string const description =
    "This is an  application which acts as a client for the "
    " Replication system's table data exporting server.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = false;

string parse(string const& context, json const& jsonObj, string const& key) {
    if ((0 == jsonObj.count(key) or not jsonObj[key].is_string())) {
        throw invalid_argument(
                context + "No key for <" + key + "> found in the current element of the JSON array"
                " or its value is not a string");
    }
    return jsonObj[key];
}


template<typename T>
T parse(string const& context, json const& jsonObj, string const& key, T minValue) {
    if ((0 == jsonObj.count(key) or not jsonObj[key].is_number())) {
        throw invalid_argument(
                context + "No key for <" + key + "> found in the current element of the JSON array"
                " or its value is not a number");
    }
    uint64_t const num = jsonObj[key];
    if (num < minValue or num > numeric_limits<T>::max()) {
        throw invalid_argument(
                context
                + "Failed to parse JSON object, a value " + to_string(num)
                + " of <" + key + "> is not in a range of " + to_string(minValue) + "-"
                + to_string(numeric_limits<T>::max()) + ".");
    }
    return (T)num;
}

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

list<FileExportApp::FileExportSpec> FileExportApp::parseFileList(json const& jsonObj) {
    string const context = "FileExportApp::" + string(__func__) + "  ";

    list<FileExportApp::FileExportSpec> files;

    if (not jsonObj.is_array()) {
        throw invalid_argument(
                context + "The input parameter doesn't represent a JSON array of file"
                " specifications.");
    }

    for (auto&& fileSpecJson: jsonObj) {
        if (not fileSpecJson.is_object()) {
            throw invalid_argument(
                    context + "The next element in the JSON array doesn't represent a JSON object"
                    " with a file specification.");
        }
        FileExportApp::FileExportSpec file;

        file.workerHost   = parse(context, fileSpecJson, "worker-host");
        file.workerPort   = parse<uint16_t>(context, fileSpecJson, "worker-port", 1);
        file.databaseName = parse(context, fileSpecJson, "database");
        file.tableName    = parse(context, fileSpecJson, "table");
        file.chunk        = parse<unsigned int>(context, fileSpecJson, "chunk", 0);
        file.overlap      = parse<unsigned int>(context, fileSpecJson, "overlap", 0);
        file.outFileName  = parse(context, fileSpecJson, "path");

        files.push_back(file);
    }
    return files;
}


FileExportApp::Ptr FileExportApp::create(int argc, char* argv[]) {
    return Ptr(new FileExportApp(argc, argv));
}


FileExportApp::FileExportApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            ::injectDatabaseOptions,
            ::boostProtobufVersionCheck,
            ::enableServiceProvider
        ) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"FILE", "FILE-LIST"},
        _command
    ).option(
        "column-separator",
        "The column separator in the output files. Allowed values: COMMA, TAB.",
        _columnSeparatorStr
    ).flag(
        "verbose",
        "Print various stats upon a completion of the export.",
        _verbose
    );

    parser().command(
        "FILE"
    ).description(
        "The single file export option. A source of the export and a path to"
        " an output file are specified via a group of mandatory parameters."
    ).required(
        "worker-host",
        "The name of a worker host the Export service is run.",
        _file.workerHost
    ).required(
        "worker-port",
        "The port number of the worker's Export service.",
        _file.workerPort
    ).required(
        "database",
        "The name of a database which has the desired table.",
        _file.databaseName
    ).required(
        "table",
        "The name of a table to be exported.",
        _file.tableName
    ).required(
        "chunk",
        "The chunk number. A value of this parameter is ignored for non-partitioned tables.",
        _file.chunk
    ).required(
        "overlap",
        "The flag which is set for the partitioned tables to indicate if a table"
        " 'overlap' is requested.",
        _file.overlap
    ).required(
        "infile",
        "A path for a local output file to be created.",
        _file.outFileName
    );

    parser().command(
        "FILE-LIST"
    ).description(
        "The batch export option. A list of tables to be exported will be read from"
        " a file. The content of the file is required to be a serialized JSON array"
        " of objects. Each object specifies a source of the tab;e export request and"
        " the name name of a file to write the table data into. The general schema of the JSON object is:"
        " [{\"worker-host\":<string>,\"worker-port\":<number>,\"database\":<string>,"
        "\"table\":<string>,\"chunk\":<number>,\"overlap\":{0|1},\"path\":<string>},...]."
        " Where values for the keys \"chunk\" and \"overlap\" are ignored"
        " for the non-partitioned tables. The tables will be exported sequentially."
    ).required(
        "file-list",
        "The name of a file with export specifications. If the file name is set to '-'"
        " then the specifications will be read from the standard input stream",
        _fileListName
    );
}


int FileExportApp::runImpl() {
    string const context = "FileExportApp::" + string(__func__) + "  ";

    list<FileExportSpec> files;
    if (_command == "FILE") {
        files.push_back(_file);
    } else if (_command == "FILE-LIST") {
        files = _readFileList();
    } else {
        throw invalid_argument(context + "Unsupported loading method " + _command);
    }
    for (auto&& file: files) {
        _export(file);
    }
    return 0;
}


list<FileExportApp::FileExportSpec> FileExportApp::_readFileList() const {
    string const context = "FileExportApp::" + string(__func__) + "  ";

    ifstream file(_fileListName);
    if (not file.good()) {
        throw invalid_argument(context + "Failed to open file: " + _fileListName);
    }
    string str;
    try {
        str = string(istreambuf_iterator<char>(file), istreambuf_iterator<char>());
    } catch (exception const& ex) {
        throw invalid_argument(
                context + "Failed to read file: " + _fileListName
                + ", exception: " + string(ex.what()));
    }
    json jsonObj;
    try {
        jsonObj = json::parse(str);
    } catch (exception const& ex) {
        throw invalid_argument(
                context + "Failed to parse the content of file: " + _fileListName
                + " into a JSON object, exception: " + string(ex.what()));
    }
    return parseFileList(jsonObj);
}


void FileExportApp::_export(FileExportSpec const& file) const {
    string const context = "FileExportApp::" + string(__func__) + "  ";

    // Export the table into the file
    //
    // TODO: consider doing this asynchronously in a separate thread while
    // limiting the maximum duration of the operation by a timeout (BOOST ASIO
    // timer launched in a separate thread). A duration of the timeout could be
    // set via an optional parameter to the application.

    ExportClient::ColumnSeparator columnSeparator;
    if ("COMMA" == _columnSeparatorStr) {
        columnSeparator = ExportClient::ColumnSeparator::COMMA;
    } else if ("TAB" == _columnSeparatorStr) {
        columnSeparator = ExportClient::ColumnSeparator::TAB;
    } else {
        throw invalid_argument(
                context + "unsupported value of the column separator: '" + _columnSeparatorStr + "'");
    }
    uint64_t const startedMs = PerformanceUtils::now();
    auto const ptr = ExportClient::connect(
        file.workerHost,
        file.workerPort,
        file.databaseName,
        file.tableName,
        file.chunk,
        file.overlap,
        file.outFileName,
        columnSeparator,
        authKey()
    );
    ptr->receive();
    uint64_t const finishedMs = PerformanceUtils::now();
    
    if (_verbose) {
        uint64_t const elapsedMs  = max(1UL, finishedMs - startedMs);
        double   const elapsedSec = elapsedMs / 1000;
        double   const rowsPerSec = ptr->totalNumRows() / elapsedSec;
        double   const megaBytesPerSec = ptr->sizeBytes() / 1000000 / elapsedSec;
        cout << "Exporting service location: " << file.workerHost << ":" << file.workerPort << "\n"
             << "           Source database: " << file.databaseName << "\n"
             << "              Source table: " << file.tableName << "\n"
             << "                     Chunk: " << file.chunk << "\n"
             << "          Is chunk overlap: " << bool2str(file.overlap) << "\n"
             << "          Output file name: " << file.outFileName << "\n"
             << "               Start  time: " << PerformanceUtils::toDateTimeString(chrono::milliseconds(startedMs)) << "\n"
             << "               Finish time: " << PerformanceUtils::toDateTimeString(chrono::milliseconds(finishedMs)) << "\n"
             << "              Elapsed time: " << elapsedSec << " sec\n"
             << "            Rows  received: " << ptr->totalNumRows() << "\n"
             << "            Bytes received: " << ptr->sizeBytes() << "\n"
             << "                  Rows/sec: " << rowsPerSec << "\n"
             << "                 MByte/sec: " << megaBytesPerSec << "\n"
             << endl;
    }
}

}}} // namespace lsst::qserv::replica
