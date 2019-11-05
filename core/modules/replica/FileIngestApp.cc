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
#include "replica/FileIngestApp.h"

// System headers
#include <algorithm>
#include <iostream>
#include <limits>
#include <regex>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

// Qserv headers
#include "replica/IngestClient.h"
#include "replica/Performance.h"
#include "util/File.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

string const description =
    "This is an  application which acts as a a catalog data loading"
    " client of the Replication system's catalog data ingest server.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

FileIngestApp::Ptr FileIngestApp::create(int argc, char* argv[]) {
    return Ptr(
        new FileIngestApp(argc, argv)
    );
}


FileIngestApp::FileIngestApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            false   /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            false   /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "command",
        {"FILE", "FILE-LIST"},
        _command
    ).flag(
        "verbose",
        "Print various stats upon a completion of the ingest",
        _verbose
    );

    parser().command(
        "FILE"
    ).description(
        "The single file ingest option. A destination of the ingest and a path to"
        " the file to ingest are specified via a group of mandatory parameters."
    ).required(
        "worker-host",
        "The name of a worker host the Ingest service is run.",
        _file.workerHost
    ).required(
        "worker-port",
        "The port number of the worker's Ingest service.",
        _file.workerPort
    ).required(
        "transaction-id",
        "A unique identifier (number) of a super-transaction which must be already"
        "open.",
        _file.transactionId
    ).required(
        "table",
        "The name of a table to be ingested.",
        _file.tableName
    ).required(
        "type",
        "The type of a table to be ingested. Allowed options: 'P' for contributions"
        " into partitioned tables, and 'R' for contributions into the regular tables.",
        _file.tableType
    ).required(
        "infile",
        "A path to an input file to be sent to the worker.",
        _file.inFileName
    );

    parser().command(
        "FILE-LIST"
    ).description(
        "The batch ingest option. A list of files to be ingested will be read from"
        " a file.  Each line of the file specifies a destination of the ingest and"
        " the name name of a file to ingest via the following space-separated fields:"
        " <worker-host> <worker-port> <transaction-id> <table> {'P'|'R'} <infile>."
        " Where 'P' is for the partitioned (chunked) table contributions, and 'R' is"
        " for the regular tables contributions. Input files for the partitioned tables"
        " are expected to have the following names: 'chunk_<num>.txt' or"
        " 'chunk_<num>_overlap.txt'. The files will be ingested sequentially."
    ).required(
        "file-list",
        "The name of a file with ingest specifications. If the file name is set to '-'"
        " then the specifications will be read from the Standard Input Stream",
        _fileListName
    );
}

int FileIngestApp::runImpl() {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    list<FileIngestSpec> files;
    if (_command == "FILE") {
        files.push_back(_file);
    } else if (_command == "FILE-LIST") {
        files = _readFileList();
    } else {
        throw invalid_argument(
                context
                + "Unsupported loading method " + _command);
    }
    for (auto&& file: files) {
        _ingest(file);
    }
    return 0;
}


list<FileIngestApp::FileIngestSpec> FileIngestApp::_readFileList() const {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    list<FileIngestSpec> files;
    int lineNum = 0;

    for (auto&& line: util::File::getLines(_fileListName)) {
        ++lineNum;

        // Fields to be parsed:
        // <worker-host> <worker-port> <transaction-id> <table> <type> <infile>
        regex const re("^(.+)[ ]+([0-9]+)[ ]+([0-9]+)[ ]+(.+)[ ]+(P|R)[ ]+(.+)$", regex::extended);
        smatch match;
        if (not regex_search(line, match, re) or match.size() != 7) {
            throw invalid_argument(
                    context
                    + "Failed to parse the content of file " + _fileListName + ":" + to_string(lineNum)
                    + ", expected fields: <worker-host> <worker-port> <transaction-id> <table> <type> <infile>.");
        }

        FileIngestSpec file;
        file.workerHost = match[1].str();

        auto const workerPort = stoul(match[2].str());
        if (workerPort < 1 or workerPort > numeric_limits<uint16_t>::max()) {
            throw invalid_argument(
                    context
                    + "Failed to parse the content of file " + _fileListName + ":" + to_string(lineNum)
                    + ", a value " + to_string(workerPort) + " of <worker-port> is not in a range of 1-"
                    + to_string(numeric_limits<uint16_t>::max()) + ".");
        }
        file.workerPort = (uint16_t)workerPort;

        auto const transactionId = stoul(match[3].str());
        if (transactionId > numeric_limits<uint32_t>::max()) {
            throw invalid_argument(
                    context
                    + "Failed to parse the content of file " + _fileListName + ":" + to_string(lineNum)
                    + ", a value " + to_string(transactionId) + " of <transaction-id> is not in a range of 0-"
                    + to_string(numeric_limits<uint32_t>::max()) + ".");
        }
        file.transactionId = (uint32_t)transactionId;
        file.tableName = match[4].str();
        file.tableType = match[5].str();
        file.inFileName = match[6].str();
        
        files.push_back(file);
    }
    return files;
}


void FileIngestApp::_ingest(FileIngestSpec const& file) const {
    string const context = "FileIngestApp::" + string(__func__) + "  ";

    // Analyze the file to make sure it's a regular file, and it can be read.

    fs::path const path = file.inFileName;
    boost::system::error_code ec;
    fs::file_status const status = fs::status(path, ec);
    if (ec.value() != 0) {
        throw invalid_argument(context + "file doesn't exist: " + path.string());
    }
    if (not fs::is_regular(status)) {
        throw invalid_argument(context + "not a regular file: " + path.string());
    }
    
    // For partitioned tables analyze file name and extract a chunk number and
    // the 'overlap' attribute
    unsigned int chunk = 0;
    bool isOverlap = false;

    if (file.tableType == "P") {
        string const filename = fs::absolute(path).filename().string();
        regex const re("^chunk_([0-9]+)(_overlap)?\\.txt$", regex::extended);
        smatch match;
        if (not regex_search(filename, match, re) or match.size() != 3) {
            throw invalid_argument(
                    context + "allowed file names for contributions into partitioned tables:"
                    " 'chunk_<chunk>.txt', 'chunk_<chunk>_overlap.txt'");
        }
        chunk = stoul(match[1].str());
        isOverlap = not match[2].str().empty();

    } else if (file.tableType == "R") {
        ;
    } else {
        throw invalid_argument(
                context + "a value '" + file.tableType
                + "' of <type> is not in a set of {'P','R'}.");
    }

    // Push the file
    //
    // TODO: consider doing this asynchronously in a separate thread while
    // limiting the maximum duration of the operation by a timeout (BOOST ASIO
    // timer launched in a separate thread). A duration of the timeout could be
    // set via an optional parameter to the application.

    uint64_t const startedMs = PerformanceUtils::now();
    auto const ptr = IngestClient::connect(
        file.workerHost,
        file.workerPort,
        file.transactionId,
        file.tableName,
        chunk,
        isOverlap,
        file.inFileName
    );
    ptr->send();
    uint64_t const finishedMs = PerformanceUtils::now();
    
    if (_verbose) {
        uint64_t const elapsedMs  = max(1UL, finishedMs - startedMs);
        double   const elapsedSec = elapsedMs / 1000.;
        uint64_t const rowsPerSec = ptr->totalNumRows() / elapsedSec;
        uint64_t const megaBytesPerSec = ptr->sizeBytes() / 1000000 / elapsedSec;
        cout << "Ingest service location: " << file.workerHost << ":" << file.workerPort << "\n"
             << " Transaction identifier: " << file.transactionId << "\n"
             << "      Destination table: " << file.tableName << "\n"
             << "                  Chunk: " << chunk << "\n"
             << "       Is chunk overlap: " << (isOverlap ? "1" : "0") << "\n"
             << "        Input file name: " << file.inFileName << "\n"
             << "            Start  time: " << PerformanceUtils::toDateTimeString(chrono::milliseconds(startedMs)) << "\n"
             << "            Finish time: " << PerformanceUtils::toDateTimeString(chrono::milliseconds(finishedMs)) << "\n"
             << "           Elapsed time: " << elapsedSec << " sec\n"
             << "             Rows  sent: " << ptr->totalNumRows() << "\n"
             << "             Bytes sent: " << ptr->sizeBytes() << "\n"
             << "               Rows/sec: " << rowsPerSec << "\n"
             << "              MByte/sec: " << megaBytesPerSec << "\n"
             << endl;
    }
}

}}} // namespace lsst::qserv::replica
