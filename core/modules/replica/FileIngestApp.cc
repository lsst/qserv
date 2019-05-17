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
#include <regex>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

// Qserv headers
#include "replica/IngestClient.h"
#include "replica/Performance.h"

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

    parser().required(
        "worker-host",
        "The name of a worker host the Ingest service is run.",
        _workerHost);

    parser().required(
        "worker-port",
        "The port number of the worker's Ingest service.",
        _workerPort);

    parser().required(
        "transaction-id",
        "A unique identifier (number) of a super-transaction which must be already"
        "open.",
        _transactionId);

    parser().required(
        "table",
        "The name of a table to be ingested.",
        _tableName);

    parser().required(
        "infile",
        "A path to an input file to be sent to the worker.",
        _inFileName);

    parser().flag(
        "verbose",
        "Print various stats upon a completion of the ingest",
        _verbose);
}

int FileIngestApp::runImpl() {

    // Analyze the file to make sure it's a regular file, and it can be read.
    fs::path const path = _inFileName;
    boost::system::error_code ec;
    fs::file_status const status = fs::status(path, ec);
    if (ec.value() != 0) throw invalid_argument("file doesn't exist: " + path.string());
    if (not fs::is_regular(status)) throw invalid_argument("not a regular file: " + path.string());

    // Analyze file name and extract a chunk number and the 'overlap' attribute
    string const filename = fs::absolute(path).filename().string();
    regex const re("^chunk_([0-9]+)(_overlap)?\\.txt$", regex::extended);
    smatch match;
    if (not regex_search(filename, match, re) or match.size() != 3) {
        throw invalid_argument("allowed file names: chunk_<chunk>.txt, chunk_<chunk>_overlap.txt");
    }
    unsigned int const chunk = stoul(match[1].str());
    bool const isOverlap = not match[2].str().empty();

    // Push the file
    uint64_t const startedMilliseconds = PerformanceUtils::now();
    auto const ptr = IngestClient::connect(
        _workerHost,
        _workerPort,
        _transactionId,
        _tableName,
        chunk,
        isOverlap,
        _inFileName
    );
    ptr->send();
    uint64_t const finishedMilliseconds = PerformanceUtils::now();
    
    if (_verbose) {
        uint64_t const elapsedMilliseconds = max(1UL, finishedMilliseconds - startedMilliseconds);
        double   const elapsedSeconds      = elapsedMilliseconds / 1000.;
        uint64_t const rowsPerSec          = ptr->totalNumRows() / elapsedSeconds;
        uint64_t const megaBytesPerSec     = ptr->sizeBytes() / 1000000 / elapsedSeconds;
        cout << "Start  time:  " << PerformanceUtils::toDateTimeString(chrono::milliseconds(startedMilliseconds)) << "\n"
             << "Finish time:  " << PerformanceUtils::toDateTimeString(chrono::milliseconds(finishedMilliseconds)) << "\n"
             << "Elapsed time: " << elapsedSeconds << " sec\n"
             << "Rows  sent:   " << ptr->totalNumRows() << "\n"
             << "Bytes sent:   " << ptr->sizeBytes() << "\n"
             << "Rows/sec:     " << rowsPerSec << "\n"
             << "MByte/sec:    " << megaBytesPerSec << endl;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
