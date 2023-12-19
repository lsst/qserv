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
#include "replica/apps/FileReadApp.h"

// System headers
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/worker/FileClient.h"

using namespace std;

namespace {

string const description =
        "This is an  application which acts as a read-only client of"
        " the Replication system's file server.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

FileReadApp::Ptr FileReadApp::create(int argc, char* argv[]) { return Ptr(new FileReadApp(argc, argv)); }

FileReadApp::FileReadApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().required("worker-host",
                      "The host name or an IP address of a worker where the input file is located.",
                      _workerHost)
            .required("worker-port",
                      "The port number for the worker service where the input file is located.", _workerPort)
            .required("database", "The name of a database.", _databaseName)
            .required("infile",
                      "The name of an input file to be copied from the worker. The name should not"
                      " include any directories.",
                      _inFileName)
            .required("outfile", "The name of a local file to be created and populated with received data.",
                      _outFileName)
            .option("record-size-bytes",
                    "The maximum number of bytes to be read from a server at each request.", _recordSizeBytes)
            .flag("verbose", "Report on a progress of the operation.", _verbose);
}

int FileReadApp::runImpl() {
    if (_recordSizeBytes == 0) {
        throw invalid_argument("record size 0 is not allowed.");
    }
    _buf.resize(_recordSizeBytes);

    FILE* fp = 0;
    try {
        if (FileClient::Ptr const file = FileClient::open(serviceProvider(), _workerHost, _workerPort,
                                                          _databaseName, _inFileName)) {
            size_t const fileSize = file->size();
            if (_verbose) {
                cout << "file size: " << fileSize << " bytes" << endl;
            }
            if ((fp = fopen(_outFileName.c_str(), "wb"))) {
                size_t totalRead = 0;
                size_t num;
                while ((num = file->read(_buf.data(), _recordSizeBytes))) {
                    totalRead += num;
                    if (_verbose) cout << "read " << totalRead << "/" << fileSize << endl;
                    fwrite(_buf.data(), sizeof(uint8_t), num, fp);
                }
                if (fileSize == totalRead) {
                    fflush(fp);
                    fclose(fp);
                    return 0;
                }
                cerr << "input file was closed too early after reading " << totalRead << " bytes instead of "
                     << fileSize << endl;
                fclose(fp);
                return 1;
            }
            cerr << "failed to open the output file, error: " << strerror(errno) << endl;
            return 1;
        }
        cerr << "failed to open the input file" << endl;
        return 1;

    } catch (exception const& ex) {
        cerr << ex.what() << endl;
    }
    if (fp) fclose(fp);

    return 1;
}

}  // namespace lsst::qserv::replica
