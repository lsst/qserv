/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/FileReadApp.h"

// System headers
#include <cerrno>
#include <cstdio>           // std::FILE, C-style file I/O
#include <cstring>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/FileClient.h"

using namespace std;

namespace {

string const description {
    "This is an  application which acts as a read-only client of"
    " the Replication system's file server"
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

FileReadApp::Ptr FileReadApp::create(int argc,
                                     const char* const argv[]) {
    return Ptr(
        new FileReadApp(
            argc,
            argv
        )
    );
}


FileReadApp::FileReadApp(int argc,
                         const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().required(
        "worker",
        "the name of a worker where the input file is located",
        _workerName);

    parser().required(
        "database",
        "the name of a database",
        _databaseName);

    parser().required(
        "infile",
        "the name of an input file to be copied from the worker. The name should not"
        " include any directories.",
        _inFileName);

    parser().required(
        "outfile",
        "the name of a local file to be created and populated with received data",
        _outFileName);

    parser().option(
        "record-size-bytes",
        "the maximum number of bytes to be read from a server at each request",
        _recordSizeBytes);

    parser().flag(
        "verbose",
        "report on a progress of the operation",
        _verbose);
}


int FileReadApp::runImpl() {

    if (_recordSizeBytes == 0) {
        throw invalid_argument("record size 0 is not allowed");
    }
    _buf.resize(_recordSizeBytes);

    FILE* fp = 0;
    try {
        if (FileClient::Ptr const file =
            FileClient::open(serviceProvider(), _workerName, _databaseName, _inFileName)) {

            size_t const fileSize = file->size();
            if (_verbose) {
                cout << "file size: " << fileSize << " bytes" << endl;
            }
            if ((fp = fopen(_outFileName.c_str(), "wb"))) {
                
                size_t totalRead = 0;
                size_t num;
                while ((num = file->read(_buf.data(), _recordSizeBytes))) {
                    totalRead += num;
                    if (_verbose)
                        cout << "read " << totalRead << "/" << fileSize << endl;
                    fwrite(_buf.data(), sizeof(uint8_t), num, fp);
                }
                if (fileSize == totalRead) {
                    fflush(fp);
                    fclose(fp);
                    return 0;
                }
                cerr << "input file was closed too early after reading " << totalRead
                     << " bytes instead of " << fileSize << endl;
            }
            cerr << "failed to open the output file, error: " << strerror(errno) << endl;
        }
        cerr << "failed to open the input file" << endl;

    } catch (exception const& ex) {
        cerr << ex.what() << endl;
    }
    if (fp) fclose(fp);

    return 1;
}

}}} // namespace lsst::qserv::replica
