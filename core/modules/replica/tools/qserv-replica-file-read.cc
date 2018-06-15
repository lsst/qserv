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

/// qserv-replica-file-read.cc is a test application for the FileClient
/// interface.

// System headers
#include <cerrno>
#include <cstdio>           // std::FILE, C-style file I/O
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/ServiceProvider.h"
#include "replica/FileClient.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;

namespace {

// Command line parameters

std::string workerName;
std::string databaseName;
std::string inFileName;
std::string outFileName;
std::string configUrl;

bool verbose = false;

// Record buffer

constexpr size_t bufSize{1000000};
uint8_t buf[bufSize];

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
int run() {
  
    std::FILE* fp = 0;
    try {
        replica::ServiceProvider::Ptr const provider = replica::ServiceProvider::create(configUrl);

        if (replica::FileClient::Ptr file =
            replica::FileClient::open(provider, workerName, databaseName, inFileName)) {

            size_t const fileSize = file->size();
            if (verbose) {
                std::cout << "file size: " << fileSize << " bytes" << std::endl;
            }
            if ((fp = std::fopen(outFileName.c_str(), "wb"))) {
                
                size_t totalRead = 0;
                size_t num;
                while ((num = file->read(buf, bufSize))) {
                    totalRead += num;
                    if (verbose)
                        std::cout << "read " << totalRead << "/" << fileSize << std::endl;
                    std::fwrite(buf, sizeof(uint8_t), num, fp);
                }
                if (fileSize == totalRead) {
                    std::fflush(fp);
                    std::fclose(fp);
                    return 0;
                }
                std::cerr << "input file was closed too early after reading " << totalRead
                    << " bytes instead of " << fileSize << std::endl;
            }
            std::cerr << "failed to open the output file, error: " << std::strerror(errno) << std::endl;
        }
        std::cerr << "failed to open the input file" << std::endl;

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
    }
    if (fp) { std::fclose(fp); }

    return 1;
}
}  /// namespace

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <worker> <database> <infile> <outfile> [--verbose] [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <worker>   - the name of a worker\n"
            "  <database> - the name of a database\n"
            "  <infile>   - the name of an input file to be copied from the worker\n"
            "  <outfile>  - the name of a local file to be created and populated\n"
            "\n"
            "Flags and options:\n"
            "  --verbose  - the flag triggering a report on a progress of the operation\n"
            "  --config   - a configuration URL (a configuration file or a set of the database\n"
            "               connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::workerName   = parser.parameter<std::string>(1);
        ::databaseName = parser.parameter<std::string>(2);
        ::inFileName   = parser.parameter<std::string>(3);
        ::outFileName  = parser.parameter<std::string>(4);

        ::verbose      = parser.flag("verbose");
        ::configUrl    = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    } 
    ::run ();
    return 0;
}
