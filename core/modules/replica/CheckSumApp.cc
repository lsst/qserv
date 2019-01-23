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
#include "replica/CheckSumApp.h"

// System headers
#include <iostream>

// Qserv headers
#include "replica/FileUtils.h"

using namespace std;

namespace {

string const description {
    "This application calculates and prints a checksum of a file"
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

CheckSumApp::Ptr CheckSumApp::create(int argc,
                                     const char* const argv[]) {
    return Ptr(
        new CheckSumApp(
            argc,
            argv
        )
    );
}


CheckSumApp::CheckSumApp(int argc,
                         const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            false   /* injectDatabaseOptions */,
            false   /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ),
        _incremental(false) {

    // Configure the command line parser

    parser().required(
        "file",
        "The name of a file to process",
        _file);

    parser().flag(
        "incremental",
        "use the incremental file reader",
        _incremental);
}


int CheckSumApp::runImpl() {

    if (_incremental) {
        FileCsComputeEngine eng(_file);
        while (not eng.execute()) { ; }
        cout << _file << ": " << eng.cs() << endl;
    } else {
        cout << _file << ": " << FileUtils::compute_cs(_file) << endl;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
