/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// replica_calculate_cs.cc calculate and print a checksum of
/// the specified file.

// System headers
#include <iostream> 
#include <stdexcept>
#include <string> 
#include <vector> 

// Qserv headers
#include "replica/FileUtils.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

/// The name of an input file to be processed
std::vector<std::string> fileNames;

/// USe the incremental engine if set
bool incremental;

/// The test
void test() {
    try {
        if (incremental) {
            replica::MultiFileCsComputeEngine eng(fileNames);
            while (not eng.execute()) { ; }
            for (auto const& name: fileNames) {
                std::cout << name << ": " << eng.cs(name) << std::endl;
            }
        } else {
            for (auto const& name: fileNames) {
                std::cout << name << ": " << replica::FileUtils::compute_cs(name) << std::endl;
            }
        }
    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        std::exit(1);
    }
}
} // namespace

int main(int argc, const char *argv[]) {

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <file> [<file> [<file> ... ] [--incremental]\n"
            "\n"
            "Parameters:\n"
            "  <file>  - the name of a file to read. Multiple files can be specified\n"
            "\n"
            "Flags and options\n"
            "  --incremental  -- use the incremental ile reader instead\n");

        parser.parameters<std::string>(::fileNames);
        ::incremental = parser.flag("incremental");

    } catch (std::exception const &ex) {
        return 1;
    } 
    ::test();
    return 0;
}