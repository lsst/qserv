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
#include "replica/ApplicationColl.h"

// System headers
#include <iostream>
#include <stdexcept>

using namespace std;

namespace lsst { namespace qserv { namespace replica {

int ApplicationColl::run(int argc, char* argv[]) const {
    if (argc < 2) {
        _printUsage();
        return 1;
    }
    string const name = argv[1];
    auto const itr = _coll.find(name);
    if (itr == _coll.end()) {
        _printUsage("unsupported application '" + name + "'");
        return 1;
    }

    // Make another collection of arguments that excludes the name of the application.
    int const argc_ = argc - 1;
    unique_ptr<char*[]> const argv_(new char*[argc_]);
    argv_.get()[0] = argv[0];
    for (int i = 1; i < argc_; ++i) {
        argv_.get()[i] = argv[i + 1];
    }
    return itr->second->run(argc_, argv_.get());
}

void ApplicationColl::_printUsage(string const& err) const {
    if (!err.empty()) {
        cerr << "error: " << err << "\n";
    }
    string usage =
            "Usage:\n"
            "  <application> [parameters] [options] [flags]\n"
            "  <application> --help\n"
            "\n"
            "Supported applications:\n";
    for (auto&& itr : _coll) {
        usage += "  " + itr.first + "\n";
    }
    cerr << usage << endl;
}

}}}  // namespace lsst::qserv::replica
