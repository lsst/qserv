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

/**
 * @see HttpFileReader
 */
// System headers
#include <iostream>

// Qserv headers
#include "replica/HttpFileReader.h"

using namespace std;
using namespace lsst::qserv::replica;

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        cout << "usage: <method> <url> <data> <header>" << endl;
        return 1;
    }
    string const method = argv[1];
    string const url = argv[2];
    string const data = argv[3];
    string const header = argv[4];
    vector<string> headers;
    if (!header.empty()) headers.push_back(header);
    try {
        HttpFileReader reader(method, url, data, headers);
        reader.read([](string const& line) {
            cout << line << "\n";
        });
    } catch(exception const& ex) {
        cout << ex.what() << endl;
        return 1;
    }
    return 0;
}
