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
 * The CPP-HTTPLIB-based frontend for Czar.
 */

// System headers
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

// Qserv headers
#include "czar/Czar.h"
#include "czar/HttpCzarSvc.h"
#include "global/stringUtil.h"  // for qserv::stoui

using namespace std;
namespace czar = lsst::qserv::czar;
namespace qserv = lsst::qserv;

namespace {

string const usage = "Usage: <czar-name> <config> <port> <threads> <ssl-cert-file> <ssl-private-key-file>";

}  // namespace

int main(int argc, char* argv[]) {
    // Parse command-line parameters to get:
    // - the name of Czar
    // - a path to the configuration files
    // - the port number (0 value would result in allocating the first available port)
    // - the number of service threads (0 value would assume the number of host machine's
    //   hardware threads)
    if (argc != 5) {
        cerr << __func__ << ": insufficient number of the command-line parameters\n" << ::usage << endl;
        return 1;
    }
    int nextArg = 1;
    string const czarName = argv[nextArg++];
    string const configFilePath = argv[nextArg++];
    uint16_t port = 0;
    unsigned int numThreads = 0;
    try {
        int const portParsed = stoi(argv[nextArg++]);
        if (portParsed < 0 || portParsed > numeric_limits<uint16_t>::max()) {
            cerr << __func__ << ": the port number is not valid\n" << ::usage << endl;
            return 1;
        }
        port = static_cast<uint16_t>(portParsed);
        numThreads = qserv::stoui(argv[nextArg++]);
        if (numThreads == 0) numThreads = thread::hardware_concurrency();
    } catch (exception const& ex) {
        cerr << __func__ << ": failed to parse command line parameters\n" << ::usage << endl;
        return 1;
    }
    try {
        auto const czar = czar::Czar::createCzar(configFilePath, czarName);
        auto const svc = czar::HttpCzarSvc::create(port, numThreads, sslCertFile, sslPrivateKeyFile);
        cout << __func__ << ": HTTP-based query processing service of Czar bound to port: " << svc->port()
             << endl;
        svc->startAndWait();
    } catch (exception const& ex) {
        cerr << __func__ << ": the application failed, exception: " << ex.what() << endl;
        return 1;
    }
    return 0;
}
