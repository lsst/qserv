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
#include <thread>

// Third party headers
#include "boost/program_options.hpp"

// Qserv headers
#include "wmain/WorkerMain.cc"

using namespace std;
namespace po = boost::program_options;
namespace qserv = lsst::qserv;

namespace {
char const* const context = "[WORKER]";
}  // namespace

int main(int argc, char* argv[]) {
    po::options_description desc("", 120);
    desc.add_options()("help,h", "Print this help message and exit.");
    desc.add_options()("config,c", po::value<string>()->default_value("/config-etc/qserv-worker.cnf"),
                       "The configuration file.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, const_cast<char**>(argv), desc), vm);
    po::notify(vm);

    string const configFilePath = vm["config"].as<string>();

    try {
        cout << ::context << " Starting worker\n"
             << " Configuration file: " << configFilePath << "\n"
             << endl;

        auto const workerConfig = wconfig::WorkerConfig::create(configFilePath);

        // Lifetime of WorkerMain is controlled by wwMn.
        auto wwMn = wmain::WorkerMain::setup();

        wwMn->waitForTerminate();
        cout << ::context << " stopping worker" << endl;
    } catch (exception const& ex) {
        cerr << ::context << " The application failed, exception: " << ex.what() << endl;
        return 1;
    }

    return 0;
}
