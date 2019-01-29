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

/**
 * qserv-replica-worker-all.cc runs all worker servers within a single process.
 *
 * NOTE: a special single-node configuration is required by this test.
 * Also, each logical worker must get a unique path in a data file
 * system. The files must be read-write enabled for a user account
 * under which the test is run.
 */

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/WorkerAllApp.h"

using namespace lsst::qserv::replica;

int main(int argc, const char* const argv[]) {
    try {
        auto app = WorkerAllApp::create(argc, argv);
        return app->run();
    } catch (std::exception const& ex) {
        std::cerr << "main()  the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
