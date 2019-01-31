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
 * qserv-replica-job-replicate.cc is an application which analyzes the replication
 * level for all chunks of a given database family and bring the number of replicas
 * up to the explicitly specified (via the corresponding option) or implied (as per
 * the site's Configuration) minimum level. Chunks which already have the desired
 * replication level won't be affected by the operation.
 */

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/ReplicateApp.h"

using namespace lsst::qserv::replica;

int main(int argc, const char* const argv[]) {
    try {
        auto app = ReplicateApp::create(argc, argv);
        return app->run();
    } catch (std::exception const& ex) {
        std::cerr << "main()  the application failed, exception: " << ex.what() << std::endl;
    }
    return 1;
}
