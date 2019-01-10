// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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
#include "loader/CentralWorker.h"

// System headers
#include <iostream>
#include <unistd.h>

// Third-party headers


// qserv headers


// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.appWorker");
}

using namespace lsst::qserv::loader;
using  boost::asio::ip::udp;


int main(int argc, char* argv[]) {
    std::string wCfgFile("core/modules/loader/config/worker1.cnf");
    if (argc > 1) {
        wCfgFile = argv[1];
    }
    LOGS(_log, LOG_LVL_INFO, "workerCfg=" << wCfgFile);

    std::string ourHost;
    {
        char hName[300];
        if (gethostname(hName, sizeof(hName)) < 0) {
            LOGS(_log, LOG_LVL_ERROR, "Failed to get host name errno=" << errno);
            exit(-1);
        }
        ourHost = hName;
    }

    boost::asio::io_service ioService;
    boost::asio::io_context ioContext;

    WorkerConfig wCfg(wCfgFile);
    CentralWorker cWorker(ioService, ioContext, ourHost, wCfg);
    try {
        cWorker.start();
    } catch (boost::system::system_error const& e) {
        LOGS(_log, LOG_LVL_ERROR, "cWorker.start() failed e=" << e.what());
        exit(-1);
    }

    // Need to start several threads so messages aren't dropped while being processed.
    cWorker.run();
    cWorker.run();
    cWorker.run();

    bool loop = true;
    while(loop) {
        sleep(10);
    }
    ioService.stop(); // this doesn't seem to work cleanly
    LOGS(_log, LOG_LVL_INFO, "worker DONE");
}

