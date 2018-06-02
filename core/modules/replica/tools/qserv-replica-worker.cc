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

/// qserv-replica-worker.cc represents a worker service.

// System headers
#include <iostream>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/FileServer.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessor.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace lsst::qserv;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.tools.qserv-replica-worker");

// Command line parameters

std::string workerName;
std::string configUrl;

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
void service() {
    
    try {
        replica::ServiceProvider::Ptr const provider = replica::ServiceProvider::create(configUrl);
        replica::WorkerRequestFactory requestFactory(provider);

        replica::WorkerServer::Ptr const reqProcSvr =
            replica::WorkerServer::create(provider, requestFactory, workerName);

        std::thread reqProcSvrThread([reqProcSvr] () {
            reqProcSvr->run();
        });

        replica::FileServer::Ptr const fileSvr =
            replica::FileServer::create(provider, workerName);

        std::thread fileSvrThread([fileSvr]() {
            fileSvr->run();
        });

        util::BlockPost blockPost(1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
                << "  worker: " << reqProcSvr->worker()
                << "  processor.state: " << reqProcSvr->processor()->state2string()
                << "  new, in-progress, finished: "
                << reqProcSvr->processor()->numNewRequests() << ", "
                << reqProcSvr->processor()->numInProgressRequests() << ", "
                << reqProcSvr->processor()->numFinishedRequests());
        }
        reqProcSvrThread.join();
        fileSvrThread.join();

    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
    }
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
            "  <worker> [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <worker>   - the name of a worker\n"
            "\n"
            "Flags and options:\n"
            "  --config   - a configuration URL (a configuration file or a set of the database\n"
            "               connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::workerName = parser.parameter<std::string> (1);
        ::configUrl  = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::service();
    return 0;
}
