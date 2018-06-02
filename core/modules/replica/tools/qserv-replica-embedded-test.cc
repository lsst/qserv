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

/// qserv-replica-embedded-test.cc incorporates multiple worker servers
/// within a single process.
///
/// NOTE: a special single-node configuration is required by this test.
//        Also, each logical worker must get a unique path in a data file
///       system. The files must be read-write enabled for a user account
///       under which the test is run.

// System headers
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/FileServer.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace lsst::qserv;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.tools.qserv-replica-embedded-test");

// Command line parameters

bool        enableFileServer;
std::string configUrl;

/**
 * Launch all worker servers in dedicated detached threads. Also run
 * one extra thread per each worked for the 'hearbeat' monitoring.
 */
void runAllWorkers(replica::ServiceProvider::Ptr const& provider,
                   replica::WorkerRequestFactory& requestFactory) {

    for (std::string const& workerName : provider->config()->workers()) {
        
        // Create the request pocessing server and run it within a dedicated thread
        // because it's the blocking operation for the launching thread.

        replica::WorkerServer::Ptr const reqProcSrv =
            replica::WorkerServer::create(provider, requestFactory, workerName);

        std::thread reqProcSrvThread([reqProcSrv] () {
            reqProcSrv->run();
        });
        reqProcSrvThread.detach();
        
        // Run the heartbeat monitor for the server within another thread
 
        std::thread reqProcMonThread([reqProcSrv] () {
            util::BlockPost blockPost(1000, 5000);
            while (true) {
                blockPost.wait();
                LOGS(_log, LOG_LVL_INFO, "<WORKER:" << reqProcSrv->worker() << " HEARTBEAT> "
                    << " processor state: " << reqProcSrv->processor()->state2string()
                    << " new:"              << reqProcSrv->processor()->numNewRequests()
                    << " in-progress: "     << reqProcSrv->processor()->numInProgressRequests()
                    << " finished: "        << reqProcSrv->processor()->numFinishedRequests());
            }
        });
        reqProcMonThread.detach();

        // If requested then also create and run the file server. Note the server
        // should be running in a separate thread because it's the blocking
        // operation fr the launching thread.

        if (enableFileServer) {
            replica::FileServer::Ptr const fileSrv =
                replica::FileServer::create(provider, workerName);
    
            std::thread fileSrvThread([fileSrv] () {
                fileSrv->run();
            });
            fileSrvThread.detach();
        }
    }
}

/**
 * Instantiate and run all threads. Then block the current thread in
 * a series of repeated timeouts.
 */
void run() {
    
    try {
        replica::ServiceProvider::Ptr const provider = replica::ServiceProvider::create(configUrl);
        replica::WorkerRequestFactory requestFactory(provider);

        // Run the worker servers

        runAllWorkers(provider, requestFactory);

        // Then block the calling thread foreever.
        util::BlockPost blockPost(1000, 5000);
        while (true) {
            blockPost.wait();  
        }

    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, e.what());
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
            "  [--enable-file-server] [--config=<url>]\n"
            "\n"
            "Flags and options:\n"
            "  --enable-file-server  - talso launch a dedicated FileServer for each worker\n"
            "  --config              - a configuration URL (a configuration file or a set of the database\n"
            "                          connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::enableFileServer = parser.flag("enable-file-server");
        ::configUrl        = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::run();
    return 0;
}
