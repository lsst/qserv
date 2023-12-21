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
#include "replica/apps/FileServerApp.h"

// System headers
#include <thread>

// Qserv headers
#include "replica/worker/FileServer.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
        "This is an application which runs a read-only file server"
        " on behalf of a Replication system's worker.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

FileServerApp::Ptr FileServerApp::create(int argc, char* argv[]) {
    return Ptr(new FileServerApp(argc, argv));
}

FileServerApp::FileServerApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider),
          _log(LOG_GET("lsst.qserv.replica.tools.qserv-replica-file-server")) {
    // Configure the command line parser

    parser().required("worker", "The name of a worker for which the server will be run.", _workerName)
            .flag("verbose", "Enable the periodic 'heartbeat' printouts.", _verbose);
}

int FileServerApp::runImpl() {
    FileServer::Ptr const server = FileServer::create(serviceProvider(), _workerName);

    thread serverLauncherThread([server]() { server->run(); });
    serverLauncherThread.detach();

    // Block the current thread while periodically printing the "heartbeat"
    // report after a random delay in an interval of [1,5] seconds

    util::BlockPost blockPost(1000, 5000);
    while (true) {
        blockPost.wait();
        if (_verbose) {
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT  worker: " << server->worker());
        }
    }
    return 0;
}

}  // namespace lsst::qserv::replica
