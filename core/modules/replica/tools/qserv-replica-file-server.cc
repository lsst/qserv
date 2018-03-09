#include <iostream>
#include <string>
#include <thread>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/ServiceProvider.h"
#include "replica/FileServer.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.qserv-replica-file-server");

// Command line parameters

std::string workerName;
std::string configUrl;

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
void service() {
    
    try {
        replica::ServiceProvider::pointer const provider = replica::ServiceProvider::create(configUrl);

        replica::FileServer::pointer const server =
            replica::FileServer::create(provider, workerName);

        std::thread serverLauncherThread([server] () {
            server->run();
        });
        util::BlockPost blockPost(1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT  worker: " << server->worker());
        }
        serverLauncherThread.join();

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

        ::workerName = parser.parameter<std::string>(1);
        ::configUrl  = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    } 
    ::service();
    return 0;
}