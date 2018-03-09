#include <stdexcept>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/FileServer.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_worker");

// Command line parameters

bool        enableFileServer;
std::string configUrl;

/**
 * Launch all worker servers in dedicated detached threads. Also run
 * one extra thread per each worked for the 'hearbeat' monitoring.
 */
void runAllWorkers(replica::ServiceProvider::pointer const& provider,
                   replica::WorkerRequestFactory& requestFactory) {

    for (std::string const& workerName : provider->config()->workers()) {
        
        // Create the request pocessing server and run it within a dedicated thread
        // because it's the blocking operation fr the launching thread.

        replica::WorkerServer::pointer const reqProcSrv =
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
                    << " processor state: " << replica::WorkerProcessor::state2string(reqProcSrv->processor().state())
                    << " new:"              << reqProcSrv->processor().numNewRequests()
                    << " in-progress: "     << reqProcSrv->processor().numInProgressRequests()
                    << " finished: "        << reqProcSrv->processor().numFinishedRequests());
            }
        });
        reqProcMonThread.detach();

        // If requested then also create and run the file server. Note the server
        // should be running in a separate thread because it's the blocking
        // operation fr the launching thread.

        if (enableFileServer) {
            replica::FileServer::pointer const fileSrv =
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
        replica::ServiceProvider::pointer const provider = replica::ServiceProvider::create(configUrl);
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