
#include <iostream>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica_core/BlockPost.h"
#include "replica_core/FileServer.h"
#include "replica_core/ServiceProvider.h"
#include "replica_core/WorkerProcessor.h"
#include "replica_core/WorkerRequestFactory.h"
#include "replica_core/WorkerServer.h"

namespace r  = lsst::qserv::replica;
namespace rc = lsst::qserv::replica_core;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_worker");

// Command line parameters

std::string workerName;
std::string configUrl;

/**
 * Instantiate and launch the service in its own thread. Then block
 * the current thread in a series of repeated timeouts.
 */
void service () {
    
    try {
        rc::ServiceProvider      provider       (configUrl);
        rc::WorkerRequestFactory requestFactory (provider);

        rc::WorkerServer::pointer reqProcSvr =
            rc::WorkerServer::create (provider,
                                      requestFactory,
                                      workerName);
        std::thread reqProcSvrThread ([reqProcSvr]() {
            reqProcSvr->run();
        });

        rc::FileServer::pointer fileSvr =
            rc::FileServer::create (provider,
                                    workerName);
        std::thread fileSvrThread ([fileSvr]() {
            fileSvr->run();
        });
        rc::BlockPost blockPost (1000, 5000);
        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
                << "  worker: " << reqProcSvr->worker()
                << "  processor: " << rc::WorkerProcessor::state2string(reqProcSvr->processor().state())
                << "  new, in-progress, finished: "
                << reqProcSvr->processor().numNewRequests() << ", "
                << reqProcSvr->processor().numInProgressRequests() << ", "
                << reqProcSvr->processor().numFinishedRequests());
        }
        reqProcSvrThread.join();
        fileSvrThread.join();

    } catch (std::exception& e) {
        LOGS(_log, LOG_LVL_ERROR, e.what());
    }
}
}  /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;
 
     // Parse command line parameters
    try {
        r::CmdParser parser (
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
        ::configUrl  = parser.option   <std::string> ("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::service();
    return 0;
}
